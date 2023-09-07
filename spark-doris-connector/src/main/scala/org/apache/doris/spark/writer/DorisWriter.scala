// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.writer

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.listener.DorisTransactionListener
import org.apache.doris.spark.load.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.doris.spark.etl.{BucketPartitioner, DppUtils, SparkDppUtils}
import org.apache.doris.spark.rest.RestService
import org.apache.doris.spark.sql.Utils
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.time.Duration
import java.{lang, util}
import java.util.Objects
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.mutable
import scala.util.{Failure, Success}

class DorisWriter(settings: SparkSettings) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DorisWriter])
  val COMMIT_INTERVAL_MILLIS = 10;

  val batchSize: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_SIZE,
    ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  private val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_RETRIES,
    ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  private val sinkTaskPartitionSize: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_TASK_PARTITION_SIZE)
  private val sinkTaskUseRepartition: Boolean = settings.getProperty(ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION,
    ConfigurationOptions.DORIS_SINK_TASK_USE_REPARTITION_DEFAULT.toString).toBoolean
  private val batchInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS,
    ConfigurationOptions.DORIS_SINK_BATCH_INTERVAL_MS_DEFAULT)
  private val maxSinkBlocks: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_BLOCKING_TIMES,
    ConfigurationOptions.SINK_MAX_BLOCKING_TIMES_DEFAULT)
  private val blockTriggerKeys: String = settings.getProperty(ConfigurationOptions.DORIS_SINK_BLOCKING_TRIGGER_KEYS,
    ConfigurationOptions.SINK_BLOCKING_TRIGGER_KEYS_DEFAULT)
  private val maxBlockInterValMs: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_MAX_BLOCKING_INTERVAL_MS,
    ConfigurationOptions.SINK_MAX_BLOCKING_INTERVAL_MS_DEFAULT)
  private val blockingIndexBase: Integer = settings.getIntegerProperty(ConfigurationOptions.SINK_BLOCKING_INDEX_BASE,
    ConfigurationOptions.SINK_BLOCKING_INDEX_BASE_DEFAULT)
  private val blockTriggerKeysArray: Array[String] = blockTriggerKeys.split(",")

  private val enable2PC: Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_ENABLE_2PC,
    ConfigurationOptions.DORIS_SINK_ENABLE_2PC_DEFAULT);

  private val enableBucketRepartition: lang.Boolean = settings.getBooleanProperty(ConfigurationOptions.DORIS_SINK_BUCKET_REPARTITION,
    ConfigurationOptions.DORIS_SINK_BUCKET_REPARTITION_DEFAULT)

  private val bucketsNum: Integer = settings.getIntegerProperty(ConfigurationOptions.DORIS_SINK_BUCKET_NUM,
    ConfigurationOptions.DORIS_SINK_BUCKET_NUM_DEFAULT)

  private val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)

  def write(dataFrame: DataFrame): Unit = {

    val sc = dataFrame.sqlContext.sparkContext
    val preCommittedTxnAcc = sc.collectionAccumulator[Int]("preCommittedTxnAcc")
    if (enable2PC) {
      logger.info("enable 2PC, add DorisTransactionListener.")
      sc.addSparkListener(new DorisTransactionListener(preCommittedTxnAcc, dorisStreamLoader))
    }

    val dfColumns = dataFrame.columns
    var resultRdd = dataFrame.rdd

    if (enableBucketRepartition) {
      val etlSchema = RestService.getEtlSchema(settings, logger)
      SparkDppUtils.repartitionByBuckets(dataFrame, etlSchema.partitionInfo, etlSchema.columns, bucketsNum)
        .map(_.toSeq.map(_.asInstanceOf[AnyRef]).toList.asJava)
        .foreachPartition(partition => {
          partition
            .grouped(batchSize)
            .foreach(batch => flush(batch, dfColumns))
        })
    } else {
      if (Objects.nonNull(sinkTaskPartitionSize)) {
        resultRdd = if (sinkTaskUseRepartition) resultRdd.repartition(sinkTaskPartitionSize) else resultRdd.coalesce(sinkTaskPartitionSize)
      }
      resultRdd
        .map(_.toSeq.map(_.asInstanceOf[AnyRef]).toList.asJava)
        .foreachPartition(partition => {
          partition
            .grouped(batchSize)
            .foreach(batch => flush(batch, dfColumns))
        })
    }

    /**
     * flush data to Doris and do retry when flush error
     *
     */
    def flush(batch: Iterable[util.List[Object]], dfColumns: Array[String]): Unit = {
      Utils.retry[util.List[Integer], Exception](maxRetryTimes, maxSinkBlocks, Duration.ofMillis(batchInterValMs.toLong),
        Duration.ofMillis(maxBlockInterValMs.toLong), blockTriggerKeysArray, blockingIndexBase, logger) {
        dorisStreamLoader.loadV2(batch.toList.asJava, dfColumns, enable2PC)
      } match {
        case Success(txnIds) => if (enable2PC) {
          txnIds.asScala.foreach(txnId => preCommittedTxnAcc.add(txnId))
          logger.info("success add txnIds: {}", txnIds.asScala.mkString(","))
        }
        case Failure(e) =>
          if (enable2PC) {
            // if task run failed, acc value will not be returned to driver,
            // should abort all pre committed transactions inside the task
            logger.info("load task failed, start aborting previously pre-committed transactions")
            val abortFailedTxnIds = mutable.Buffer[Int]()
            preCommittedTxnAcc.value.asScala.foreach(txnId => {
              Utils.retry[Unit, Exception](3, Duration.ofMillis(COMMIT_INTERVAL_MILLIS), logger) {
                dorisStreamLoader.abort(txnId)
              } match {
                case Success(_) =>
                case Failure(_) => abortFailedTxnIds += txnId
              }
            })
            if (abortFailedTxnIds.nonEmpty) logger.warn("not aborted txn ids: {}", abortFailedTxnIds.mkString(","))
            preCommittedTxnAcc.reset()
          }
          throw new IOException(
            s"Failed to load batch data on BE: ${dorisStreamLoader.getLoadUrlStr} node and exceeded the max ${maxRetryTimes} retry times.", e)
      }
    }

  }


}
