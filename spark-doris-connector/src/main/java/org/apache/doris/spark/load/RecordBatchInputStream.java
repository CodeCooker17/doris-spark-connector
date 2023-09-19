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

package org.apache.doris.spark.load;

import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.exception.ShouldNeverHappenException;
import org.apache.doris.spark.util.DataUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * InputStream for batch load
 */
public class RecordBatchInputStream extends InputStream {

    public static final Logger LOG = LoggerFactory.getLogger(RecordBatchInputStream.class);

    /**
     * Load record batch
     */
    private final RecordBatch recordBatch;

    /**
     * first line flag
     */
    private boolean isFirst = true;

    /**
     * record buffer
     */

    private ByteBuffer lineBuf;

    private ByteBuffer deliBuf;

    private final byte[] delim;

    /**
     * record count has been read
     */
    private int readCount = 0;

    /**
     * streaming mode pass through data without process
     */
    private final boolean passThrough;

    public RecordBatchInputStream(RecordBatch recordBatch, boolean passThrough) {
        this.recordBatch = recordBatch;
        this.passThrough = passThrough;
        this.delim = recordBatch.getDelim();
    }

    @Override
    public int read() throws IOException {
        try {
            if (deliBuf != null && deliBuf.remaining() > 0) {
                return deliBuf.get() & 0xff;
            }
            if (lineBuf.remaining() == 0 && endOfBatch()) {
                return -1;
            }

        } catch (DorisException e) {
            throw new IOException(e);
        }
        return lineBuf.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            if (deliBuf != null && deliBuf.remaining() > 0) {
                int bytesRead = Math.min(len, deliBuf.remaining());
                deliBuf.get(b, off, bytesRead);
                return bytesRead;
            }

            if (lineBuf.remaining() == 0 && endOfBatch()) {
                return -1;
            }
        } catch (DorisException e) {
            throw new IOException(e);
        }
        int bytesRead = Math.min(len, lineBuf.remaining());
        lineBuf.get(b, off, bytesRead);
        return bytesRead;
    }

    /**
     * Check if the current batch read is over.
     * If the number of reads is greater than or equal to the batch size or there is no next record, return false,
     * otherwise return true.
     *
     * @return Whether the current batch read is over
     * @throws DorisException
     */
    public boolean endOfBatch() throws DorisException {
        Iterator<InternalRow> iterator = recordBatch.getIterator();
        if (readCount >= recordBatch.getBatchSize() || !iterator.hasNext()) {
            return true;
        }
        readNext(iterator);
        return false;
    }

    /**
     * read next record into buffer
     *
     * @param iterator row iterator
     * @throws DorisException
     */
    private void readNext(Iterator<InternalRow> iterator) throws DorisException {
        if (!iterator.hasNext()) {
            throw new ShouldNeverHappenException();
        }
        byte[] rowBytes = rowToByte(iterator.next());
        if (isFirst) {
            deliBuf = null;
            lineBuf = ByteBuffer.wrap(rowBytes);
            isFirst = false;
        } else {
            deliBuf =  ByteBuffer.wrap(delim);
            lineBuf = ByteBuffer.wrap(rowBytes);
        }
        readCount++;
    }

    /**
     * Convert Spark row data to byte array
     *
     * @param row row data
     * @return byte array
     * @throws DorisException
     */
    private byte[] rowToByte(InternalRow row) throws DorisException {

        byte[] bytes;

        if (passThrough) {
            bytes = row.getString(0).getBytes(StandardCharsets.UTF_8);
            return bytes;
        }

        switch (recordBatch.getFormat().toLowerCase()) {
            case "csv":
                if (recordBatch.getAddDoubleQuotes()) {
                    bytes = DataUtil.rowAddDoubleQuotesToCsvBytes(row, recordBatch.getSchema(), recordBatch.getSep());
                } else {
                    bytes = DataUtil.rowToCsvBytes(row, recordBatch.getSchema(), recordBatch.getSep());
                }
                break;
            case "json":
                try {
                    bytes = DataUtil.rowToJsonBytes(row, recordBatch.getSchema());
                } catch (JsonProcessingException e) {
                    throw new DorisException("parse row to json bytes failed", e);
                }
                break;
            default:
                throw new IllegalArgumentException("format", recordBatch.getFormat());
        }

        return bytes;

    }


}
