/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.delta;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;

/**
 * Write integers with delta encoding and binary packing
 * The format is as follows:
 * <p/>
 * <pre>
 *   {@code
 *     delta-binary-packing: <page-header> <block>*
 *     page-header := <block size in values> <number of miniblocks in a block> <total value count> <first value>
 *     block := <min delta> <list of bitwidths of miniblocks> <miniblocks>
 *
 *     min delta : zig-zag var int encoded
 *     bitWidthsOfMiniBlock : 1 byte little endian
 *     blockSizeInValues,blockSizeInValues,totalValueCount,firstValue : unsigned varint
 *   }
 * </pre>
 *
 * The algorithm and format is inspired by D. Lemire's paper: http://lemire.me/blog/archives/2012/09/12/fast-integer-compression-decoding-billions-of-integers-per-second/
 *
 * @author Tianshuo Deng
 */
public class DeltaBinaryPackingValuesWriter extends ValuesWriter {
  public static final int DEFAULT_NUM_BLOCK_VALUES = 128;

  public static final int DEFAULT_NUM_MINIBLOCKS = 4;

  private final CapacityByteArrayOutputStream baos;

  /**
   * stores blockSizeInValues, miniBlockNumInABlock and miniBlockSizeInValues
   */
  private final DeltaBinaryPackingConfig config;
  
  private final DeltaBinaryPackingValuesWriterForInteger integerImplementation;
  
  private final DeltaBinaryPackingValuesWriterForLong longImplementation;

  public DeltaBinaryPackingValuesWriter(int slabSize, int pageSize) {
    this(DEFAULT_NUM_BLOCK_VALUES, DEFAULT_NUM_MINIBLOCKS, slabSize, pageSize);
  }

  public DeltaBinaryPackingValuesWriter(int blockSizeInValues, int miniBlockNum, int slabSize, int pageSize) {
    this.config = new DeltaBinaryPackingConfig(blockSizeInValues, miniBlockNum);
    baos = new CapacityByteArrayOutputStream(slabSize, pageSize);
    integerImplementation = new DeltaBinaryPackingValuesWriterForInteger(config);
    longImplementation = new DeltaBinaryPackingValuesWriterForLong(config);
  }

  @Override
  public long getBufferedSize() {
    return baos.size();
  }

  @Override
  public void writeInteger(int v) {
    integerImplementation.writeInteger(v, baos);
  }
  
  @Override
  public void writeLong(long v) {
    longImplementation.writeLong(v, baos);
  }

  /**
   * getBytes will trigger flushing block buffer, DO NOT write after getBytes() is called without calling reset()
   *
   * @return
   */
  @Override
  public BytesInput getBytes() {
    if (longImplementation.getTotalValuesCount() > 0) {
      return longImplementation.getBytes(baos);
    } else {
      return integerImplementation.getBytes(baos);
    }
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.DELTA_BINARY_PACKED;
  }

  @Override
  public void reset() {
    integerImplementation.reset();
    longImplementation.reset();
    this.baos.reset();
  }

  @Override
  public long getAllocatedSize() {
    return baos.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s DeltaBinaryPacking %d bytes", prefix, getAllocatedSize());
  }
}
