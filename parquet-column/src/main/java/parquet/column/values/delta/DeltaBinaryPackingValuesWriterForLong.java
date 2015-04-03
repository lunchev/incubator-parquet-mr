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
package parquet.column.values.delta;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.values.bitpacking.BytePackerForLong;
import parquet.column.values.bitpacking.Packer;
import parquet.io.ParquetEncodingException;

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
public class DeltaBinaryPackingValuesWriterForLong {
  /**
   * max bitwidth for a mini block, it is used to allocate miniBlockByteBuffer which is
   * reused between flushes.
   */
  public static final int MAX_BITWIDTH = 64;

  /**
   * stores blockSizeInValues, miniBlockNumInABlock and miniBlockSizeInValues
   */
  private final DeltaBinaryPackingConfig config;

  /**
   * bit width for each mini block, reused between flushes
   */
  private final int[] bitWidths;

  private int totalValueCount = 0;

  /**
   * a pointer to deltaBlockBuffer indicating the end of deltaBlockBuffer
   * the number of values in the deltaBlockBuffer that haven't flushed to baos
   * it will be reset after each flush
   */
  private int deltaValuesToFlush = 0;

  /**
   * stores delta values starting from the 2nd value written(1st value is stored in header).
   * It's reused between flushes
   */
  private long[] deltaBlockBuffer;

  /**
   * bytes buffer for a mini block, it is reused for each mini block.
   * Therefore the size of biggest miniblock with bitwith of MAX_BITWITH is allocated
   */
  private byte[] miniBlockByteBuffer;

  /**
   * firstValue is written to the header of the page
   */
  private long firstValue = 0;

  /**
   * cache previous written value for calculating delta
   */
  private long previousValue = 0;

  /**
   * min delta is written to the beginning of each block.
   * it's zig-zag encoded. The deltas stored in each block is actually the difference to min delta,
   * therefore are all positive
   * it will be reset after each flush
   */
  private long minDeltaInCurrentBlock = Long.MAX_VALUE;

  public DeltaBinaryPackingValuesWriterForLong(final DeltaBinaryPackingConfig config) {
    this.config = config;
    bitWidths = new int[config.miniBlockNumInABlock];
    deltaBlockBuffer = new long[config.blockSizeInValues];
    miniBlockByteBuffer = new byte[config.miniBlockSizeInValues * MAX_BITWIDTH];
  }

  public void writeLong(long v, final CapacityByteArrayOutputStream baos) {
    totalValueCount++;

    if (totalValueCount == 1) {
      firstValue = v;
      previousValue = firstValue;
      return;
    }

    // Calculate delta. The possible overflow is accounted for. The algorithm is correct because
    // Java long is working as a mudalar ring with base 2^64 and because of the plus and minus
    // properties of a ring. http://en.wikipedia.org/wiki/Modular_arithmetic#Integers_modulo_n
    long delta = v - previousValue;
    previousValue = v;

    deltaBlockBuffer[deltaValuesToFlush++] = delta;

    if (delta < minDeltaInCurrentBlock) {
      minDeltaInCurrentBlock = delta;
    }

    if (config.blockSizeInValues == deltaValuesToFlush) {
      flushBlockBuffer(baos);
    }
  }

  private void flushBlockBuffer(final CapacityByteArrayOutputStream baos) {
    //since we store the min delta, the deltas will be converted to be the difference to min delta and all positive
    for (int i = 0; i < deltaValuesToFlush; i++) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaInCurrentBlock;
    }

    writeMinDelta(baos);
    int miniBlocksToFlush = getMiniBlockCountToFlush(deltaValuesToFlush);

    calculateBitWidthsForDeltaBlockBuffer(miniBlocksToFlush);
    for (int i = 0; i < config.miniBlockNumInABlock; i++) {
      writeBitWidthForMiniBlock(i, baos);
    }

    for (int i = 0; i < miniBlocksToFlush; i++) {
      //writing i th miniblock
      int currentBitWidth = bitWidths[i];
      BytePackerForLong packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(currentBitWidth);
      int miniBlockStart = i * config.miniBlockSizeInValues;
      for (int j = miniBlockStart; j < (i + 1) * config.miniBlockSizeInValues; j += 8) {//8 values per pack
        // mini block is atomic in terms of flushing
        // This may write more values when reach to the end of data writing to last mini block,
        // since it may not be aligend to miniblock,
        // but doesnt matter. The reader uses total count to see if reached the end.
        packer.pack8Values(deltaBlockBuffer, j, miniBlockByteBuffer, 0);
        baos.write(miniBlockByteBuffer, 0, currentBitWidth);
      }
    }

    minDeltaInCurrentBlock = Long.MAX_VALUE;
    deltaValuesToFlush = 0;
  }

  private void writeBitWidthForMiniBlock(int i, final CapacityByteArrayOutputStream baos) {
    try {
      BytesUtils.writeIntLittleEndianOnOneByte(baos, bitWidths[i]);
    } catch (IOException e) {
      throw new ParquetEncodingException("can not write bitwith for miniblock", e);
    }
  }

  private void writeMinDelta(final CapacityByteArrayOutputStream baos) {
    try {
      BytesUtils.writeZigZagVarLong(minDeltaInCurrentBlock, baos);
    } catch (IOException e) {
      throw new ParquetEncodingException("can not write min delta for block", e);
    }
  }

  /**
   * iterate through values in each mini block and calculate the bitWidths of max values.
   *
   * @param miniBlocksToFlush
   */
  private void calculateBitWidthsForDeltaBlockBuffer(int miniBlocksToFlush) {
    for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksToFlush; miniBlockIndex++) {

      long mask = 0;
      int miniStart = miniBlockIndex * config.miniBlockSizeInValues;

      //The end of current mini block could be the end of current block(deltaValuesToFlush) buffer when data is not aligned to mini block
      int miniEnd = Math.min((miniBlockIndex + 1) * config.miniBlockSizeInValues, deltaValuesToFlush);

      for (int i = miniStart; i < miniEnd; i++) {
        mask |= deltaBlockBuffer[i];
      }
      bitWidths[miniBlockIndex] = 64 - Long.numberOfLeadingZeros(mask);
    }
  }

  private int getMiniBlockCountToFlush(double numberCount) {
    return (int) Math.ceil(numberCount / config.miniBlockSizeInValues);
  }

  /**
   * getBytes will trigger flushing block buffer, DO NOT write after getBytes() is called without calling reset()
   *
   * @return
   */
  public BytesInput getBytes(final CapacityByteArrayOutputStream baos) {
    //The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
    if (deltaValuesToFlush != 0) {
      flushBlockBuffer(baos);
    }
    return BytesInput.concat(
            config.toBytesInput(),
            BytesInput.fromUnsignedVarInt(totalValueCount),
            BytesInput.fromZigZagVarLong(firstValue),
            BytesInput.from(baos));
  }

  public void reset() {
    this.totalValueCount = 0;
    this.deltaValuesToFlush = 0;
    this.minDeltaInCurrentBlock = Long.MAX_VALUE;
  }
  
  public int getTotalValuesCount() {
    return this.totalValueCount;
  }
}
