/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.IOException;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.CodecMetaFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class RLECodec extends AdaptiveCompressionCodec implements ColumnPageCodec {

  RLECodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, Compressor compressor) {
    super(srcDataType, targetDataType, stats, compressor);
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public EncodedColumnPage encode(ColumnPage input) throws MemoryException, IOException {
    RLEEncoding codec = RLEEncoding.newEncoder(input.getPageSize(), input.getDataType());
    input.encode(codec);
    return new EncodedMeasurePage(
        input.getPageSize(),
        codec.getEncodedBytes(),
        CodecMetaFactory.createMeta(stats, targetDataType),
        ((SimpleStatsResult)input.getStatistics()).getNullBits());
  }

  @Override
  public EncodedColumnPage[] encodeComplexColumn(ComplexColumnPage input) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    return null;
  }


}
