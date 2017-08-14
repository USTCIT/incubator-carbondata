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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForShort;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

public class DirectDictDimensionIndexCodec extends IndexStorageCodec {

  public DirectDictDimensionIndexCodec(boolean isSort, boolean isInvertedIndex,
      Compressor compressor) {
    super(isSort, isInvertedIndex, compressor);
  }

  @Override
  public String getName() {
    return "DirectDictDimensionIndexCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {
      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        IndexStorage indexStorage;
        byte[][] data = input.getByteArrayPage();
        if (isInvertedIndex) {
          if (version == ColumnarFormatVersion.V3) {
            indexStorage = new BlockIndexerStorageForShort(data, false, false, isSort);
          } else {
            indexStorage = new BlockIndexerStorageForInt(data, false, false, isSort);
          }
        } else {
          if (version == ColumnarFormatVersion.V3) {
            indexStorage = new BlockIndexerStorageForNoInvertedIndexForShort(data, false);
          } else {
            indexStorage = new BlockIndexerStorageForNoInvertedIndexForInt(data);
          }
        }
        byte[] flattened = ByteUtil.flatten(indexStorage.getDataPage());
        byte[] compressed = compressor.compressByte(flattened);
        return compressed;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.DICTIONARY);
        encodings.add(Encoding.DIRECT_DICTIONARY);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return null;
      }

      @Override
      protected void fillLegacyFields(ColumnPage inputPage, DataChunk2 dataChunk)
          throws IOException {

      }
    };
  }

}