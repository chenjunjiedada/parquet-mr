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
package org.apache.parquet.column.values.bloomfilter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

import java.util.HashSet;
import java.util.Set;

public class BloomFilterValuesWriter extends ValuesWriter {
  private Set<Long> hashValues = new HashSet<>();

  private BloomFilter bloomFilter;

  public BloomFilterValuesWriter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  @Override
  public long getBufferedSize() {
    return hashValues.size() * Long.BYTES;
  }

  @Override
  public BytesInput getBytes() {
    return null;
  }

  @Override
  public Encoding getEncoding() {
    return null;
  }

  @Override
  public void reset() {
    hashValues.clear();
  }

  @Override
  public long getAllocatedSize() {
    return hashValues.size() * Long.BYTES;
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("bloom filter cache size: %d bytes", getBufferedSize()) ;
  }

  @Override
  public final void writeBytes(Binary v) {
    hashValues.add(bloomFilter.hash(v));
  }

  @Override
  public final void writeInteger(int v) {
    hashValues.add(bloomFilter.hash(v));
  }

  @Override
  public final void writeLong(long v) {
    hashValues.add(bloomFilter.hash(v));
  }

  @Override
  public final void writeDouble(double v) {
    hashValues.add(bloomFilter.hash(v));
  }

  @Override
  public final void writeFloat(float v) {
    hashValues.add(bloomFilter.hash(v));
  }

  @Override
  public void writeByte(int v) {
    hashValues.add(bloomFilter.hash(v));
  }

  public BloomFilter toBloomFilter() {
    for (Long value : hashValues) {
      bloomFilter.insertHash(value);
    }
    return bloomFilter;
  }

}
