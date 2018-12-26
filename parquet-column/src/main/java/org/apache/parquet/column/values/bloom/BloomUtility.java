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
package org.apache.parquet.column.values.bloom;

import org.apache.parquet.io.api.Binary;

public abstract class BloomUtility<T extends Comparable<T>> {
  public Bloom bloom;
  public abstract boolean contains(T value);

  public static class IntBloom extends BloomUtility<Integer> {
    public IntBloom (Bloom bloom) {
      this.bloom = bloom;
    }

    @Override
    public boolean contains(Integer value) {
      return bloom.find(bloom.hash(value.intValue()));
    }
  }

  public static class LongBloom extends BloomUtility<Long> {
    public LongBloom (Bloom bloom) {
      this.bloom = bloom;
    }

    @Override
    public boolean contains(Long value) {
      return bloom.find(bloom.hash(value.longValue()));
    }
  }

  public static class DoubleBloom extends BloomUtility<Double> {
    public DoubleBloom (Bloom bloom) {
      this.bloom = bloom;
    }

    @Override
    public boolean contains(Double value) {
      return bloom.find(bloom.hash(value.doubleValue()));
    }
  }

  public static class FloatBloom extends BloomUtility<Float> {
    public FloatBloom (Bloom bloom) {
      this.bloom = bloom;
    }

    @Override
    public boolean contains(Float value) {
      return bloom.find(bloom.hash(value.floatValue()));
    }
  }

  public static class BinaryBloom extends BloomUtility<Binary> {
    public BinaryBloom (Bloom bloom) {
      this.bloom = bloom;
    }

    @Override
    public boolean contains(Binary value) {
      return bloom.find(bloom.hash(value));
    }
  }
}
