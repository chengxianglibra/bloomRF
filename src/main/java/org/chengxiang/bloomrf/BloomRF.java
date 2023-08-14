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

package org.chengxiang.bloomrf;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.BitSet;

/**
 * A Range Filter which is implemented on https://arxiv.org/abs/2012.15596. Like BloomFilter,
 * support approximate check whether value existed in set, while support both range filter and point query.
 * No fpp(False Positive Possibility) guaranteed, use 5-10 bits per key for bit set is recommended.
 */
public class BloomRF<T extends Number> {

  private static final Logger LOG = LoggerFactory.getLogger(BloomRF.class);
  private static int[] primes = {3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 57, 59, 61, 67,
      71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163,
      167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263,
      269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353};
  private static final int DEFAULT_TRACE_TREE_BITS = 4;

  // used value bits based on max value, same prefix bits would be ignored.
  private int valueBitsNumber;
  // use this bit set to store the bit value of monotone hash value in each layer.
  private BitSet bitSet;
  // the bits size in bit set.
  private int bitSize;
  // Trace Tree layer number of Dyadic Trace Tree, the layer start from layer1 as leaf layer.
  private int layerNumber;
  // the bits number in a Trace Tree, for example, if layer number is 16, interval is 64/16 - 1 = 3, A Trace Tree
  // would contain 2^3 = 8 values.
  private int intervalBits;
  // primes to be used in primeHash function.
  private int[] selectedPrimes;

  // the max value of this range filter.
  private UnsignedLong maxValue;

  private Mapper<T> typeMapper;

  public static BloomRF<Long> longBloomRF(int bitSize) {
    return new BloomRF<>(bitSize, Long.MAX_VALUE, new LongMapper());
  }

  public static BloomRF<Long> longBloomRF(int bitSize, int traceTreeBits, long maxValue) {
    return new BloomRF<>(bitSize, traceTreeBits, maxValue, new LongMapper());
  }

  public static BloomRF<Integer> intBloomRF(int bitSize) {
    return new BloomRF<>(bitSize, Integer.MAX_VALUE, new IntegerMapper());
  }

  public static BloomRF<Integer> intBloomRF(int bitSize, int traceTreeBits, int maxValue) {
    return new BloomRF<>(bitSize, traceTreeBits, maxValue, new IntegerMapper());
  }

  public static BloomRF<Double> doubleBloomRF(int bitSize) {
    return new BloomRF<>(bitSize, Double.MAX_VALUE, new DoubleMapper());
  }

  public static BloomRF<Double> doubleBloomRF(int bitSize, int traceTreeBits, double maxValue) {
    return new BloomRF<>(bitSize, traceTreeBits, maxValue, new DoubleMapper());
  }

  public static BloomRF<Float> floatBloomRF(int bitSize) {
    return new BloomRF<>(bitSize, Float.MAX_VALUE, new FloatMapper());
  }

  public static BloomRF<Float> floatBloomRF(int bitSize, int traceTreeBits, float maxValue) {
    return new BloomRF<>(bitSize, traceTreeBits, maxValue, new FloatMapper());
  }

  /**
   * Construct BloomRF with bitSize.
   *
   * @param bitSize  the number of bits for bit set.
   * @param maxValue the max value in this range filter.
   */
  private BloomRF(int bitSize, T maxValue, Mapper<T> typeMapper) {
    this(bitSize, DEFAULT_TRACE_TREE_BITS, maxValue, typeMapper);
  }

  /**
   * Construct BloomRF with bitSize and layerNumber.
   *
   * @param bitSize       the number of bits for bit set.
   * @param traceTreeBits Trace Tree bits in Dyadic Trace Tree.
   */
  private BloomRF(int bitSize, int traceTreeBits, T maxValue, Mapper<T> typeMapper) {
    Preconditions.checkArgument(bitSize > 0, "bitSize must be positive");
    Preconditions.checkArgument(traceTreeBits > 1, "layerNumber must be positive");
    Preconditions.checkArgument(bitSize > (1 << (traceTreeBits - 1)), "bitSize is too small," +
        " must bigger than 1 << (traceTreeBits - 1)");
    this.maxValue = typeMapper.toULong(maxValue);
    this.valueBitsNumber = bitNumber(this.maxValue, traceTreeBits);
    this.layerNumber = valueBitsNumber / traceTreeBits;
    this.bitSet = new BitSet(bitSize);
    this.bitSize = bitSize;
    this.intervalBits = traceTreeBits - 1;
    this.typeMapper = typeMapper;
    selectedPrimes = new int[2 * this.layerNumber];
    for (int i = 0; i < 2 * this.layerNumber; i++) {
      if (i % 2 == 0) {
        selectedPrimes[i] = primes[primes.length - (i + 1)];
      } else {
        selectedPrimes[i] = primes[i];
      }
    }
  }

  public void add(T value) {
    add(typeMapper.toULong(value));
  }

  private void add(UnsignedLong value) {
    Preconditions.checkArgument(value.compareTo(maxValue) <= 0,
        "input value could not greater than maxValue.");
    for (int i = 1; i <= layerNumber; i++) {
      int mh = monotoneHash(value, i);
      bitSet.set(mh);
    }
  }

  public boolean exists(T value) {
    UnsignedLong longValue = typeMapper.toULong(value);
    if (longValue.compareTo(maxValue) > 0) {
      return false;
    }

    for (int i = 1; i <= layerNumber; i++) {
      int mh = monotoneHash(longValue, i);
      if (!bitSet.get(mh)) {
        return false;
      }
    }
    return true;
  }

  public boolean greatThan(T value) {
    UnsignedLong longValue = typeMapper.toULong(value);
    return existsInRange(longValue, false, maxValue, true);
  }

  public boolean greatOrEqualsThan(T value) {
    UnsignedLong longValue = typeMapper.toULong(value);
    return existsInRange(longValue, true, maxValue, true);
  }

  public boolean lessThan(T value) {
    UnsignedLong longValue = typeMapper.toULong(value);
    return existsInRange(UnsignedLong.ZERO, true, longValue, false);
  }

  public boolean lessOrEqualsThan(T value) {
    UnsignedLong longValue = typeMapper.toULong(value);
    return existsInRange(UnsignedLong.ZERO, true, longValue, true);
  }

  public boolean isNull() {
    return bitSet.isEmpty();
  }

  public boolean isNotNull() {
    return !bitSet.isEmpty();
  }

  public boolean existsInRange(T min, boolean minInclusive, T max, boolean maxInclusive) {
    UnsignedLong uMin = typeMapper.toULong(min);
    UnsignedLong uMax = typeMapper.toULong(max);
    return existsInRange(uMin, minInclusive, uMax, maxInclusive);
  }

  private boolean existsInRange(UnsignedLong min, boolean minInclusive, UnsignedLong max, boolean maxInclusive) {
    Preconditions.checkArgument(max.compareTo(min) > 0, "max value must be greater than min");
    UnsignedLong uMin = min;
    UnsignedLong uMax = max;
    if (!minInclusive) {
      uMin = min.plus(UnsignedLong.ONE);
    }
    if (!maxInclusive) {
      uMax = max.minus(UnsignedLong.ONE);
    }
    if (uMin.compareTo(maxValue) > 0) {
      return false;
    } else if (uMax.compareTo(maxValue) > 0) {
      uMax = maxValue;
    }

    // find which layer of Trace Tree should we start to verify according to range scope.
    int samePrefixLength = uMin.samePrefixLength(uMax) - (64 - valueBitsNumber);
    int startLayer = layerNumber - samePrefixLength / (valueBitsNumber / layerNumber);

    // check the upper layers, if they do not exist, return false.
    if (startLayer != layerNumber) {
      for (int i = layerNumber; i > startLayer; i--) {
        if (!bitSet.get(monotoneHash(uMin, i))) {
          return false;
        }
      }
    }

    int bits = valueBitsNumber / layerNumber;
    // interval of Trace Tree on specified layer.
    UnsignedLong intervalOfCurrentLevel = UnsignedLong.ONE.bitShiftLeft(startLayer * bits - 1);
    UnsignedLong mask = mask(startLayer);
    UnsignedLong end = uMin.bitAnd(mask).plus(intervalOfCurrentLevel.minus(UnsignedLong.ONE));
    while (end.compareTo(uMin) < 0) { // min is in the second part of TraceTree, switch end to next interval.
      end = end.plus(intervalOfCurrentLevel);
    }
    if (end.compareTo(uMax) >= 0) {
      // (min, max) is in a single Trace Tree of current layer.
      return existsInInterval(uMin, uMax, startLayer);
    } else {
      // (min, max) cross 2 different Trace Tree of current layer.
      return existsInInterval(uMin, end, startLayer) || existsInInterval(end.plus(UnsignedLong.ONE), uMax, startLayer);
    }
  }

  // (min, max) must be in a single Trace Tree of input layer.
  private boolean existsInInterval(UnsignedLong min, UnsignedLong max, int layer) {
    if (min.compareTo(maxValue) > 0) {
      return false;
    } else if (min.equals(max)) {
      return bitSet.get(monotoneHash(min, layer));
    } else if (layer == 1) { // leaf layer of Trace Tree
      int minMonotoneHash = monotoneHash(min, layer);
      int maxMonotoneHash = monotoneHash(max, layer);
      if (minMonotoneHash > maxMonotoneHash) {
        LOG.error("Something is wrong, monotoneHash value:{} for min:{} is bigger than monotoneHash value:{} for" +
            " max:{} on layer:{}", minMonotoneHash, min, maxMonotoneHash, max, layer);
        return true;
      } else if (minMonotoneHash == maxMonotoneHash) {
        return bitSet.get(minMonotoneHash);
      } else {
        // the monotone hash would keep partial order inside a single TraceTree, so we could just use the
        // min/max monotone hash values to verify whether the whole range exists in bit set.
        return !bitSet.get(minMonotoneHash, maxMonotoneHash + 1).isEmpty();
      }
    } else {
      int minMonotoneHash = monotoneHash(min, layer);
      int maxMonotoneHash = monotoneHash(max, layer);
      if (minMonotoneHash > maxMonotoneHash) {
        LOG.error("Something is wrong, monotoneHash value:{} for min:{} is bigger than monotoneHash value:{} for" +
            " max:{} on layer:{}", minMonotoneHash, min, maxMonotoneHash, max, layer);
        return true;
      } else if (bitSet.get(minMonotoneHash, maxMonotoneHash + 1).isEmpty()) {
        // not exists in this layer, we do not need to verify in deeper layers.
        return false;
      } else {
        // TODO if bitSet is true in more than threshold bits, fast return true as it may exists with high possibility.
        // break interval into small ones to verify in next TraceTree layer, and we can skip the Trace Tree if it's 0
        // in current layer.
        BitSet subBitSet = bitSet.get(minMonotoneHash, maxMonotoneHash + 1);
        int nextLayer = layer - 1;
        UnsignedLong intervalOfNextLevel =
            UnsignedLong.ONE.bitShiftLeft(nextLayer * (valueBitsNumber / layerNumber) - 1);
        UnsignedLong mask = mask(nextLayer);
        UnsignedLong start = min.bitAnd(mask);
        int bits = maxMonotoneHash - minMonotoneHash + 1;

        for (int i = 0; i < 2 * bits; i++) {
          UnsignedLong next = start.plus(intervalOfNextLevel.minus(UnsignedLong.ONE));
          if (next.compareTo(max) > 0) {
            next = max;
          }
          if (next.compareTo(max(min, start)) >= 0 && subBitSet.get(i / 2)) {
            if (existsInInterval(max(min, start), next, nextLayer)) {
              return true;
            }
          }
          if (next.compareTo(max) == 0) {
            break;
          }
          start = next.plus(UnsignedLong.ONE);
        }
        return false;
      }
    }
  }

  private UnsignedLong mask(int layer) {
    int shiftBits = layer * (valueBitsNumber / layerNumber);
    if (shiftBits == 64) {
      return UnsignedLong.ZERO;
    } else {
      return UnsignedLong.MAX_VALUE.bitShiftRight(shiftBits).bitShiftLeft(shiftBits);
    }
  }

  private UnsignedLong max(UnsignedLong first, UnsignedLong second) {
    return first.compareTo(second) > 0 ? first : second;
  }

  // MonotoneHash: PrimeHash(value >> (layer * (interval + 1) - 1) mod (bitSize >> interval)) << interval +
  // (value >> (layer - 1) * (interval + 1)) mod (1 << interval)
  private int monotoneHash(UnsignedLong value, int layer) {
    int intervalSize = 1 << intervalBits;
    int shift1 = layer * (intervalBits + 1) - 1;
    int shift2 = (layer - 1) * (intervalBits + 1);
    return primeHash(value.bitShiftRight(shift1), layer)
        .mod(UnsignedLong.valueOf(bitSize >>> intervalBits))
        .times(UnsignedLong.valueOf(intervalSize))
        .plus(value.bitShiftRight(shift2).mod(UnsignedLong.valueOf(intervalSize)))
        .intValue();
  }

  // PrimeHash: Prime1 * value + Prime2
  private UnsignedLong primeHash(UnsignedLong value, int layer) {
    UnsignedLong result = value.times(UnsignedLong.valueOf(selectedPrimes[2 * (layer - 1)]))
        .plus(UnsignedLong.valueOf(selectedPrimes[2 * (layer - 1) + 1]));
    return result;
  }

  private int bitNumber(UnsignedLong value, int traceTreeBits) {
    int bitNumber = getFirstBitPosition(value);
    if (bitNumber < traceTreeBits) {
      bitNumber = traceTreeBits;
    } else if (bitNumber % traceTreeBits != 0) {
      bitNumber = bitNumber + (traceTreeBits - bitNumber % traceTreeBits);
    }
    Preconditions.checkArgument(bitNumber <= 64, "invalid traceTreeBits");
    return bitNumber;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("bitSize: ").append(bitSize).append("\n")
        .append("intervalBits: ").append(intervalBits).append("\n")
        .append("layerNumber: ").append(layerNumber).append("\n")
        .append("valueBitsNumber: ").append(valueBitsNumber).append("\n")
        .append("maxValue: ").append(maxValue.longValue()).append("\n")
        .append("bit Array: ").append(bitSet).append("\n")
        .append("selectedPrimes: ").append(Arrays.toString(selectedPrimes)).append("\n");
    return sb.toString();
  }

  public int estimateMemoryUsage() {
    return 4 + 4 + 4 + 4 + 8 + 4 + this.bitSet.toLongArray().length * 8 + 4 + this.selectedPrimes.length * 4;
  }

  // return the first true bit position(from right to left)
  private static int getFirstBitPosition(UnsignedLong value) {
    for (int i = 1; i <= 64; i++) {
      if (value.bitShiftRight(i).equals(UnsignedLong.ZERO)) {
        return i - 1;
      }
    }
    return 64;
  }
}
