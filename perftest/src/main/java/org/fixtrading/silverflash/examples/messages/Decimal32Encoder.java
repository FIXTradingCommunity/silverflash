/**
 *    Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.fixtrading.silverflash.examples.messages;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;

public class Decimal32Encoder {
  public static final int ENCODED_LENGTH = 4;
  private MutableDirectBuffer buffer;
  private int offset;

  public Decimal32Encoder wrap(final MutableDirectBuffer buffer, final int offset) {
    this.buffer = buffer;
    this.offset = offset;
    return this;
  }

  public int encodedLength() {
    return ENCODED_LENGTH;
  }

  public static int mantissaNullValue() {
    return -2147483648;
  }

  public static int mantissaMinValue() {
    return -2147483647;
  }

  public static int mantissaMaxValue() {
    return 2147483647;
  }

  public Decimal32Encoder mantissa(final int value) {
    CodecUtil.int32Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static byte exponentNullValue() {
    return (byte) -128;
  }

  public static byte exponentMinValue() {
    return (byte) -127;
  }

  public static byte exponentMaxValue() {
    return (byte) 127;
  }

  public byte exponent() {
    return (byte) -4;
  }
}
