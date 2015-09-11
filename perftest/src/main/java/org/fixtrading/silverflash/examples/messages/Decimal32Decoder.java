/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.DirectBuffer;

public class Decimal32Decoder {
  public static final int ENCODED_LENGTH = 4;
  private DirectBuffer buffer;
  private int offset;

  public Decimal32Decoder wrap(final DirectBuffer buffer, final int offset) {
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

  public int mantissa() {
    return CodecUtil.int32Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
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
