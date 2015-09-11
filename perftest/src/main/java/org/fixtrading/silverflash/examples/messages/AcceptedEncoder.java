/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

import uk.co.real_logic.sbe.codec.java.*;
import uk.co.real_logic.agrona.MutableDirectBuffer;

public class AcceptedEncoder {
  public static final int BLOCK_LENGTH = 73;
  public static final int TEMPLATE_ID = 2;
  public static final int SCHEMA_ID = 2;
  public static final int SCHEMA_VERSION = 2;

  private final AcceptedEncoder parentMessage = this;
  private MutableDirectBuffer buffer;
  protected int offset;
  protected int limit;
  protected int actingBlockLength;
  protected int actingVersion;

  public int sbeBlockLength() {
    return BLOCK_LENGTH;
  }

  public int sbeTemplateId() {
    return TEMPLATE_ID;
  }

  public int sbeSchemaId() {
    return SCHEMA_ID;
  }

  public int sbeSchemaVersion() {
    return SCHEMA_VERSION;
  }

  public String sbeSemanticType() {
    return "A";
  }

  public int offset() {
    return offset;
  }

  public AcceptedEncoder wrap(final MutableDirectBuffer buffer, final int offset) {
    this.buffer = buffer;
    this.offset = offset;
    limit(offset + BLOCK_LENGTH);
    return this;
  }

  public int encodedLength() {
    return limit - offset;
  }

  public int limit() {
    return limit;
  }

  public void limit(final int limit) {
    buffer.checkLimit(limit);
    this.limit = limit;
  }

  public static long transactTimeNullValue() {
    return 0xffffffffffffffffL;
  }

  public static long transactTimeMinValue() {
    return 0x0L;
  }

  public static long transactTimeMaxValue() {
    return 0xfffffffffffffffeL;
  }

  public AcceptedEncoder transactTime(final long value) {
    CodecUtil.uint64Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static byte clOrdIdNullValue() {
    return (byte) 0;
  }

  public static byte clOrdIdMinValue() {
    return (byte) 32;
  }

  public static byte clOrdIdMaxValue() {
    return (byte) 126;
  }

  public static int clOrdIdLength() {
    return 14;
  }

  public void clOrdId(final int index, final byte value) {
    if (index < 0 || index >= 14) {
      throw new IndexOutOfBoundsException("index out of range: index=" + index);
    }

    CodecUtil.charPut(buffer, this.offset + 8 + (index * 1), value);
  }

  public static String clOrdIdCharacterEncoding() {
    return "UTF-8";
  }

  public AcceptedEncoder putClOrdId(final byte[] src, final int srcOffset) {
    final int length = 14;
    if (srcOffset < 0 || srcOffset > (src.length - length)) {
      throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
    }

    CodecUtil.charsPut(buffer, this.offset + 8, src, srcOffset, length);
    return this;
  }

  public AcceptedEncoder side(final Side value) {
    CodecUtil.charPut(buffer, offset + 22, value.value());
    return this;
  }

  public static long orderQtyNullValue() {
    return 4294967294L;
  }

  public static long orderQtyMinValue() {
    return 0L;
  }

  public static long orderQtyMaxValue() {
    return 4294967293L;
  }

  public AcceptedEncoder orderQty(final long value) {
    CodecUtil.uint32Put(buffer, offset + 23, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static byte symbolNullValue() {
    return (byte) 0;
  }

  public static byte symbolMinValue() {
    return (byte) 32;
  }

  public static byte symbolMaxValue() {
    return (byte) 126;
  }

  public static int symbolLength() {
    return 8;
  }

  public void symbol(final int index, final byte value) {
    if (index < 0 || index >= 8) {
      throw new IndexOutOfBoundsException("index out of range: index=" + index);
    }

    CodecUtil.charPut(buffer, this.offset + 27 + (index * 1), value);
  }

  public static String symbolCharacterEncoding() {
    return "UTF-8";
  }

  public AcceptedEncoder putSymbol(final byte[] src, final int srcOffset) {
    final int length = 8;
    if (srcOffset < 0 || srcOffset > (src.length - length)) {
      throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
    }

    CodecUtil.charsPut(buffer, this.offset + 27, src, srcOffset, length);
    return this;
  }

  private final Decimal32Encoder price = new Decimal32Encoder();

  public Decimal32Encoder price() {
    price.wrap(buffer, offset + 35);
    return price;
  }

  public static long expireTimeNullValue() {
    return 4294967294L;
  }

  public static long expireTimeMinValue() {
    return 0L;
  }

  public static long expireTimeMaxValue() {
    return 4294967293L;
  }

  public AcceptedEncoder expireTime(final long value) {
    CodecUtil.uint32Put(buffer, offset + 39, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public static byte clientIDNullValue() {
    return (byte) 0;
  }

  public static byte clientIDMinValue() {
    return (byte) 32;
  }

  public static byte clientIDMaxValue() {
    return (byte) 126;
  }

  public static int clientIDLength() {
    return 4;
  }

  public void clientID(final int index, final byte value) {
    if (index < 0 || index >= 4) {
      throw new IndexOutOfBoundsException("index out of range: index=" + index);
    }

    CodecUtil.charPut(buffer, this.offset + 43 + (index * 1), value);
  }

  public static String clientIDCharacterEncoding() {
    return "UTF-8";
  }

  public AcceptedEncoder putClientID(final byte[] src, final int srcOffset) {
    final int length = 4;
    if (srcOffset < 0 || srcOffset > (src.length - length)) {
      throw new IndexOutOfBoundsException("srcOffset out of range for copy: offset=" + srcOffset);
    }

    CodecUtil.charsPut(buffer, this.offset + 43, src, srcOffset, length);
    return this;
  }

  public AcceptedEncoder display(final Display value) {
    CodecUtil.uint8Put(buffer, offset + 47, value.value());
    return this;
  }

  public static long orderIdNullValue() {
    return 0xffffffffffffffffL;
  }

  public static long orderIdMinValue() {
    return 0x0L;
  }

  public static long orderIdMaxValue() {
    return 0xfffffffffffffffeL;
  }

  public AcceptedEncoder orderId(final long value) {
    CodecUtil.uint64Put(buffer, offset + 48, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public AcceptedEncoder orderCapacity(final OrderCapacity value) {
    CodecUtil.charPut(buffer, offset + 56, value.value());
    return this;
  }

  public AcceptedEncoder intermarketSweepEligibility(final IntermarketSweepEligibility value) {
    CodecUtil.uint8Put(buffer, offset + 57, value.value());
    return this;
  }

  public static long minimumQuantityNullValue() {
    return 4294967294L;
  }

  public static long minimumQuantityMinValue() {
    return 0L;
  }

  public static long minimumQuantityMaxValue() {
    return 4294967293L;
  }

  public AcceptedEncoder minimumQuantity(final long value) {
    CodecUtil.uint32Put(buffer, offset + 58, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }

  public AcceptedEncoder crossType(final CrossType value) {
    CodecUtil.charPut(buffer, offset + 62, value.value());
    return this;
  }

  public AcceptedEncoder ordStatus(final OrdStatus value) {
    CodecUtil.charPut(buffer, offset + 63, value.value());
    return this;
  }

  public AcceptedEncoder bBOWeightIndicator(final BBOWeight value) {
    CodecUtil.charPut(buffer, offset + 64, value.value());
    return this;
  }

  public static long orderEntryTimeNullValue() {
    return 0xffffffffffffffffL;
  }

  public static long orderEntryTimeMinValue() {
    return 0x0L;
  }

  public static long orderEntryTimeMaxValue() {
    return 0xfffffffffffffffeL;
  }

  public AcceptedEncoder orderEntryTime(final long value) {
    CodecUtil.uint64Put(buffer, offset + 65, value, java.nio.ByteOrder.LITTLE_ENDIAN);
    return this;
  }
}
