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
import uk.co.real_logic.agrona.DirectBuffer;

public class AcceptedDecoder {
  public static final int BLOCK_LENGTH = 73;
  public static final int TEMPLATE_ID = 2;
  public static final int SCHEMA_ID = 2;
  public static final int SCHEMA_VERSION = 2;

  private final AcceptedDecoder parentMessage = this;
  private DirectBuffer buffer;
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

  public AcceptedDecoder wrap(final DirectBuffer buffer, final int offset,
      final int actingBlockLength, final int actingVersion) {
    this.buffer = buffer;
    this.offset = offset;
    this.actingBlockLength = actingBlockLength;
    this.actingVersion = actingVersion;
    limit(offset + actingBlockLength);

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

  public static int TransactTimeId() {
    return 60;
  }

  public static String TransactTimeMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public long transactTime() {
    return CodecUtil.uint64Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
  }


  public static int ClOrdIdId() {
    return 11;
  }

  public static String ClOrdIdMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public byte clOrdId(final int index) {
    if (index < 0 || index >= 14) {
      throw new IndexOutOfBoundsException("index out of range: index=" + index);
    }

    return CodecUtil.charGet(buffer, this.offset + 8 + (index * 1));
  }


  public static String clOrdIdCharacterEncoding() {
    return "UTF-8";
  }

  public int getClOrdId(final byte[] dst, final int dstOffset) {
    final int length = 14;
    if (dstOffset < 0 || dstOffset > (dst.length - length)) {
      throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
    }

    CodecUtil.charsGet(buffer, this.offset + 8, dst, dstOffset, length);
    return length;
  }


  public static int SideId() {
    return 54;
  }

  public static String SideMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public Side side() {
    return Side.get(CodecUtil.charGet(buffer, offset + 22));
  }


  public static int OrderQtyId() {
    return 38;
  }

  public static String OrderQtyMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public long orderQty() {
    return CodecUtil.uint32Get(buffer, offset + 23, java.nio.ByteOrder.LITTLE_ENDIAN);
  }


  public static int SymbolId() {
    return 55;
  }

  public static String SymbolMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public byte symbol(final int index) {
    if (index < 0 || index >= 8) {
      throw new IndexOutOfBoundsException("index out of range: index=" + index);
    }

    return CodecUtil.charGet(buffer, this.offset + 27 + (index * 1));
  }


  public static String symbolCharacterEncoding() {
    return "UTF-8";
  }

  public int getSymbol(final byte[] dst, final int dstOffset) {
    final int length = 8;
    if (dstOffset < 0 || dstOffset > (dst.length - length)) {
      throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
    }

    CodecUtil.charsGet(buffer, this.offset + 27, dst, dstOffset, length);
    return length;
  }


  public static int PriceId() {
    return 44;
  }

  public static String PriceMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  private final Decimal32Decoder price = new Decimal32Decoder();

  public Decimal32Decoder price() {
    price.wrap(buffer, offset + 35);
    return price;
  }

  public static int ExpireTimeId() {
    return 126;
  }

  public static String ExpireTimeMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public long expireTime() {
    return CodecUtil.uint32Get(buffer, offset + 39, java.nio.ByteOrder.LITTLE_ENDIAN);
  }


  public static int ClientIDId() {
    return 109;
  }

  public static String ClientIDMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public byte clientID(final int index) {
    if (index < 0 || index >= 4) {
      throw new IndexOutOfBoundsException("index out of range: index=" + index);
    }

    return CodecUtil.charGet(buffer, this.offset + 43 + (index * 1));
  }


  public static String clientIDCharacterEncoding() {
    return "UTF-8";
  }

  public int getClientID(final byte[] dst, final int dstOffset) {
    final int length = 4;
    if (dstOffset < 0 || dstOffset > (dst.length - length)) {
      throw new IndexOutOfBoundsException("dstOffset out of range for copy: offset=" + dstOffset);
    }

    CodecUtil.charsGet(buffer, this.offset + 43, dst, dstOffset, length);
    return length;
  }


  public static int DisplayId() {
    return 5009;
  }

  public static String DisplayMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public Display display() {
    return Display.get(CodecUtil.uint8Get(buffer, offset + 47));
  }


  public static int OrderIdId() {
    return 37;
  }

  public static String OrderIdMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public long orderId() {
    return CodecUtil.uint64Get(buffer, offset + 48, java.nio.ByteOrder.LITTLE_ENDIAN);
  }


  public static int OrderCapacityId() {
    return 528;
  }

  public static String OrderCapacityMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public OrderCapacity orderCapacity() {
    return OrderCapacity.get(CodecUtil.charGet(buffer, offset + 56));
  }


  public static int IntermarketSweepEligibilityId() {
    return 5011;
  }

  public static String IntermarketSweepEligibilityMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public IntermarketSweepEligibility intermarketSweepEligibility() {
    return IntermarketSweepEligibility.get(CodecUtil.uint8Get(buffer, offset + 57));
  }


  public static int MinimumQuantityId() {
    return 12;
  }

  public static String MinimumQuantityMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public long minimumQuantity() {
    return CodecUtil.uint32Get(buffer, offset + 58, java.nio.ByteOrder.LITTLE_ENDIAN);
  }


  public static int CrossTypeId() {
    return 549;
  }

  public static String CrossTypeMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public CrossType crossType() {
    return CrossType.get(CodecUtil.charGet(buffer, offset + 62));
  }


  public static int OrdStatusId() {
    return 39;
  }

  public static String OrdStatusMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public OrdStatus ordStatus() {
    return OrdStatus.get(CodecUtil.charGet(buffer, offset + 63));
  }


  public static int BBOWeightIndicatorId() {
    return 5012;
  }

  public static String BBOWeightIndicatorMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
  }

  public BBOWeight bBOWeightIndicator() {
    return BBOWeight.get(CodecUtil.charGet(buffer, offset + 64));
  }


  public static int OrderEntryTimeId() {
    return 5013;
  }

  public static String OrderEntryTimeMetaAttribute(final MetaAttribute metaAttribute) {
    switch (metaAttribute) {
      case EPOCH:
        return "unix";
      case TIME_UNIT:
        return "nanosecond";
      case SEMANTIC_TYPE:
        return "";
    }

    return "";
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

  public long orderEntryTime() {
    return CodecUtil.uint64Get(buffer, offset + 65, java.nio.ByteOrder.LITTLE_ENDIAN);
  }

}
