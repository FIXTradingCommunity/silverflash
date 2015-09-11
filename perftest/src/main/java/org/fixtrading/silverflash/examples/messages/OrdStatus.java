/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum OrdStatus {
  New((byte) 48), PartiallyFilled((byte) 49), Filled((byte) 50), DoneForDay((byte) 51), Canceled(
      (byte) 52), NULL_VAL((byte) 0);

  private final byte value;

  OrdStatus(final byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public static OrdStatus get(final byte value) {
    switch (value) {
      case 48:
        return New;
      case 49:
        return PartiallyFilled;
      case 50:
        return Filled;
      case 51:
        return DoneForDay;
      case 52:
        return Canceled;
    }

    if ((byte) 0 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
