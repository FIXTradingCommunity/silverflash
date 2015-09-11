/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum CrossType {
  NoCross((byte) 49), OpeningCross((byte) 50), ClosingCross((byte) 51), Halt((byte) 52), SupplementalOrder(
      (byte) 53), NULL_VAL((byte) 0);

  private final byte value;

  CrossType(final byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public static CrossType get(final byte value) {
    switch (value) {
      case 49:
        return NoCross;
      case 50:
        return OpeningCross;
      case 51:
        return ClosingCross;
      case 52:
        return Halt;
      case 53:
        return SupplementalOrder;
    }

    if ((byte) 0 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
