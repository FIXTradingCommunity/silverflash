/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum Side {
  Buy((byte) 49), Sell((byte) 50), SellShort((byte) 53), SellShortExempt((byte) 52), NULL_VAL(
      (byte) 0);

  private final byte value;

  Side(final byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public static Side get(final byte value) {
    switch (value) {
      case 49:
        return Buy;
      case 50:
        return Sell;
      case 53:
        return SellShort;
      case 52:
        return SellShortExempt;
    }

    if ((byte) 0 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
