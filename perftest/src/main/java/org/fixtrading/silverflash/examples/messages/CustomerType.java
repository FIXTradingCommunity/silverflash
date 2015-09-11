/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum CustomerType {
  Retail((byte) 82), NotRetail((byte) 78), NULL_VAL((byte) 0);

  private final byte value;

  CustomerType(final byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public static CustomerType get(final byte value) {
    switch (value) {
      case 82:
        return Retail;
      case 78:
        return NotRetail;
    }

    if ((byte) 0 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
