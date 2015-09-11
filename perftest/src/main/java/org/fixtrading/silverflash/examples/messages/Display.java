/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum Display {
  AttributablePrice((short) 1), AnonymousPrice((short) 2), NonDisplay((short) 3), PostOnly(
      (short) 4), NULL_VAL((short) 255);

  private final short value;

  Display(final short value) {
    this.value = value;
  }

  public short value() {
    return value;
  }

  public static Display get(final short value) {
    switch (value) {
      case 1:
        return AttributablePrice;
      case 2:
        return AnonymousPrice;
      case 3:
        return NonDisplay;
      case 4:
        return PostOnly;
    }

    if ((short) 255 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
