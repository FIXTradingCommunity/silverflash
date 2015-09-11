/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum BBOWeight {
  Level0((byte) 48), Level1((byte) 49), Level2((byte) 50), Level3((byte) 51), NULL_VAL((byte) 0);

  private final byte value;

  BBOWeight(final byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public static BBOWeight get(final byte value) {
    switch (value) {
      case 48:
        return Level0;
      case 49:
        return Level1;
      case 50:
        return Level2;
      case 51:
        return Level3;
    }

    if ((byte) 0 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
