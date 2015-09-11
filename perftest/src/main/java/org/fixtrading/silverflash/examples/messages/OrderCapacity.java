/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum OrderCapacity {
  Agency((byte) 49), Principal((byte) 50), Riskless((byte) 51), NULL_VAL((byte) 0);

  private final byte value;

  OrderCapacity(final byte value) {
    this.value = value;
  }

  public byte value() {
    return value;
  }

  public static OrderCapacity get(final byte value) {
    switch (value) {
      case 49:
        return Agency;
      case 50:
        return Principal;
      case 51:
        return Riskless;
    }

    if ((byte) 0 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
