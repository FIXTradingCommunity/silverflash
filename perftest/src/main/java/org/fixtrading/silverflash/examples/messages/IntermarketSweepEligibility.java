/* Generated SBE (Simple Binary Encoding) message codec */
package org.fixtrading.silverflash.examples.messages;

public enum IntermarketSweepEligibility {
  Eligible((short) 1), NotEligible((short) 2), NULL_VAL((short) 255);

  private final short value;

  IntermarketSweepEligibility(final short value) {
    this.value = value;
  }

  public short value() {
    return value;
  }

  public static IntermarketSweepEligibility get(final short value) {
    switch (value) {
      case 1:
        return Eligible;
      case 2:
        return NotEligible;
    }

    if ((short) 255 == value) {
      return NULL_VAL;
    }

    throw new IllegalArgumentException("Unknown value: " + value);
  }
}
