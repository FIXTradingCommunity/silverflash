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
