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
