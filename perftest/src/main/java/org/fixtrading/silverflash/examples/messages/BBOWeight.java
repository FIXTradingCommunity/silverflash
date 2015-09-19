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
