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
