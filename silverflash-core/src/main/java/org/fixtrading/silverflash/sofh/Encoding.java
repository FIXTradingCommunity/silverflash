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
package org.fixtrading.silverflash.sofh;

public enum Encoding {
  SBE_1_0_BIG_ENDIAN((short)0x5BE0),
  SBE_1_0_LITTLE_ENDIAN((short)0xEB50),
  FIX_GPB_1_0((short)0x4700),
  FIX_ASN_1_PER_1_0((short)0xA500),
  FIX_ASN_1_BER_1_0((short)0xA501),
  FIX_ASN_1_OER_1_0((short)0xA502),
  FIX_TAG_VALUE((short)0xF000),
  FIXML_SCHEMA((short)0XF100),
  FIX_FAST((short)0xFAFF),
  FIX_JSON((short)0xF500),
  FIX_BSON((short)0xFB00),
  ;
  private short code;

  Encoding(short code) {
    this.code = code;
  }
  
  public short getCode() {
    return this.code;
  }
}