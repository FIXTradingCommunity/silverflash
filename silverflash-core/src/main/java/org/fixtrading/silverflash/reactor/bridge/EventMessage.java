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

package org.fixtrading.silverflash.reactor.bridge;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.Topics;

/**
 * Serializes events as Topic plus message payload
 * 
 * @author Don Mendelson
 *
 */
class EventMessage {

  public static final int MESSAGE_TYPE = 1;
  public static final int SCHEMA_ID = 32001;
  public static final int SCHEMA_VERSION = 0;

  private ByteBuffer buffer;
  private final MessageLengthFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
  private int offset;
  private final SbeMessageHeaderDecoder sbeDecoder = new SbeMessageHeaderDecoder();
  private final SbeMessageHeaderEncoder sbeMessageHeaderEncoder = new SbeMessageHeaderEncoder();
  private int variableLength = 0;

  public EventMessage attachForDecode(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
    sbeDecoder.wrap(buffer, offset);
    if (MESSAGE_TYPE != sbeDecoder.getTemplateId() && SCHEMA_ID != sbeDecoder.getSchemaId()) {
      return null;
    }
    buffer.position(SbeMessageHeaderDecoder.getLength());
    variableLength = 0;
    return this;
  }

  public EventMessage attachForEncode(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
   
    frameEncoder.wrap(buffer);
    frameEncoder.encodeFrameHeader();

    sbeMessageHeaderEncoder.wrap(buffer, offset + frameEncoder.getHeaderLength());
    sbeMessageHeaderEncoder.setBlockLength(0)
        .setTemplateId(MESSAGE_TYPE).setSchemaId(SCHEMA_ID)
        .getSchemaVersion(SCHEMA_VERSION);

    
    variableLength = 0;
    return this;
  }

  public EventMessage getPayload(ByteBuffer payload) {
    short length = buffer.getShort(offset + SbeMessageHeaderDecoder.getLength() + variableLength);
    buffer.position(offset + SbeMessageHeaderDecoder.getLength() + this.variableLength + 2);
    payload.clear();
    payload.put(buffer);
    return this;
  }

  public Topic getTopic() throws UnsupportedEncodingException {
    short length = buffer.getShort(offset + SbeMessageHeaderDecoder.getLength());
    this.variableLength += (length + 2);
    buffer.position(offset + SbeMessageHeaderDecoder.getLength() + 2);
    byte[] dest = new byte[length];
    buffer.get(dest, 0, length);
    String s = new String(dest, "UTF-8");
    return Topics.parse(s);
  }

  public EventMessage setPayload(ByteBuffer payload) {
    final int length = payload.remaining();
    buffer.putShort(offset + SbeMessageHeaderDecoder.getLength() + this.variableLength,
        (short) length);
    buffer.position(offset + SbeMessageHeaderDecoder.getLength() + this.variableLength + 2);
    buffer.put(payload);
    this.variableLength += (length + 2);
    frameEncoder.setMessageLength(SbeMessageHeaderDecoder.getLength()
        + variableLength);
    frameEncoder.encodeFrameTrailer();

    return this;
  }

  public EventMessage setTopic(Topic topic) throws UnsupportedEncodingException {
    byte[] bytes = topic.toString().getBytes("UTF-8");
    buffer.putShort(offset + SbeMessageHeaderDecoder.getLength(), (short) bytes.length);
    buffer.position(offset + SbeMessageHeaderDecoder.getLength() + 2);
    buffer.put(bytes, 0, bytes.length);
    this.variableLength = bytes.length + 2;
    return this;
  }
}
