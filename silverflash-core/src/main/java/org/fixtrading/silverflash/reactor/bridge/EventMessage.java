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

import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.Topics;

/**
 * Serializes events as Topic plus message payload
 * 
 * @author Don Mendelson
 *
 */
class EventMessage {

  public static final int SCHEMA_ID = 32001;
  public static final int SCHEMA_VERSION = 0;
  public static final int MESSAGE_TYPE = 1;

  private ByteBuffer buffer;
  private int offset;
  private int variableLength = 0;
  private final MessageHeaderWithFrame header = new MessageHeaderWithFrame();

  public EventMessage attachForEncode(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;

    MessageHeaderWithFrame.encode(buffer, offset, 0, MESSAGE_TYPE, SCHEMA_ID, SCHEMA_VERSION, 0);
    buffer.position(MessageHeaderWithFrame.getLength());
    variableLength = 0;
    return this;
  }

  public EventMessage setTopic(Topic topic) throws UnsupportedEncodingException {
    byte[] bytes = topic.toString().getBytes("UTF-8");
    buffer.putShort(offset + MessageHeaderWithFrame.getLength(), (short) bytes.length);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + 2);
    buffer.put(bytes, 0, bytes.length);
    this.variableLength = bytes.length + 2;
    return this;
  }

  public EventMessage setPayload(ByteBuffer payload) {
    final int length = payload.remaining();
    buffer.putShort(offset + MessageHeaderWithFrame.getLength() + this.variableLength,
        (short) length);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + this.variableLength + 2);
    buffer.put(payload);
    this.variableLength += (length + 2);
    MessageHeaderWithFrame.encodeMessageLength(buffer, offset, MessageHeaderWithFrame.getLength()
        + variableLength);
    return this;
  }

  public EventMessage attachForDecode(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
    header.attachForDecode(buffer, offset);
    if (MESSAGE_TYPE != header.getTemplateId() && SCHEMA_ID != header.getSchemaId()) {
      return null;
    }
    buffer.position(MessageHeaderWithFrame.getLength());
    variableLength = 0;
    return this;
  }

  public Topic getTopic() throws UnsupportedEncodingException {
    short length = buffer.getShort(offset + MessageHeaderWithFrame.getLength());
    this.variableLength += (length + 2);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + 2);
    byte[] dest = new byte[length];
    buffer.get(dest, 0, length);
    String s = new String(dest, "UTF-8");
    return Topics.parse(s);
  }

  public EventMessage getPayload(ByteBuffer payload) {
    short length = buffer.getShort(offset + MessageHeaderWithFrame.getLength() + variableLength);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + this.variableLength + 2);
    payload.clear();
    payload.put(buffer);
    return this;
  }
}
