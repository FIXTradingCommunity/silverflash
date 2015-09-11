package org.fixtrading.silverflash.auth.proxy;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;

/**
 * Serializes a request for credentials
 * 
 * @author Don Mendelson
 *
 */
public class CredentialsRequestMessage {

  public static final int SCHEMA_ID = 32002;
  public static final int SCHEMA_VERSION = 0;
  public static final int MESSAGE_TYPE = 1;

  private ByteBuffer buffer;
  private int offset;
  private int variableLength = 0;
  private final MessageHeaderWithFrame header = new MessageHeaderWithFrame();

  public CredentialsRequestMessage attachForEncode(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;

    MessageHeaderWithFrame.encode(buffer, offset, 0, MESSAGE_TYPE, SCHEMA_ID, SCHEMA_VERSION, 0);
    buffer.position(MessageHeaderWithFrame.getLength());
    variableLength = 0;
    return this;
  }

  public CredentialsRequestMessage setName(String name) throws UnsupportedEncodingException {
    byte[] bytes = name.getBytes("UTF-8");
    buffer.putShort(offset + MessageHeaderWithFrame.getLength(), (short) bytes.length);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + 2);
    buffer.put(bytes, 0, bytes.length);
    this.variableLength = bytes.length + 2;
    return this;
  }

  public CredentialsRequestMessage setPassword(char[] key) {
    ByteBuffer keyBuffer = Charset.forName("UTF-8").encode(CharBuffer.wrap(key));
    int length = keyBuffer.remaining();
    buffer.putShort(offset + MessageHeaderWithFrame.getLength() + this.variableLength,
        (short) length);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + this.variableLength + 2);
    buffer.put(keyBuffer);
    this.variableLength += (length + 2);
    MessageHeaderWithFrame.encodeMessageLength(buffer, offset, MessageHeaderWithFrame.getLength()
        + variableLength);
    return this;
  }

  public CredentialsRequestMessage attachForDecode(ByteBuffer buffer, int offset) {
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

  public String getName() throws UnsupportedEncodingException {
    short length = buffer.getShort(offset + MessageHeaderWithFrame.getLength());
    this.variableLength += (length + 2);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + 2);
    byte[] dest = new byte[length];
    buffer.get(dest, 0, length);
    return new String(dest, "UTF-8");
  }

  public char[] getPassword() {
    short length = buffer.getShort(offset + MessageHeaderWithFrame.getLength() + variableLength);
    buffer.position(offset + MessageHeaderWithFrame.getLength() + this.variableLength + 2);
    CharBuffer charBuffer = Charset.forName("UTF-8").decode(buffer);
    char[] dest = new char[length];
    charBuffer.get(dest);
    return dest;
  }
}
