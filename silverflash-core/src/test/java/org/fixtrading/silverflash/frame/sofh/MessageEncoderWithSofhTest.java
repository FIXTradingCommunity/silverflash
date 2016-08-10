/**
 *    Copyright 2015-2016 FIX Protocol Ltd
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
package org.fixtrading.silverflash.frame.sofh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.EstablishDecoder;
import org.fixtrading.silverflash.fixp.messages.EstablishEncoder;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.NegotiateDecoder;
import org.fixtrading.silverflash.fixp.messages.NegotiateEncoder;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
public class MessageEncoderWithSofhTest {

  private static final String credentials = "Alexander";
  private ByteBuffer buffer;
  private final EstablishDecoder establishDecoder = new EstablishDecoder();
  private final EstablishEncoder establishEncoder = new EstablishEncoder();
  private final SofhFrameEncoder frameEncoder = new SofhFrameEncoder();
  private SofhFrameSpliterator framer;
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private MutableDirectBuffer mutableBuffer;
  private final NegotiateDecoder negotiateDecoder = new NegotiateDecoder();
  private final NegotiateEncoder negotiateEncoder = new NegotiateEncoder();

  @Test
  public void encodeAndDecode() {
    UUID uuid = SessionId.generateUUID();
    byte[] uuidAsBytes = SessionId.UUIDAsBytes(uuid);
    long nanotime = System.nanoTime();

    int offset = 0;
    frameEncoder.wrap(buffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(negotiateEncoder.sbeBlockLength())
        .templateId(negotiateEncoder.sbeTemplateId()).schemaId(negotiateEncoder.sbeSchemaId())
        .version(negotiateEncoder.sbeSchemaVersion());
    offset += MessageHeaderEncoder.ENCODED_LENGTH;
    negotiateEncoder.wrap(mutableBuffer, offset);
    negotiateEncoder.timestamp(nanotime);
    for (int i = 0; i < 16; i++) {
      negotiateEncoder.sessionId(i, uuidAsBytes[i]);
    }
    negotiateEncoder.clientFlow(FlowType.Idempotent);
    negotiateEncoder.credentials(credentials);
    offset += negotiateEncoder.sbeBlockLength();
    frameEncoder.encodeFrameTrailer();
    
    frameEncoder.wrap(buffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(establishEncoder.sbeBlockLength())
        .templateId(establishEncoder.sbeTemplateId()).schemaId(establishEncoder.sbeSchemaId())
        .version(establishEncoder.sbeSchemaVersion());
    offset += MessageHeaderEncoder.ENCODED_LENGTH;
    establishEncoder.wrap(mutableBuffer, offset);

    establishEncoder.nextSeqNo(888);
    establishEncoder.timestamp(nanotime);
    for (int i = 0; i < 16; i++) {
      establishEncoder.sessionId(i, uuidAsBytes[i]);
    }
    establishEncoder.credentials(credentials);
    frameEncoder.encodeFrameTrailer();

    buffer.rewind();
    framer.wrap(buffer);
    framer.tryAdvance(new Consumer<ByteBuffer>() {

      @Override
      public void accept(ByteBuffer buf) { 
        immutableBuffer.wrap(buffer);
        messageHeaderDecoder.wrap(immutableBuffer, buffer.position());
        if (messageHeaderDecoder.schemaId() == negotiateDecoder.sbeSchemaId()) {

          switch (messageHeaderDecoder.templateId()) {
          case NegotiateDecoder.TEMPLATE_ID:
            assertEquals(FlowType.Idempotent, negotiateDecoder.clientFlow());
            assertEquals(credentials, negotiateDecoder.credentials());
            break;
          case EstablishDecoder.TEMPLATE_ID:
            assertEquals(888, establishDecoder.nextSeqNo());
            assertEquals(credentials, establishDecoder.credentials());
          default:
            fail("Unexpected message type");
          }
        }

      }
    });
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    framer = new SofhFrameSpliterator();
    buffer = ByteBuffer.allocate(2048);
    mutableBuffer = new UnsafeBuffer(buffer);

  }
}
