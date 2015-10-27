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
package org.fixtrading.silverflash.fixp.messages;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiateDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.NegotiateEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameSpliterator;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Donald
 *
 */
public class MessageEncoderTest {

  private MessageEncoder encoder;
  private MessageDecoder decoder;
  private ByteBuffer buffer;
  private MessageLengthFrameSpliterator framer;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    encoder = new MessageEncoder(MessageLengthFrameEncoder.class);
    framer = new MessageLengthFrameSpliterator();
    decoder = new MessageDecoder();
    buffer = ByteBuffer.allocate(1024);
  }

  @Test
  public void encodeAndDecode() {
    UUID uuid = SessionId.generateUUID();
    byte[] bytes = SessionId.UUIDAsBytes(uuid);
    long nanotime = System.nanoTime();
    byte[] credentials = "Alexander".getBytes();
   
    NegotiateEncoder negotiateEncoder =
        (NegotiateEncoder) encoder.attachForEncode(buffer, 0, MessageType.NEGOTIATE);
    negotiateEncoder.setTimestamp(nanotime);
    negotiateEncoder.setSessionId(bytes);
    negotiateEncoder.setClientFlow(FlowType.IDEMPOTENT);
    negotiateEncoder.setCredentials(credentials);
    
    framer.wrap(buffer);
    framer.tryAdvance(new Consumer<ByteBuffer>(){

      @Override
      public void accept(ByteBuffer buf) {
        Optional<Decoder> opt = decoder.attachForDecode(buf, 0);
        if (opt.isPresent()) {
          final Decoder decoder = opt.get();
          assertEquals(MessageType.NEGOTIATE, decoder.getMessageType());
          NegotiateDecoder negotiateDecoder = (NegotiateDecoder) decoder;
          assertEquals(FlowType.IDEMPOTENT, negotiateDecoder.getClientFlow());
          
          byte[] dest = new byte[credentials.length];
          negotiateDecoder.getCredentials(dest , 0);
          assertArrayEquals(credentials, dest);
      }
      
    }
  });
  }
}
