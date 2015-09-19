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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.ContextDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.EstablishDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.EstablishmentAckDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.FinishedReceivingDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.FinishedSendingDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiateDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiationRejectDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiationResponseDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.RetransmissionDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.TerminateDecoder;

/**
 * Retrieves session ID from a FIXP session message
 * 
 * @author Don Mendelson
 *
 */
public class MessageSessionIdentifier implements Function<ByteBuffer, UUID> {

  private final MessageDecoder decoder = new MessageDecoder();
  private final byte[] sessionId = new byte[16];
  private Consumer<UUID> newSessionConsumer;

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  public UUID apply(ByteBuffer buffer) {
    int pos = buffer.position();
    boolean newSession = false;
    try {
      Optional<Decoder> optDecoder = decoder.attachForDecode(buffer, pos);
      if (optDecoder.isPresent()) {
        final Decoder decoder = optDecoder.get();
        switch (decoder.getMessageType()) {
          case APPLIED:
          case NOT_APPLIED:
          case SEQUENCE:
          case UNSEQUENCED_HEARTBEAT:
            // Invalid on a multiplexed transport or application
            // messages
            return null;
          case NEGOTIATE:
            ((NegotiateDecoder) decoder).getSessionId(sessionId, 0);
            newSession = true;
            break;
          case NEGOTIATION_RESPONSE:
            ((NegotiationResponseDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case NEGOTIATION_REJECT:
            ((NegotiationRejectDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case ESTABLISH:
            ((EstablishDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case ESTABLISHMENT_ACK:
            ((EstablishmentAckDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case ESTABLISHMENT_REJECT:
            break;
          case TERMINATE:
            ((TerminateDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case RETRANSMIT_REQUEST:
            break;
          case RETRANSMISSION:
            ((RetransmissionDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case FINISHED_SENDING:
            ((FinishedSendingDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case FINISHED_RECEIVING:
            ((FinishedReceivingDecoder) decoder).getSessionId(sessionId, 0);
            break;
          case CONTEXT:
            ((ContextDecoder) decoder).getSessionId(sessionId, 0);
            break;
          default:
            return null;
        }
        UUID uuid = SessionId.UUIDFromBytes(sessionId);
        if (newSession && (newSessionConsumer != null)) {
          newSessionConsumer.accept(uuid);
        }
        return uuid;
      }
      return null;
    } finally {
      // Idempotent for buffer position
      buffer.position(pos);
    }
  }

  public MessageSessionIdentifier withNewSessionConsumer(Consumer<UUID> newSessionConsumer) {
    this.newSessionConsumer = newSessionConsumer;
    return this;
  }
}
