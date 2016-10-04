package io.fixprotocol.silverflash.fixp.flow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.fixp.messages.FlowType;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.fixp.messages.TopicEncoder;

/**
 * Sends messages on an idempotent flow on a Transport that guarantees FIFO delivery. The
 * implementation sends a Sequence message only at startup and for heartbeats. Additionally, it
 * periodically resends a Topic message for late joiners.
 * 
 * @author Don Mendelson
 */
public class IdempotentFlowSenderWithTopic extends IdempotentFlowSender {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends IdempotentFlowSenderWithTopic, B extends IdempotentFlowSender.Builder<IdempotentFlowSender, FlowBuilder>>
      extends IdempotentFlowSender.Builder {

    private String topic;

    public IdempotentFlowSenderWithTopic build() {
      return new IdempotentFlowSenderWithTopic(this);
    }

    @SuppressWarnings("unchecked")
    public B withTopic(String topic) {
      this.topic = topic;
      return (B) this;
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder<IdempotentFlowSenderWithTopic, IdempotentFlowSender.Builder<IdempotentFlowSender, FlowBuilder>> builder() {
    return new Builder<IdempotentFlowSenderWithTopic, IdempotentFlowSender.Builder<IdempotentFlowSender, FlowBuilder>>();
  }

  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final ByteBuffer sendBuffer =
      ByteBuffer.allocateDirect(128).order(ByteOrder.nativeOrder());
  private final TopicEncoder topicEncoder = new TopicEncoder();
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);

  protected IdempotentFlowSenderWithTopic(Builder builder) {
    super(builder);
    final String topic = builder.topic;
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(topicEncoder.sbeBlockLength())
        .templateId(topicEncoder.sbeTemplateId()).schemaId(topicEncoder.sbeSchemaId())
        .version(topicEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    topicEncoder.wrap(mutableBuffer, offset);

    topicEncoder.flow(FlowType.Idempotent);
    for (int i = 0; i < 16; i++) {
      topicEncoder.sessionId(i, uuidAsBytes[i]);
    }
    topicEncoder.classification(topic);
    frameEncoder.setMessageLength(offset + topicEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
  }

  @Override
  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue()) {
      send(sendBuffer);
    }
  }

}
