package org.fixtrading.silverflash.fixp.flow;

import org.fixtrading.silverflash.fixp.messages.MessageType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.TopicEncoder;

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

  private final ByteBuffer topicMessageBuffer =
      ByteBuffer.allocateDirect(128).order(ByteOrder.nativeOrder());

  protected IdempotentFlowSenderWithTopic(Builder builder) {
    super(builder);
    final String topic = builder.topic;
    final TopicEncoder topicEncoder =
        (TopicEncoder) messageEncoder.wrap(topicMessageBuffer, 0, MessageType.TOPIC);
    topicEncoder.setFlow(FlowType.IDEMPOTENT);
    topicEncoder.setSessionId(uuidAsBytes);
    topicEncoder.setClassification(topic.getBytes());
  }

  @Override
  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue()) {
      send(topicMessageBuffer);
    }
  }

}
