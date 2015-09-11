package org.fixtrading.silverflash.reactor.bridge;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.Topics;
import org.fixtrading.silverflash.reactor.bridge.EventReactorWithBridge;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
public class EventReactorWithBridgeTest {

  int received = 0;
  private Engine engine;
  private PipeTransport memoryTransport;
  private EventReactorWithBridge reactor1;
  private EventReactorWithBridge reactor2;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    received = 0;

    engine = Engine.builder().build();
    engine.open();
    memoryTransport = new PipeTransport(engine.getIOReactor().getSelector());

    reactor1 =
        EventReactorWithBridge.builder().withTransport(memoryTransport.getClientTransport())
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    reactor2 =
        EventReactorWithBridge.builder().withTransport(memoryTransport.getServerTransport())
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();

    // reactor1.setTrace(true, "reactor1");
    // reactor2.setTrace(true, "reactor2");
    reactor1.open();
    reactor2.open();
  }

  @After
  public void tearDown() throws Exception {
    reactor1.close();
    reactor2.close();
  }

  @Test
  public void testForward() throws Exception {
    Topic topic = Topics.getTopic("test1");

    reactor1.forward(topic);

    Receiver receiver2 = new Receiver() {

      public void accept(ByteBuffer buf) {
        received++;
      }
    };
    Subscription subscription2 = reactor2.subscribe(topic, receiver2);

    ByteBuffer msg = ByteBuffer.allocate(1024);
    msg.put("This is a test".getBytes());
    reactor1.post(topic, msg);

    Thread.sleep(500L);
    assertEquals(1, received);
  }

}
