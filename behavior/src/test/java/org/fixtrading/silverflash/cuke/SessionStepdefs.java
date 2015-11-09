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
package org.fixtrading.silverflash.cuke;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.buffer.BufferSupplier;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.FixpSession;
import org.fixtrading.silverflash.fixp.SessionReadyFuture;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NotAppliedDecoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

public class SessionStepdefs {

  private class TestReceiver implements MessageConsumer<UUID> {

    private byte[] message = new byte[16 * 1024];
    private int length = message.length;
    private int totalMessages = 0;

    public ByteBuffer getLastMessage() {
      return ByteBuffer.wrap(message, 0, length).order(ByteOrder.nativeOrder());
    }

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      length = buf.limit() - buf.position();
      buf.get(message, 0, length);
      // System.out.println("Length="+length);
      totalMessages++;
    }

    public int getTotalMessages() {
      return totalMessages;
    }

  }

  private class TestTransportDecorator implements Transport {
    private final Transport component;
    private final boolean isFifo;

    /**
     * Wrap a Transport
     * 
     * @param component a Transport to wrap
     * @param isFifo override attribute of the Transport
     */
    public TestTransportDecorator(Transport component, boolean isFifo) {
      this.component = component;
      this.isFifo = isFifo;
    }

    public CompletableFuture<? extends Transport> open(BufferSupplier buffers, TransportConsumer consumer) {
      return component.open(buffers, consumer);
    }

    public void close() {
      component.close();
    }

    public int read() throws IOException {
      return component.read();
    }

    public int write(ByteBuffer src) throws IOException {
      return component.write(src);
    }

    public long write(ByteBuffer[] srcs) throws IOException {
      return component.write(srcs);
    }

    public boolean isFifo() {
      return isFifo;
    }

    @Override
    public boolean isOpen() {
      return component.isOpen();
    }

    /* (non-Javadoc)
     * @see org.fixtrading.silverflash.transport.Transport#isReadyToRead()
     */
    @Override
    public boolean isReadyToRead() {
      return component.isReadyToRead();
    }

    /* (non-Javadoc)
     * @see org.fixtrading.silverflash.transport.Transport#isMessageOriented()
     */
    @Override
    public boolean isMessageOriented() {
      return false;
    }

  }


  private FixpSession serverSession;
  private FixpSession clientSession;
  private TestReceiver serverReceiver;
  private final ByteBuffer applicationMessageBuffer;
  private TestReceiver clientReceiver;
  private int outboundKeepaliveInterval = 500;
  private MessageFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
  private SbeMessageHeaderEncoder sbeEncoder = new SbeMessageHeaderEncoder();

  public SessionStepdefs() throws Exception {
    byte[] message = "This an application message".getBytes();
    applicationMessageBuffer =
        ByteBuffer.allocate(128).order(ByteOrder.nativeOrder());
    encodeApplicationMessageWithFrame(applicationMessageBuffer, message);
  }

  @Given("^client and server applications$")
  public void client_and_server_applications() throws Throwable {
    final String USER1_CREDENTIALS = "User1";
    Directory directory = RunAllTests.getDirectory();
    directory.add(USER1_CREDENTIALS);

    final PipeTransport memoryTransport =
        new PipeTransport(RunAllTests.getServerEngine().getIOReactor().getSelector());

    // force explicit sequencing
    Transport serverTransport =
        new TestTransportDecorator(memoryTransport.getServerTransport(), true);
    serverReceiver = new TestReceiver();

    serverSession =
        FixpSession
            .builder()
            .withReactor(RunAllTests.getServerEngine().getReactor())
            .withTransport(serverTransport)
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withMessageConsumer(serverReceiver)
            .withOutboundFlow(FlowType.IDEMPOTENT)
            .withOutboundKeepaliveInterval(outboundKeepaliveInterval).asServer().build();


    Transport clientTransport =
        new TestTransportDecorator(memoryTransport.getClientTransport(), true);
    clientReceiver = new TestReceiver();

    clientSession =
        FixpSession
            .builder()
            .withReactor(RunAllTests.getClientEngine().getReactor())
            .withTransport(clientTransport)
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocateDirect(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withMessageConsumer(clientReceiver)
            .withOutboundFlow(FlowType.IDEMPOTENT).withSessionId(UUID.randomUUID())
            .withClientCredentials(USER1_CREDENTIALS.getBytes())
            .withOutboundKeepaliveInterval(outboundKeepaliveInterval).build();
  }

  @Given("^the client establishes a session with the server$")
  public void the_client_establishes_a_session_with_the_server() throws Throwable {
    serverSession.open();

    SessionReadyFuture future =
        new SessionReadyFuture(clientSession.getSessionId(), RunAllTests.getClientEngine()
            .getReactor());
    clientSession.open();
    future.get(3000, TimeUnit.MILLISECONDS);
  }

  @Given("^the client establishes an idempotent flow to server$")
  public void the_client_establishes_an_idempotent_flow_to_server() throws Throwable {
    Thread.sleep(500L);
    assertEquals(FlowType.IDEMPOTENT, clientSession.getOutboundFlow());
  }

  @When("^the client application sends an application message$")
  public void the_client_application_sends_an_application_message() throws Throwable {
    clientSession.send(applicationMessageBuffer);
  }

  @Then("^the server session accepts message with sequence number (\\d+)$")
  public void the_server_session_accepts_message_with_sequence_number(int arg1) throws Throwable {
    // Allow IO event to fire before testing result
    Thread.sleep(100L);
    assertEquals(arg1, serverSession.getNextSeqNoToReceive() - 1);
  }

  @Then("^presents it to its application$")
  public void presents_it_to_its_application() throws Throwable {
    applicationMessageBuffer.rewind();
    assertEquals(applicationMessageBuffer, serverReceiver.getLastMessage());
  }

  @Then("^the server application has received a total of (\\d+) messages$")
  public void the_server_application_has_received_a_total_of_messages(int arg1) throws Throwable {
    Thread.sleep(500L);
    assertEquals(arg1, serverReceiver.getTotalMessages());
  }

  @Then("^the client application has received a total of (\\d+) messages$")
  public void the_client_application_has_received_a_total_of_messages(int arg1) throws Throwable {
    Thread.sleep(500L);
    assertEquals(arg1, clientReceiver.getTotalMessages());
  }

  @When("^the client application sends an application message with sequence number (\\d+)$")
  public void the_client_application_sends_an_application_message_with_sequence_number(int arg1)
      throws Throwable {
    clientSession.setNextSeqNoToSend(arg1);
    clientSession.send(applicationMessageBuffer);
  }

  @Then("^the client receives a NotApplied message with fromSeqNo (\\d+) and count (\\d+)$")
  public void the_client_receives_a_NotApplied_message_with_fromSeqNo_and_count(int arg1, int arg2)
      throws Throwable {
    Thread.sleep(1000L);
    final ByteBuffer buffer = clientReceiver.getLastMessage();
    if (buffer == null) {
      fail("No response message received");
    }
    final SbeMessageHeaderDecoder messageHeader = new SbeMessageHeaderDecoder();
    messageHeader.wrap(buffer, 0);
    assertEquals("NoApplied was not last message recieved", MessageType.NOT_APPLIED.getCode(),
        messageHeader.getTemplateId());

    final MessageDecoder messageDecoder = new MessageDecoder();
    Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());

    if (optDecoder.isPresent()) {
      final Decoder decoder = optDecoder.get();
      switch (decoder.getMessageType()) {
        case NOT_APPLIED:
          NotAppliedDecoder notApplied = (NotAppliedDecoder) decoder;
          assertEquals(arg1, notApplied.getFromSeqNo());
          assertEquals(arg2, notApplied.getCount());
          break;
        default:
          fail("NoApplied was not last message recieved");
      }
    } else {
      fail("NoApplied was not last message recieved");
    }
  }

  @Then("^the client closes the session with the server$")
  public void the_client_closes_the_session_with_the_server() throws Throwable {
    clientSession.close();
    Thread.sleep(1000L);
    clientSession = null;
    serverSession = null;
  }
  
  private int encodeApplicationMessageWithFrame(ByteBuffer buf, byte[] message) {
    frameEncoder.wrap(buf);
    frameEncoder.encodeFrameHeader();
    sbeEncoder.wrap(buf, frameEncoder.getHeaderLength()).setBlockLength(message.length).setTemplateId(1)
        .setTemplateId(2).getSchemaVersion(0);
    buf.put(message, 0, message.length);
    final int lengthwithHeader = message.length + SbeMessageHeaderDecoder.getLength();
    frameEncoder.setMessageLength(lengthwithHeader);
    frameEncoder.encodeFrameTrailer();
    return lengthwithHeader;
  }
}
