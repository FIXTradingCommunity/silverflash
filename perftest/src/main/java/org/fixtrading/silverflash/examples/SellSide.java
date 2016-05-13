/**
 * Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.fixtrading.silverflash.examples;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.examples.messages.AcceptedEncoder;
import org.fixtrading.silverflash.examples.messages.BBOWeight;
import org.fixtrading.silverflash.examples.messages.CrossType;
import org.fixtrading.silverflash.examples.messages.Display;
import org.fixtrading.silverflash.examples.messages.EnterOrderDecoder;
import org.fixtrading.silverflash.examples.messages.IntermarketSweepEligibility;
import org.fixtrading.silverflash.examples.messages.OrdStatus;
import org.fixtrading.silverflash.examples.messages.OrderCapacity;
import org.fixtrading.silverflash.examples.messages.Side;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.FixpSharedTransportAdaptor;
import org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NotAppliedDecoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.transport.Dispatcher;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.TcpAcceptor;
import org.fixtrading.silverflash.transport.Transport;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Test order injector
 * <p>
 * FixpSession layer: FIXP
 * <p>
 * Presentation layer: SBE
 * <p>
 * Command line:
 * {@code java -cp session-perftest-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.fixtrading.silverflash.examples.SellSide 
 * <properties-file> }
 *
 * @author Don Mendelson
 * 
 */
public class SellSide {
  private class ConsumerSupplier implements Supplier<MessageConsumer<UUID>> {

    private List<ServerListener> receivers = new ArrayList<>();

    public MessageConsumer<UUID> get() {
      ServerListener receiver = new ServerListener();
      receivers.add(receiver);
      return receiver;
    }

    public List<ServerListener> getReceivers() {
      return receivers;
    }
  }

  private class ServerListener implements MessageConsumer<UUID> {

    class OrderStruct {
      byte[] clientId = new byte[4];
      byte[] clOrdId = new byte[14];
      byte[] symbol = new byte[8];
      long transactTime;
    }

    private final AcceptedEncoder accept = new AcceptedEncoder();
    private final ByteBuffer byteBuffer =
        ByteBuffer.allocateDirect(1420).order(ByteOrder.nativeOrder());
    private final MutableDirectBuffer directBuffer = new UnsafeBuffer(byteBuffer);
    private final SbeMessageHeaderDecoder messageHeaderIn = new SbeMessageHeaderDecoder();
    private final EnterOrderDecoder order = new EnterOrderDecoder();
    private long orderId = 0;
    private OrderStruct orderStruct = new OrderStruct();
    private int serverAccepted = 0;
    private int serverDecodeErrors = 0;
    private int serverReceived = 0;
    private int serverUnknown = 0;
    private MessageFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
    private SbeMessageHeaderEncoder sbeEncoder = new SbeMessageHeaderEncoder();


    public void accept(ByteBuffer inboundBuffer, Session<UUID> session, long seqNo) {

      serverReceived++;

      messageHeaderIn.wrap(inboundBuffer, inboundBuffer.position());

      final int templateId = messageHeaderIn.getTemplateId();
      switch (templateId) {
        case EnterOrderDecoder.TEMPLATE_ID:

          boolean decoded = decodeOrder(inboundBuffer, orderStruct);
          if (decoded) {

            try {
              byteBuffer.clear();
              encodeAccept(orderStruct, directBuffer, byteBuffer);
              session.send(byteBuffer);
              serverAccepted++;

            } catch (IOException e) {
              System.out.println("Closing session due to IOException");
              try {
                session.close();
              } catch (Exception e1) {
                exceptionConsumer.accept(e1);
              }
            }

          } else {
            serverDecodeErrors++;
          }
          break;
        case 0xfff0:
          decodeNotApplied(inboundBuffer);
          break;
        default:
          serverUnknown++;
          System.err.format("SellSide Receiver: Unknown template %s buffer %s\n",
              messageHeaderIn.toString(), inboundBuffer.toString());
      }

      if (serverReceived % 10000 == 0) {
        printStats();
      }
    }

    public void printStats() {
      System.out.println("Total requests received:   " + serverReceived);
      System.out.println("Requests unknown template: " + serverUnknown);
      System.out.println("Requests decode errors:    " + serverDecodeErrors);
      System.out.println("Total responses:  " + serverAccepted);
    }

    private boolean decodeOrder(ByteBuffer buffer, OrderStruct orderStruct) {
      directBuffer.wrap(buffer);
      order.wrap(directBuffer, buffer.position() + SbeMessageHeaderDecoder.getLength(),
          messageHeaderIn.getBlockLength(), messageHeaderIn.getSchemaVersion());

      order.getClOrdId(orderStruct.clOrdId, 0);
      order.side();
      order.orderQty();
      order.getSymbol(orderStruct.symbol, 0);
      order.price();
      order.expireTime();
      order.getClientID(orderStruct.clientId, 0);
      order.display();
      order.orderCapacity();
      order.intermarketSweepEligibility();
      order.minimumQuantity();
      order.crossType();
      order.customerType();
      orderStruct.transactTime = order.transactTime();
      return true;
    }

    private void decodeNotApplied(ByteBuffer buffer) {
      Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());
      final Decoder decoder = optDecoder.get();
      switch (decoder.getMessageType()) {
        case NOT_APPLIED:
          NotAppliedDecoder notAppliedDecoder = (NotAppliedDecoder) decoder;
          long fromSeqNo = notAppliedDecoder.getFromSeqNo();
          int count = notAppliedDecoder.getCount();
          System.err.format("Not Applied from seq no %d count %d%n", fromSeqNo, count);
          break;
        default:
          System.err.println("Unexpected application message");
      }
    }

    private void encodeAccept(OrderStruct orderStruct, MutableDirectBuffer directBuffer,
        ByteBuffer byteBuffer) {
      int bufferOffset = byteBuffer.position();

      frameEncoder.wrap(byteBuffer);
      frameEncoder.encodeFrameHeader();

      bufferOffset += frameEncoder.getHeaderLength();

      sbeEncoder.wrap(byteBuffer, bufferOffset).setBlockLength(accept.sbeBlockLength())
          .setTemplateId(accept.sbeTemplateId()).setSchemaId(accept.sbeSchemaId())
          .getSchemaVersion(accept.sbeSchemaVersion());

      bufferOffset += SbeMessageHeaderDecoder.getLength();

      directBuffer.wrap(byteBuffer);
      accept.wrap(directBuffer, bufferOffset);

      accept.transactTime(0);
      accept.putClOrdId(orderStruct.clOrdId, 0);
      accept.side(Side.Sell);
      accept.orderQty(1L);
      accept.putSymbol(orderStruct.symbol, 0);
      accept.price().mantissa(10000000);
      accept.expireTime(1000L);
      accept.putClientID(orderStruct.clientId, 0);
      accept.display(Display.AnonymousPrice);
      accept.orderId(++orderId);
      accept.orderCapacity(OrderCapacity.Agency);
      accept.intermarketSweepEligibility(IntermarketSweepEligibility.Eligible);
      accept.minimumQuantity(1L);
      accept.crossType(CrossType.NoCross);
      accept.ordStatus(OrdStatus.New);
      accept.bBOWeightIndicator(BBOWeight.Level0);
      accept.orderEntryTime(orderStruct.transactTime);

      final int lengthwithHeader = SbeMessageHeaderDecoder.getLength() + accept.encodedLength();
      frameEncoder.setMessageLength(lengthwithHeader);
      frameEncoder.encodeFrameTrailer();
    }

  }

  public static final String CSET_MAX_CORE = "maxcore";
  public static final String CSET_MIN_CORE = "mincore";
  public static final String LOCAL_HOST_KEY = "localhost";
  public static final String LOCAL_PORT_KEY = "localport";
  public static final String MULTIPLEXED_KEY = "multiplexed";
  public static final String NUMBER_OF_CLIENTS = "clients";
  public static final String PROTOCOL_KEY = "protocol";
  public static final String PROTOCOL_SHARED_MEMORY = "sharedmemory";
  public static final String PROTOCOL_SSL = "ssl";
  public static final String PROTOCOL_TCP = "tcp";
  public static final String PROTOCOL_UDP = "udp";
  public static final String SERVER_FLOW_RECOVERABLE_KEY = "recoverable";

  public static final String SERVER_FLOW_SEQUENCED_KEY = "sequenced";

  public static final String SERVER_KEEPALIVE_INTERVAL_KEY = "heartbeatInterval";

  private static Properties loadProperties(String fileName)
      throws IOException, FileNotFoundException {
    Properties defaults = setDefaultProperties();
    Properties props = new Properties(defaults);

    try (final FileReader reader = new FileReader(fileName)) {
      props.load(reader);
    } catch (IOException e) {
      System.err.format("Failed to read properties from file %s\n", fileName);
      throw e;
    }
    return props;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: java org.fixtrading.silverflash.examples.SellSide <conf-filename>");
      System.exit(1);
    }
    Properties props = loadProperties(args[0]);
    final SellSide sellSide = new SellSide(props);
    try {
      sellSide.init();

      try {
        Thread.sleep(1000 * 1000L);
      } catch (InterruptedException e) {
      }
    } catch (IOException e) {
      System.err.format("Failed to initialize SellSide; %s\n", e);
    }

    sellSide.shutdown();
  }

  private static Properties setDefaultProperties() {
    Properties defaults = new Properties();
    defaults.setProperty(SERVER_FLOW_RECOVERABLE_KEY, "true");
    defaults.setProperty(SERVER_FLOW_SEQUENCED_KEY, "true");
    defaults.setProperty(PROTOCOL_KEY, PROTOCOL_TCP);
    defaults.setProperty(MULTIPLEXED_KEY, "false");
    defaults.setProperty(LOCAL_HOST_KEY, LOCAL_HOST_KEY);
    defaults.setProperty(LOCAL_PORT_KEY, "6869");
    defaults.setProperty(SERVER_KEEPALIVE_INTERVAL_KEY, "1000");
    defaults.setProperty(NUMBER_OF_CLIENTS, "1");
    defaults.setProperty(CSET_MIN_CORE, "0");
    defaults.setProperty(CSET_MAX_CORE, "7");
    return defaults;
  }

  private final ConsumerSupplier consumerSupplier = new ConsumerSupplier();

  private Engine engine;
  private ExceptionConsumer exceptionConsumer = ex -> System.err.println(ex);

  private final MessageDecoder messageDecoder = new MessageDecoder();

  private final Properties props;

  private final SessionConfigurationService serverConfig = new SessionConfigurationService() {

    public byte[]getCredentials(){return null;}

  public int getKeepaliveInterval(){return Integer.parseInt(props.getProperty(SERVER_KEEPALIVE_INTERVAL_KEY));}

  @Override public boolean isOutboundFlowRecoverable(){String property=props.getProperty(SERVER_FLOW_RECOVERABLE_KEY);return Boolean.parseBoolean(property);}

  public boolean isOutboundFlowSequenced(){String property=props.getProperty(SERVER_FLOW_SEQUENCED_KEY);return Boolean.parseBoolean(property);}

  public boolean isTransportMultiplexed(){return Boolean.parseBoolean(props.getProperty(MULTIPLEXED_KEY));}

  };
  private FixpSharedTransportAdaptor sharedTransport = null;
  private TcpAcceptor tcpAcceptor = null;

  /**
   * Create a reflector with default properties
   */
  public SellSide() {
    Properties defaults = setDefaultProperties();
    this.props = new Properties(defaults);
  }

  /**
   * Create an reflector
   * 
   * @param props configuration
   */
  public SellSide(Properties props) {
    Properties defaults = setDefaultProperties();
    this.props = new Properties(defaults);
    this.props.putAll(props);
  }

  private Transport createRawTransport(String protocol) throws IOException {
    Transport transport;
    switch (protocol) {
      case PROTOCOL_SHARED_MEMORY:
        transport =
            new SharedMemoryTransport(false, true, 1, new Dispatcher(engine.getThreadFactory()));
        break;
      default:
        throw new IOException("Unsupported protocol");
    }
    return transport;
  }

  public void init() throws Exception {
    int minCore = Integer.parseInt(props.getProperty(CSET_MIN_CORE));
    int maxCore = Integer.parseInt(props.getProperty(CSET_MAX_CORE));

    SimpleDirectory directory = new SimpleDirectory();
    engine = Engine.builder().withCoreRange(minCore, maxCore)
        .withAuthenticator(new SimpleAuthenticator().withDirectory(directory)).build();
    // engine.getReactor().setTrace(true, "server");
    engine.open();

    for (int i = 0; i < 100; i++) {
      String user = "client" + i;
      directory.add(user);
    }

    boolean isMultiplexed = serverConfig.isTransportMultiplexed();

    FixpSessionFactory fixpSessionFactory = new FixpSessionFactory(engine.getReactor(),
        serverConfig.getKeepaliveInterval(), isMultiplexed);

    String protocol = props.getProperty(PROTOCOL_KEY);

    if (isMultiplexed) {
      if (sharedTransport == null) {
        sharedTransport = FixpSharedTransportAdaptor.builder().withReactor(engine.getReactor())
            .withTransport(createRawTransport(protocol))
            .withMessageConsumerSupplier(consumerSupplier)
            .withBufferSupplier(new SingleBufferSupplier(
                ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())))
            .withFlowType(FlowType.IDEMPOTENT).build();

        sharedTransport.openUnderlyingTransport();
      }
    } else if (protocol.equals(PROTOCOL_TCP)) {
      final String localhost = props.getProperty(LOCAL_HOST_KEY);
      int localport = Integer.parseInt(props.getProperty(LOCAL_PORT_KEY));
      final InetSocketAddress serverAddress = new InetSocketAddress(localhost, localport);

      Function<Transport, Session<UUID>> clientAcceptor = serverTransport -> {
        int keepaliveInterval;
        Session<UUID> serverSession = fixpSessionFactory.createServerSession(serverTransport,
            new SingleBufferSupplier(
                ByteBuffer.allocateDirect(16 * 1024).order(ByteOrder.nativeOrder())),
            consumerSupplier.get(), FlowType.IDEMPOTENT);

        try {
          serverSession.open().get(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          exceptionConsumer.accept(e);
        }

        return serverSession;
      };

      tcpAcceptor =
          new TcpAcceptor(engine.getIOReactor().getSelector(), serverAddress, clientAcceptor);
      tcpAcceptor.open().get();
      System.out.format("Listening for connections on address %s\n", serverAddress);
    }

  }

  public void shutdown() {
    System.out.println("Tearing down sessions");
    try {
      if (tcpAcceptor != null) {
        tcpAcceptor.close();
      }
      if (sharedTransport != null) {
        sharedTransport.close();
      }
    } catch (IOException e) {
      exceptionConsumer.accept(e);
    }
    engine.close();
    System.exit(0);
  }

}
