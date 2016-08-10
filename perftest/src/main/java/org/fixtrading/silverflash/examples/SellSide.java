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
import java.net.SocketAddress;
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
import org.fixtrading.silverflash.fixp.FixpSession;
import org.fixtrading.silverflash.fixp.FixpSharedTransportAdaptor;
import org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.NotAppliedDecoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.transport.Dispatcher;
import org.fixtrading.silverflash.transport.IdentifiableTransportConsumer;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.TcpAcceptor;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.fixtrading.silverflash.transport.UdpTransport;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

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

  private class ConsumerSupplier implements Supplier<MessageConsumer<UUID>>,
      Function<UUID, IdentifiableTransportConsumer<UUID>> {

    private List<ServerListener> receivers = new ArrayList<>();

    public IdentifiableTransportConsumer<UUID> apply(UUID sessionId) {
      FixpSession session = createSession(sessionId, get());
      return session.getTransportConsumer();
    }

    private FixpSession createSession(UUID sessionId, MessageConsumer<UUID> consumer) {
      FixpSession session = FixpSession.builder().withReactor(engine.getReactor())
          .withTransport(sharedTransport, true)
          .withBufferSupplier(new SingleBufferSupplier(
              ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())))
          .withMessageConsumer(consumer).withOutboundFlow(FlowType.Idempotent)
          .withMessageFrameEncoder(new MessageLengthFrameEncoder()).withSessionId(sessionId)
          .asServer().build();

      session.open().handle((s, error) -> {
        if (error instanceof Exception) {
          exceptionConsumer.accept((Exception) error);
        }
        return s;
      });
      return session;
    }

    @Override
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

    private final AcceptedEncoder acceptEncoder = new AcceptedEncoder();
    private final NotAppliedDecoder notAppliedDecoder = new NotAppliedDecoder();
    private final ByteBuffer byteBuffer =
        ByteBuffer.allocateDirect(1420).order(ByteOrder.nativeOrder());
    private final MutableDirectBuffer directBuffer = new UnsafeBuffer(byteBuffer);
    private MessageFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
    private final EnterOrderDecoder orderDecoder = new EnterOrderDecoder();
    private long orderId = 0;
    private OrderStruct orderStruct = new OrderStruct();
    private int serverAccepted = 0;
    private int serverDecodeErrors = 0;
    private int serverReceived = 0;
    private int serverUnknown = 0;
    private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();


    public void accept(ByteBuffer inboundBuffer, Session<UUID> session, long seqNo) {

      serverReceived++;

      int offset = inboundBuffer.position();
      immutableBuffer.wrap(inboundBuffer);
      messageHeaderDecoder.wrap(immutableBuffer, offset);
      offset += messageHeaderDecoder.encodedLength();

      final int templateId = messageHeaderDecoder.templateId();
      switch (templateId) {

        case EnterOrderDecoder.TEMPLATE_ID:

          orderDecoder.wrap(immutableBuffer, offset, orderDecoder.sbeBlockLength(),
              orderDecoder.sbeSchemaVersion());

          boolean decoded = decodeOrder(orderDecoder, orderStruct);
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
        case NotAppliedDecoder.TEMPLATE_ID:
          notAppliedDecoder.wrap(immutableBuffer, offset, notAppliedDecoder.sbeBlockLength(),
              notAppliedDecoder.sbeSchemaVersion());

          decodeNotApplied(notAppliedDecoder);
          break;
        default:
          serverUnknown++;
          System.err.format("SellSide Receiver: Unknown template %d%n", templateId);
      }

      if (serverReceived % 10000 == 0) {
        printStats();
      }
    }

    private void decodeNotApplied(NotAppliedDecoder notAppliedDecoder2) {
      long fromSeqNo = notAppliedDecoder.fromSeqNo();
      long count = notAppliedDecoder.count();
      System.err.format("Not Applied from seq no %d count %d%n", fromSeqNo, count);
    }

    private boolean decodeOrder(EnterOrderDecoder orderDecoder, OrderStruct orderStruct) {

      orderDecoder.getClOrdId(orderStruct.clOrdId, 0);
      orderDecoder.side();
      orderDecoder.orderQty();
      orderDecoder.getSymbol(orderStruct.symbol, 0);
      orderDecoder.price();
      orderDecoder.expireTime();
      orderDecoder.getClientID(orderStruct.clientId, 0);
      orderDecoder.display();
      orderDecoder.orderCapacity();
      orderDecoder.intermarketSweepEligibility();
      orderDecoder.minimumQuantity();
      orderDecoder.crossType();
      orderDecoder.customerType();
      orderStruct.transactTime = orderDecoder.transactTime();
      return true;
    }

    private void encodeAccept(OrderStruct orderStruct, MutableDirectBuffer mutableBuffer,
        ByteBuffer sendBuffer) {
      int offset = 0;
      frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
      offset += frameEncoder.getHeaderLength();
      messageHeaderEncoder.wrap(mutableBuffer, offset);
      messageHeaderEncoder.blockLength(acceptEncoder.sbeBlockLength())
          .templateId(acceptEncoder.sbeTemplateId()).schemaId(acceptEncoder.sbeSchemaId())
          .version(acceptEncoder.sbeSchemaVersion());
      offset += messageHeaderEncoder.encodedLength();
      acceptEncoder.wrap(mutableBuffer, offset);
      acceptEncoder.transactTime(0);
      acceptEncoder.putClOrdId(orderStruct.clOrdId, 0);
      acceptEncoder.side(Side.Sell);
      acceptEncoder.orderQty(1L);
      acceptEncoder.putSymbol(orderStruct.symbol, 0);
      acceptEncoder.price().mantissa(10000000);
      acceptEncoder.expireTime(1000L);
      acceptEncoder.putClientID(orderStruct.clientId, 0);
      acceptEncoder.display(Display.AnonymousPrice);
      acceptEncoder.orderId(++orderId);
      acceptEncoder.orderCapacity(OrderCapacity.Agency);
      acceptEncoder.intermarketSweepEligibility(IntermarketSweepEligibility.Eligible);
      acceptEncoder.minimumQuantity(1L);
      acceptEncoder.crossType(CrossType.NoCross);
      acceptEncoder.ordStatus(OrdStatus.New);
      acceptEncoder.bBOWeightIndicator(BBOWeight.Level0);
      acceptEncoder.orderEntryTime(orderStruct.transactTime);
      frameEncoder.setMessageLength(offset + acceptEncoder.encodedLength());
      frameEncoder.encodeFrameTrailer();
    }

    public void printStats() {
      System.out.println("Total requests received:   " + serverReceived);
      System.out.println("Requests unknown template: " + serverUnknown);
      System.out.println("Requests decode errors:    " + serverDecodeErrors);
      System.out.println("Total responses:  " + serverAccepted);
    }

  }

  public static final String CSET_MAX_CORE = "maxcore";
  public static final String CSET_MIN_CORE = "mincore";
  public static final String LOCAL_HOST_KEY = "localhost";
  public static final String LOCAL_PORT_KEY = "localport";
  public static final String MULTIPLEXED_KEY = "multiplexed";
  public static final String NUMBER_OF_CLIENTS_KEY = "clients";
  public static final String PROTOCOL_KEY = "protocol";
  public static final String PROTOCOL_SHARED_MEMORY = "sharedmemory";
  public static final String PROTOCOL_SSL = "ssl";
  public static final String PROTOCOL_TCP = "tcp";
  public static final String PROTOCOL_UDP = "udp";
  public static final String REACTIVE_TRANSPORT_KEY = "reactive";
  public static final String REMOTE_HOST_KEY = "remotehost";
  public static final String REMOTE_PORT_KEY = "remoteport";
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
    defaults.setProperty(CSET_MIN_CORE, "0");
    defaults.setProperty(CSET_MAX_CORE, "7");
    defaults.setProperty(LOCAL_HOST_KEY, LOCAL_HOST_KEY);
    defaults.setProperty(LOCAL_PORT_KEY, "6801");
    defaults.setProperty(MULTIPLEXED_KEY, "false");
    defaults.setProperty(NUMBER_OF_CLIENTS_KEY, "1");
    defaults.setProperty(PROTOCOL_KEY, PROTOCOL_TCP);
    defaults.setProperty(REACTIVE_TRANSPORT_KEY, "true");
    defaults.setProperty(REMOTE_HOST_KEY, "localhost");
    defaults.setProperty(REMOTE_PORT_KEY, "6901");
    defaults.setProperty(SERVER_FLOW_RECOVERABLE_KEY, "true");
    defaults.setProperty(SERVER_FLOW_SEQUENCED_KEY, "true");
    defaults.setProperty(SERVER_KEEPALIVE_INTERVAL_KEY, "1000");
    return defaults;
  }

  private final ConsumerSupplier consumerSupplier = new ConsumerSupplier();
  private Engine engine;
  private ExceptionConsumer exceptionConsumer = ex -> System.err.println(ex);
  private final Properties props;

  private final SessionConfigurationService serverConfig = new SessionConfigurationService() {

    public byte[] getCredentials() {
      return null;
    }

    public int getKeepaliveInterval() {
      return Integer.parseInt(props.getProperty(SERVER_KEEPALIVE_INTERVAL_KEY));
    }

    @Override
    public boolean isOutboundFlowRecoverable() {
      String property = props.getProperty(SERVER_FLOW_RECOVERABLE_KEY);
      return Boolean.parseBoolean(property);
    }

    public boolean isOutboundFlowSequenced() {
      String property = props.getProperty(SERVER_FLOW_SEQUENCED_KEY);
      return Boolean.parseBoolean(property);
    }

    public boolean isTransportMultiplexed() {
      return Boolean.parseBoolean(props.getProperty(MULTIPLEXED_KEY));
    }

  };

  private List<Session<UUID>> serverSessions = new ArrayList<>();
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

  private Transport createMultiplexedTransport() throws Exception {
    if (sharedTransport == null) {
      sharedTransport = FixpSharedTransportAdaptor.builder().withReactor(engine.getReactor())
          .withTransport(createRawTransport(0)).withMessageConsumerSupplier(consumerSupplier)
          .withBufferSupplier(new SingleBufferSupplier(
              ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())))
          .build();

      sharedTransport.openUnderlyingTransport();
    }
    return sharedTransport;
  }

  private TransportConsumer createMultiplexedTransport(Transport rawTransport) throws Exception {
    if (sharedTransport == null) {
      sharedTransport = FixpSharedTransportAdaptor.builder().withReactor(engine.getReactor())
          .withTransport(rawTransport).withMessageConsumerSupplier(consumerSupplier)
          .withBufferSupplier(new SingleBufferSupplier(
              ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())))
          .build();

      sharedTransport.openUnderlyingTransport();
    }
    return sharedTransport;
  }

  private Transport createRawTransport(int sessionIndex) throws Exception {
    String protocol = props.getProperty(PROTOCOL_KEY);
    boolean isReactive = Boolean.getBoolean(props.getProperty(REACTIVE_TRANSPORT_KEY));
    Transport transport;
    switch (protocol) {
      case PROTOCOL_SHARED_MEMORY:
        transport =
            new SharedMemoryTransport(false, true, 1, new Dispatcher(engine.getThreadFactory()));
        break;
      case PROTOCOL_UDP: {
        String remotehost = props.getProperty(REMOTE_HOST_KEY);
        int remoteport = Integer.parseInt(props.getProperty(REMOTE_PORT_KEY)) + sessionIndex;
        SocketAddress remoteAddress = null;
        if (remotehost != null) {
          remoteAddress = new InetSocketAddress(remotehost, remoteport);
        }
        String localhost = props.getProperty(LOCAL_HOST_KEY);
        int localport = Integer.parseInt(props.getProperty(LOCAL_PORT_KEY)) + sessionIndex;
        SocketAddress localAddress = null;
        if (remotehost != null) {
          localAddress = new InetSocketAddress(localhost, localport);
        }
        if (isReactive) {
          transport =
              new UdpTransport(engine.getIOReactor().getSelector(), remoteAddress, localAddress);
        } else {
          transport = new UdpTransport(new Dispatcher(engine.getThreadFactory()), remoteAddress,
              localAddress);
        }
      }
        break;
      default:
        throw new IOException("Unsupported protocol");
    }
    return transport;
  }

  private void createSharedTcpAcceptor()
      throws Exception, InterruptedException, ExecutionException {
    final String localhost = props.getProperty(LOCAL_HOST_KEY);
    int localport = Integer.parseInt(props.getProperty(LOCAL_PORT_KEY));
    final InetSocketAddress serverAddress = new InetSocketAddress(localhost, localport);

    Function<Transport, TransportConsumer> clientAcceptor = serverTransport -> {
      try {
        return createMultiplexedTransport(serverTransport);
      } catch (Exception e) {
        exceptionConsumer.accept(e);
        return null;
      }
    };

    tcpAcceptor =
        new TcpAcceptor(engine.getIOReactor().getSelector(), serverAddress, clientAcceptor);
    tcpAcceptor.open().get();
    System.out.format("Listening for connections on address %s\n", serverAddress);
  }

  private void createTcpAcceptor() throws Exception, InterruptedException, ExecutionException {
    final String localhost = props.getProperty(LOCAL_HOST_KEY);
    int localport = Integer.parseInt(props.getProperty(LOCAL_PORT_KEY));
    final InetSocketAddress serverAddress = new InetSocketAddress(localhost, localport);

    FixpSessionFactory fixpSessionFactory =
        new FixpSessionFactory(engine.getReactor(), serverConfig.getKeepaliveInterval(), false);

    Function<Transport, Session<UUID>> clientAcceptor = serverTransport -> {
      int keepaliveInterval;
      Session<UUID> serverSession = fixpSessionFactory.createServerSession(serverTransport,
          new SingleBufferSupplier(
              ByteBuffer.allocateDirect(16 * 1024).order(ByteOrder.nativeOrder())),
          consumerSupplier.get(), FlowType.Idempotent);

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
    String protocol = props.getProperty(PROTOCOL_KEY);

    if (protocol.equals(PROTOCOL_TCP)) {
      if (isMultiplexed) {
        createSharedTcpAcceptor();
      } else {
        createTcpAcceptor();
      }
    } else {
      FixpSessionFactory fixpSessionFactory = new FixpSessionFactory(engine.getReactor(),
          serverConfig.getKeepaliveInterval(), isMultiplexed);

      int numberOfClients = Integer.parseInt(props.getProperty(NUMBER_OF_CLIENTS_KEY));
      Transport transport;
      for (int i = 0; i < numberOfClients; ++i) {
        if (isMultiplexed) {
          transport = createMultiplexedTransport();
        } else {
          transport = createRawTransport(i);
          Session<UUID> serverSession = fixpSessionFactory.createServerSession(transport,
              new SingleBufferSupplier(
                  ByteBuffer.allocateDirect(16 * 1024).order(ByteOrder.nativeOrder())),
              new ServerListener(), FlowType.Idempotent);
          serverSession.open().get();
          serverSessions.add(serverSession);
        }
      }
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
