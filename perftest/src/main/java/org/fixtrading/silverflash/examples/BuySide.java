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

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.examples.messages.AcceptedDecoder;
import org.fixtrading.silverflash.examples.messages.CrossType;
import org.fixtrading.silverflash.examples.messages.CustomerType;
import org.fixtrading.silverflash.examples.messages.Display;
import org.fixtrading.silverflash.examples.messages.EnterOrderEncoder;
import org.fixtrading.silverflash.examples.messages.IntermarketSweepEligibility;
import org.fixtrading.silverflash.examples.messages.OrderCapacity;
import org.fixtrading.silverflash.examples.messages.Side;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.FixpSharedTransportAdaptor;
import org.fixtrading.silverflash.fixp.SessionReadyFuture;
import org.fixtrading.silverflash.fixp.SessionTerminatedFuture;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.NotAppliedDecoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameSpliterator;
import org.fixtrading.silverflash.transport.Dispatcher;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.TcpConnectorTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.UdpTransport;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Test order server
 * <p>
 * FixpSession layer: FIXP
 * <p>
 * Presentation layer: SBE
 * <p>
 * Command line:
 * {@code java -cp session-perftest-0.0.1-SNAPSHOT-jar-with-dependencies.jar org.fixtrading.silverflash.examples.BuySide 
 * <properties-file> }
 *
 * @author Don Mendelson
 * 
 */
public class BuySide implements Runnable {

  class ClientListener implements MessageConsumer<UUID> {

    class AcceptStruct {
      byte[] clientId = new byte[4];
      byte[] clOrdId = new byte[14];
      long orderId;
      byte[] symbol = new byte[8];
    }

    private final AcceptedDecoder acceptDecoder = new AcceptedDecoder();
    private final NotAppliedDecoder notAppliedDecoder = new NotAppliedDecoder();
    private final AcceptStruct acceptStruct = new AcceptStruct();
    private final Histogram rttHistogram;
    private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();


    public ClientListener(Histogram rttHistogram) {
      this.rttHistogram = rttHistogram;
    }

    public void accept(ByteBuffer buffer, Session<UUID> session, long seqNo) {
      int offset = buffer.position();
      immutableBuffer.wrap(buffer);
      messageHeaderDecoder.wrap(immutableBuffer, offset);
      offset += messageHeaderDecoder.encodedLength();
      
      final int templateId = messageHeaderDecoder.templateId();
      switch (templateId) {
        case AcceptedDecoder.TEMPLATE_ID:
          acceptDecoder.wrap(immutableBuffer, offset, acceptDecoder.sbeBlockLength(),
              acceptDecoder.sbeSchemaVersion());
          decodeAccepted(acceptDecoder, acceptStruct);
          break;
        case NotAppliedDecoder.TEMPLATE_ID:
          notAppliedDecoder.wrap(immutableBuffer, offset, notAppliedDecoder.sbeBlockLength(),
              notAppliedDecoder.sbeSchemaVersion());
          decodeNotApplied(notAppliedDecoder);
          break;
        default:
          System.err.format("BuySide Receiver: Unknown template %d\n", templateId);
      }
    }

    private void decodeAccepted(AcceptedDecoder acceptDecoder, AcceptStruct acceptStruct) {
      try {

        // long transactTime = accept.transactTime();
        // accept.getClOrdId(acceptStruct.clOrdId, 0);
        // accept.side();
        // accept.orderQty();
        // accept.getSymbol(acceptStruct.symbol, 0);
        // accept.price();
        // accept.expireTime();
        // accept.getClientID(acceptStruct.clientId, 0);
        // accept.display();
        // acceptStruct.orderId = accept.orderId();
        // accept.orderCapacity();
        // accept.intermarketSweepEligibility();
        // accept.minimumQuantity();
        // accept.crossType();
        // accept.ordStatus();
        // accept.bBOWeightIndicator();
        long orderEntryTime = acceptDecoder.orderEntryTime();
        if (orderEntryTime != 0) {
          long now = System.nanoTime();
          rttHistogram.recordValue(now - orderEntryTime);
        }
      } catch (IllegalArgumentException e) {
        System.err.format("Decode error; %s buffer %s\n", e.getMessage(), acceptDecoder.toString());
      }
    }

    private void decodeNotApplied(NotAppliedDecoder notAppliedDecoder2) {
      long fromSeqNo = notAppliedDecoder.fromSeqNo();
      long count = notAppliedDecoder.count();
      System.err.format("Not Applied from seq no %d count %d%n", fromSeqNo, count);
    }
  }

  private class ClientRunner implements Runnable {

    private final long batchPauseMillis;
    private final int batchSize;
    private final Histogram burstHistogram;
    private final Session<UUID> client;
    private final byte[] clOrdId = new byte[14];
    private final ByteBuffer clOrdIdBuffer;
    private MessageFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
    private final int orders;
    private final int ordersPerPacket;
    private final Histogram rttHistogram;
    private SessionTerminatedFuture terminatedFuture;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    public ClientRunner(Session<UUID> client, Histogram rttHistogram, int orders, int batchSize,
        int ordersPerPacket, long batchPauseMillis) {
      this.client = client;
      this.rttHistogram = rttHistogram;

      final long highestTrackableValue = TimeUnit.MINUTES.toNanos(1);
      final int numberOfSignificantValueDigits = 3;
      burstHistogram = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);

      this.orders = orders;
      this.batchSize = batchSize;
      this.ordersPerPacket = ordersPerPacket;
      this.batchPauseMillis = batchPauseMillis;
      System.arraycopy("client00000000".getBytes(), 0, clOrdId, 0, 14);
      clOrdIdBuffer = ByteBuffer.wrap(clOrdId);
    }

    /**
     * Attempts to establish a session, and when done successfully, performs injection on its own
     * thread.
     * 
     * @return a future to handle session termination asynchronously
     * @throws Exception
     */
    public SessionTerminatedFuture connect() {
      terminatedFuture = new SessionTerminatedFuture(client.getSessionId(), engine.getReactor());

      try {
        CompletableFuture<ByteBuffer> readyFuture =
            new SessionReadyFuture(client.getSessionId(), engine.getReactor());
        client.open();
        readyFuture.get(5000, TimeUnit.MILLISECONDS);
        System.out.println("Connected; session ID=" + client.getSessionId());
        executor.execute(this);
      } catch (Exception e) {
        System.err.println("Failed to connect; session ID=" + client.getSessionId() + "; " + e);
        terminatedFuture.completeExceptionally(e);
      }
      return terminatedFuture;
    }

    private void encodeOrder(MutableDirectBuffer mutableBuffer, ByteBuffer sendBuffer,
        boolean shouldTimestamp, int orderNumber) {
      int offset = 0;
      frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
      offset += frameEncoder.getHeaderLength();
      messageHeaderEncoder.wrap(mutableBuffer, offset);
      messageHeaderEncoder.blockLength(orderEncoder.sbeBlockLength())
          .templateId(orderEncoder.sbeTemplateId()).schemaId(orderEncoder.sbeSchemaId())
          .version(orderEncoder.sbeSchemaVersion());
      offset += messageHeaderEncoder.encodedLength();
      orderEncoder.wrap(mutableBuffer, offset);
      clOrdIdBuffer.putInt(6, orderNumber);
      orderEncoder.putClOrdId(clOrdId, 0);
      orderEncoder.side(Side.Sell);
      orderEncoder.orderQty(1L);
      orderEncoder.putSymbol(symbol, 0);
      orderEncoder.price().mantissa(10000000);
      orderEncoder.expireTime(1000L);
      orderEncoder.putClientID(clientId, 0);
      orderEncoder.display(Display.AnonymousPrice);
      orderEncoder.orderCapacity(OrderCapacity.Agency);
      orderEncoder.intermarketSweepEligibility(IntermarketSweepEligibility.Eligible);
      orderEncoder.minimumQuantity(1L);
      orderEncoder.crossType(CrossType.NoCross);
      orderEncoder.customerType(CustomerType.Retail);
      if (shouldTimestamp) {
        orderEncoder.transactTime(System.nanoTime());
      } else {
        orderEncoder.transactTime(0);
      }

      frameEncoder.setMessageLength(offset + orderEncoder.encodedLength());
      frameEncoder.encodeFrameTrailer();
    }

    /**
     * @return the burstHistogram
     */
    public Histogram getBurstHistogram() {
      return burstHistogram;
    }

    public void inject(Session<UUID> client, Histogram rttHistogram, int orders, int batchSize,
        int ordersPerPacket, long batchPauseMillis) throws IOException {
      int iterations = orders / batchSize;
      int iterationsToIgnore = (int) Math.ceil(10000.0 / batchSize);
      int packetsPerBatch = (int) Math.ceil((double) batchSize / ordersPerPacket);

      System.out.format(
          "Batch size=%d orders per packet=%d iterations=%d iterations to ignore=%d\n", batchSize,
          ordersPerPacket, iterations, iterationsToIgnore);

      rttHistogram.reset();
      burstHistogram.reset();
      int orderNumber = 0;

      final ByteBuffer[] toServerByteBuffer = new ByteBuffer[packetsPerBatch];
      for (int i = 0; i < packetsPerBatch; i++) {
        toServerByteBuffer[i] = ByteBuffer.allocateDirect(1420).order(ByteOrder.nativeOrder());
      }
      final MutableDirectBuffer[] toServerBuffer = new MutableDirectBuffer[packetsPerBatch];
      for (int i = 0; i < packetsPerBatch; i++) {
        toServerBuffer[i] = new UnsafeBuffer(toServerByteBuffer[i]);
      }

      for (int j = 0; j < iterations; j++) {

        for (int i = 0; i < packetsPerBatch; i++) {
          if (ordersPerPacket == 1) {
            toServerByteBuffer[i].clear();
            encodeOrder(toServerBuffer[i], toServerByteBuffer[i], (j > iterationsToIgnore),
                ++orderNumber);
          } else {
            for (int h = 0; h < ordersPerPacket; h++) {
              toServerByteBuffer[i].clear();
              encodeOrder(toServerBuffer[i], toServerByteBuffer[i], (j > iterationsToIgnore),
                  ++orderNumber);
            }
          }
        }

        long start = System.nanoTime();

        if (ordersPerPacket == 1) {
          for (int i = 0; i < packetsPerBatch; i++) {
            long seqNo = client.send(toServerByteBuffer[i]);
          }
        } else {
          client.send(toServerByteBuffer);
        }

        long end = System.nanoTime();
        if (j > iterationsToIgnore) {
          burstHistogram.recordValue(end - start);
        }

        try {

          Thread.sleep(batchPauseMillis);
        } catch (InterruptedException e) {
        }

      }

      try {

        Thread.sleep(100 * batchPauseMillis);
      } catch (InterruptedException e) {
      }

    }

    /**
    	 * 
    	 */
    public void report() {
      System.out.format("Client session ID %s\nRTT microseconds\n", client.getSessionId());
      printStats(rttHistogram);
      System.out.format("Burst injection microseconds - burst size %d\n", batchSize);
      printStats(getBurstHistogram());
    }

    @Override
    public void run() {
      try {
        inject(client, rttHistogram, orders, batchSize, ordersPerPacket, batchPauseMillis);
      } catch (Exception e) {
        terminatedFuture.completeExceptionally(e);
      } finally {
        try {
          client.close();
        } catch (Exception e) {
          terminatedFuture.completeExceptionally(e);
        }
        System.out.println("Buy side session closed");
      }
    }
  }

  /*
   * Configuration keys
   */
  public static final String CLIENT_FLOW_RECOVERABLE_KEY = "recoverable";
  public static final String CLIENT_FLOW_SEQUENCED_KEY = "sequenced";
  public static final String CLIENT_KEEPALIVE_INTERVAL_KEY = "heartbeatInterval";
  public static final String CSET_MAX_CORE = "maxCore";
  public static final String CSET_MIN_CORE = "minCore";
  public static final String INJECT_BATCH_PAUSE_MILLIS = "batchPause";
  public static final String INJECT_BATCH_SIZE = "batchSize";
  public static final String INJECT_ORDERS_PER_PACKET = "ordersPerPacket";
  public static final String INJECT_ORDERS_TO_SEND = "orders";
  public static final String LOCAL_HOST_KEY = "localhost";
  public static final String LOCAL_PORT_KEY = "localport";
  public static final String MULTIPLEXED_TRANSPORT_KEY = "multiplexed";
  public static final String NUMBER_OF_CLIENTS_KEY = "clients";
  public static final String PROTOCOL_KEY = "protocol";
  public static final String PROTOCOL_SHARED_MEMORY = "sharedmemory";
  public static final String PROTOCOL_SSL = "ssl";
  public static final String PROTOCOL_TCP = "tcp";
  public static final String PROTOCOL_UDP = "udp";
  public static final String REACTIVE_TRANSPORT_KEY = "reactive";
  public static final String REMOTE_HOST_KEY = "remotehost";
  public static final String REMOTE_PORT_KEY = "remoteport";
  public static final String REMOTE_RECOVERY_HOST_KEY = "remoteRecoveryHost";
  public static final String REMOTE_RECOVERY_PORT_KEY = "remoteRecoveryPort";
  public static final String SERVER_RECOVERY_INBAND = "inBand";
  public static final String SERVER_RECOVERY_KEY = "serverRecovery";
  public static final String SERVER_RECOVERY_OUTOFBAND = "outOfBand";

  private static Properties getDefaultProperties() {
    Properties defaults = new Properties();
    defaults.setProperty(CLIENT_FLOW_SEQUENCED_KEY, "true");
    defaults.setProperty(CLIENT_FLOW_RECOVERABLE_KEY, "false");
    defaults.setProperty(CLIENT_KEEPALIVE_INTERVAL_KEY, "1000");
    defaults.setProperty(CSET_MIN_CORE, "0");
    defaults.setProperty(CSET_MAX_CORE, "7");
    defaults.setProperty(INJECT_BATCH_SIZE, "100");
    defaults.setProperty(INJECT_BATCH_PAUSE_MILLIS, "200");
    defaults.setProperty(INJECT_ORDERS_TO_SEND, Integer.toString(50000));
    defaults.setProperty(INJECT_ORDERS_PER_PACKET, Integer.toString(10));
    defaults.setProperty(LOCAL_HOST_KEY, "localhost");
    defaults.setProperty(LOCAL_PORT_KEY, "6901");
    defaults.setProperty(MULTIPLEXED_TRANSPORT_KEY, "false");
    defaults.setProperty(NUMBER_OF_CLIENTS_KEY, "1");
    defaults.setProperty(PROTOCOL_KEY, PROTOCOL_TCP);
    defaults.setProperty(REACTIVE_TRANSPORT_KEY, "true");
    defaults.setProperty(REMOTE_HOST_KEY, "localhost");
    defaults.setProperty(REMOTE_PORT_KEY, "6801");
    defaults.setProperty(REMOTE_RECOVERY_HOST_KEY, "localhost");
    defaults.setProperty(REMOTE_RECOVERY_PORT_KEY, "6701");
    defaults.setProperty(SERVER_RECOVERY_KEY, SERVER_RECOVERY_INBAND);
    return defaults;
  }

  private static Properties loadProperties(String fileName) throws IOException {
    Properties defaults = getDefaultProperties();
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
      System.err.println("Usage: java org.fixtrading.silverflash.examples.BuySide <conf-filename>");
      System.exit(1);
    }

    Properties props = loadProperties(args[0]);
    final BuySide buySide = new BuySide(props);
    buySide.init();
    buySide.run();
  }

  private static void printStats(Histogram data) {
    long totalCount = data.getTotalCount();
    System.out.println("Total count: " + totalCount);
    if (totalCount > 0) {
      System.out.println("MIN\tMAX\tMEAN\t30%\t50%\t90%\t95%\t99%\t99.99%\tSTDDEV");
      System.out.format("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
          TimeUnit.NANOSECONDS.toMicros(data.getMinValue()),
          TimeUnit.NANOSECONDS.toMicros(data.getMaxValue()),
          TimeUnit.NANOSECONDS.toMicros((long) data.getMean()),
          TimeUnit.NANOSECONDS.toMicros(data.getValueAtPercentile(30.0)),
          TimeUnit.NANOSECONDS.toMicros(data.getValueAtPercentile(50.0)),
          TimeUnit.NANOSECONDS.toMicros(data.getValueAtPercentile(90.0)),
          TimeUnit.NANOSECONDS.toMicros(data.getValueAtPercentile(95.0)),
          TimeUnit.NANOSECONDS.toMicros(data.getValueAtPercentile(99.0)),
          TimeUnit.NANOSECONDS.toMicros(data.getValueAtPercentile(99.99)),
          TimeUnit.NANOSECONDS.toMicros((long) data.getStdDeviation()));
    }
  }

  private final SessionConfigurationService clientConfig = new SessionConfigurationService() {

    public byte[] getCredentials() {
      return "TestUser".getBytes();
    }

    // Hearbeat interval in millis
    public int getKeepaliveInterval() {
      return Integer.parseInt(props.getProperty(CLIENT_KEEPALIVE_INTERVAL_KEY));
    }

    @Override
    public boolean isOutboundFlowRecoverable() {
      return Boolean.parseBoolean(props.getProperty(CLIENT_FLOW_RECOVERABLE_KEY));
    }

    public boolean isOutboundFlowSequenced() {
      return Boolean.parseBoolean(props.getProperty(CLIENT_FLOW_SEQUENCED_KEY));
    }


    public boolean isTransportMultiplexed() {
      return Boolean.parseBoolean(props.getProperty(MULTIPLEXED_TRANSPORT_KEY));
    }

  };

  private final byte[] clientId = "0999".getBytes();
  private Engine engine;
  private ExceptionConsumer exceptionConsumer = ex -> System.err.println(ex);

  private ExecutorService executor;

  private int numberOfClients;
  private final EnterOrderEncoder orderEncoder = new EnterOrderEncoder();
  private int ordersToSend;
  private final Properties props;
  private ClientRunner[] runners;
  private Transport sharedTransport = null;
  private final byte[] symbol = "ESH8    ".getBytes();

  /**
   * Create an injector with default properties
   */
  public BuySide() {
    this.props = getDefaultProperties();
  }

  /**
   * Create an injector
   * 
   * @param props configuration
   */
  public BuySide(Properties props) {
    this.props = getConfigurationWithDefaults(props);
    ordersToSend = Integer.parseInt(this.props.getProperty(INJECT_ORDERS_TO_SEND));
  }

  private Transport createMultiplexedTransport() throws Exception {
    if (sharedTransport == null) {
      sharedTransport = FixpSharedTransportAdaptor.builder().withReactor(engine.getReactor())
          .withTransport(createRawTransport(0))
          .withMessageFramer(new MessageLengthFrameSpliterator())
          .withBufferSupplier(new SingleBufferSupplier(
              ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder()))).build();
    }

    return sharedTransport;
  }

  private Transport createRawTransport(int sessionIndex) throws Exception {
    String protocol = props.getProperty(PROTOCOL_KEY);
    boolean isReactive = Boolean.getBoolean(props.getProperty(REACTIVE_TRANSPORT_KEY));
    Transport transport;
    switch (protocol) {
      case PROTOCOL_TCP: {
        String remotehost = props.getProperty(REMOTE_HOST_KEY);
        int remoteport = Integer.parseInt(props.getProperty(REMOTE_PORT_KEY));
        SocketAddress remoteAddress = null;
        if (remotehost != null) {
          remoteAddress = new InetSocketAddress(remotehost, remoteport);
        }
        if (isReactive) {
          transport = new TcpConnectorTransport(engine.getIOReactor().getSelector(), remoteAddress);
        } else {
          transport =
              new TcpConnectorTransport(new Dispatcher(engine.getThreadFactory()), remoteAddress);
        }
      }
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
      case PROTOCOL_SHARED_MEMORY:
        transport =
            new SharedMemoryTransport(true, true, 1, new Dispatcher(engine.getThreadFactory()));
        break;
      default:
        throw new IOException("Unsupported protocol");
    }
    return transport;
  }

  public Session<UUID> createSession(int sessionIndex, FixpSessionFactory fixpSessionFactory,
      Histogram rttHistogram) throws Exception {

    String serverRecovery = props.getProperty(SERVER_RECOVERY_KEY);

    boolean isSequenced = clientConfig.isOutboundFlowSequenced();
    boolean isRecoverable = clientConfig.isOutboundFlowRecoverable();
    FlowType outboundFlow = isSequenced
        ? (isRecoverable ? FlowType.Recoverable : FlowType.Idempotent) : FlowType.Unsequenced;
    boolean isMultiplexed = clientConfig.isTransportMultiplexed();

    ClientListener clientListener = new ClientListener(rttHistogram);

    Transport transport;
    if (isMultiplexed) {
      transport = createMultiplexedTransport();
    } else {
      transport = createRawTransport(sessionIndex);
    }

    String user = "client" + sessionIndex;
    byte[] credentials = user.getBytes();
    return fixpSessionFactory.createClientSession(credentials, transport,
        new SingleBufferSupplier(
            ByteBuffer.allocateDirect(16 * 1024).order(ByteOrder.nativeOrder())),
        clientListener, outboundFlow);
  }

  public Properties getConfigurationWithDefaults(Properties props) {
    Properties defaults = getDefaultProperties();
    Properties props2 = new Properties(defaults);
    props2.putAll(props);
    return props2;
  }

  /**
   * @return the ordersToSend
   */
  public int getOrdersToSend() {
    return ordersToSend;
  }

  public void init() throws Exception {

    final Object batchSizeString = props.getProperty(INJECT_BATCH_SIZE);
    final int batchSize = Integer.parseInt((String) batchSizeString);
    final int ordersPerPacket = Integer.parseInt(props.getProperty(INJECT_ORDERS_PER_PACKET));
    final long batchPauseMillis = Long.parseLong(props.getProperty(INJECT_BATCH_PAUSE_MILLIS));
    numberOfClients = Integer.parseInt(props.getProperty(NUMBER_OF_CLIENTS_KEY));
    final long highestTrackableValue = TimeUnit.MINUTES.toNanos(1);
    final int numberOfSignificantValueDigits = 3;

    int minCore = Integer.parseInt(props.getProperty(CSET_MIN_CORE));
    int maxCore = Integer.parseInt(props.getProperty(CSET_MAX_CORE));

    engine = Engine.builder().withCoreRange(minCore, maxCore).build();
    // engine.getReactor().setTrace(true, "client");
    engine.open();
    executor = engine.newNonAffinityThreadPool(numberOfClients);

    FixpSessionFactory fixpSessionFactory = new FixpSessionFactory(engine.getReactor(),
        clientConfig.getKeepaliveInterval(), clientConfig.isTransportMultiplexed());

    runners = new ClientRunner[numberOfClients];
    for (int i = 0; i < numberOfClients; ++i) {

      final Histogram rttHistogram =
          new Histogram(highestTrackableValue, numberOfSignificantValueDigits);

      final Session<UUID> client = createSession(i, fixpSessionFactory, rttHistogram);

      runners[i] = new ClientRunner(client, rttHistogram, ordersToSend, batchSize, ordersPerPacket,
          batchPauseMillis);
    }
  }


  public void run() {
    SessionTerminatedFuture[] terminatedFutures = new SessionTerminatedFuture[numberOfClients];
    for (int i = 0; i < numberOfClients; ++i) {
      terminatedFutures[i] = runners[i].connect();
    }

    CompletableFuture<Void> allDone = CompletableFuture.allOf(terminatedFutures);
    try {
      allDone.get();
      for (int i = 0; i < numberOfClients; ++i) {
        runners[i].report();
      }
    } catch (InterruptedException | ExecutionException e) {
      exceptionConsumer.accept(e);
    }

    shutdown();
  }

  /**
   * @param ordersToSend the ordersToSend to set
   */
  public void setOrdersToSend(int ordersToSend) {
    this.ordersToSend = ordersToSend;
  }

  public void shutdown() {
    System.out.println("Shutting down");
    engine.close();
    executor.shutdown();
    System.exit(0);
  }

}
