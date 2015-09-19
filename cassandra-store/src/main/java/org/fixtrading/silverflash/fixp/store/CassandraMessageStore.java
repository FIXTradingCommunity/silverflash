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

package org.fixtrading.silverflash.fixp.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Cassandra message store uses Datastax driver
 * <p>
 * Schema:
 * 
 * <pre>
 * CREATE TABLE messages (
 *   sessionId uuid,
 *   seqNo bigint,
 *   message blob,
 *   PRIMARY KEY  (sessionId, seqNo ) );
 * </pre>
 * 
 * @author Don Mendelson
 *
 */
public class CassandraMessageStore implements MessageStore {

  private static class RowConsumer implements Consumer<Row> {
    private final ArrayList<ByteBuffer> arrayList;
    private int index = 0;
    private final int initialSize;

    /**
     * @param result result set holder
     */
    public RowConsumer(MessageStoreResult result) {
      this.arrayList = result.getMessageList();
      this.initialSize = arrayList.size();
    }

    public void accept(Row row) {
      // Make a copy from result set row because lifetime of this object may outlast result set;
      // it may be overwritten.
      final ByteBuffer src = row.getBytes(MESSAGE_COLNAME);

      if (index >= initialSize) {
        arrayList.add(ByteBuffer.allocate(BUFFER_CAPACITY));
      }

      final ByteBuffer target = arrayList.get(index);
      target.clear();
      target.put(src);
      target.flip();
      index++;
    }
  }

  private static final int BUFFER_CAPACITY = 2048;
  private static final String KEYSPACE_NAME = "fixp";
  private static final String MESSAGE_COLNAME = "message";
  private static final String SEQ_NO_COLNAME = "seqNo";
  private static final String SESSION_ID_COLNAME = "sessionId";
  private static final String TABLE_NAME = "messages";

  private BoundStatement boundInsertStatement;
  private BoundStatement boundSelectMaxStatement;
  private BoundStatement boundSelectStatement;

  private Cluster cluster;
  // hosts ip to connect to
  private final String contactPoints;

  private int numberOfThreads = 1;
  private final Executor executor = Executors.newFixedThreadPool(numberOfThreads);
  /*
   * Sets number of simultaneous requests on all connections to an host after which more connections
   * are created(between 0 and 128).
   */
  private int maxSimultaneousRequests = 50;

  private Session session;

  public CassandraMessageStore(String contactPoints) {
    this.contactPoints = contactPoints;
  }

  /**
   * Build schema programmatically
   * <p>
   * DDL equivalent:
   * 
   * <pre>
   * CREATE TABLE messages (
   *   sessionId uuid,
   *   seqNo bigint,
   *   message blob,
   *   PRIMARY KEY  (sessionId, seqNo ) );
   * </pre>
   * 
   * @throws StoreException if the store is not open
   *
   */
  public void buildSchema() throws StoreException {
    if (session != null) {
      // Appropriate for a local test only
      session.execute(new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME
          + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"));
      System.out.format("Keyspace %s available\n", KEYSPACE_NAME);

      Create create = SchemaBuilder.createTable(KEYSPACE_NAME, TABLE_NAME).ifNotExists()
          .addPartitionKey(SESSION_ID_COLNAME, DataType.uuid())
          .addClusteringColumn(SEQ_NO_COLNAME, DataType.bigint())
          .addColumn(MESSAGE_COLNAME, DataType.blob());

      ResultSet resultSet = session.execute(create);
      System.out.format("Table %s available\n", TABLE_NAME);
    } else {
      throw new StoreException("Schema not created; store not open");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  public void close() throws Exception {
    if (session != null) {
      session.close();
      cluster.close();
      session = null;
    }
  }

  public void dropSchema() throws StoreException {
    if (session != null) {
      Drop drop = SchemaBuilder.dropTable(KEYSPACE_NAME, TABLE_NAME).ifExists();
      session.execute(drop);
      System.out.println("Schema dropped");
    } else {
      throw new StoreException("Schema not dropped; store not open");
    }
  }

  public void insertMessage(UUID sessionId, long seqNo, ByteBuffer message) throws StoreException {
    boundInsertStatement.bind(sessionId, seqNo, message);
    session.execute(boundInsertStatement);
  }

  @Override
  public CompletableFuture<CassandraMessageStore> open() {
    CompletableFuture<CassandraMessageStore> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {

        // Create session to hosts
        // PoolingOptions pools = new PoolingOptions();
        // pools.setMaxSimultaneousRequestsPerConnectionThreshold(
        // HostDistance.LOCAL, maxSimultaneousRequests);
        // int maxConnections = 1;
        // pools.setCoreConnectionsPerHost(HostDistance.LOCAL,
        // maxConnections);
        // pools.setMaxConnectionsPerHost(HostDistance.LOCAL,
        // maxConnections);
        // pools.setCoreConnectionsPerHost(HostDistance.REMOTE,
        // maxConnections);
        // pools.setMaxConnectionsPerHost(HostDistance.REMOTE,
        // maxConnections);

        cluster = new Cluster.Builder().addContactPoints(contactPoints)
            // .withPoolingOptions(pools)
            .withSocketOptions(new SocketOptions().setTcpNoDelay(true)).build();

        cluster.getConfiguration().getProtocolOptions()
            .setCompression(ProtocolOptions.Compression.LZ4);

        session = cluster.connect("system");

        Metadata metadata = cluster.getMetadata();
        System.out.println(String.format("Connected to cluster '%s' on %s.",
            metadata.getClusterName(), metadata.getAllHosts()));

        // if (!schemaExists()) {
        buildSchema();
        // }

        session.execute(new SimpleStatement("USE " + KEYSPACE_NAME));
        System.out.format("Using keyspace %s\n", KEYSPACE_NAME);

        boundInsertStatement = prepareInsertStatement();
        boundSelectStatement = prepareSelectStatement();
        boundSelectMaxStatement = prepareSelectMaxStatement();
      } catch (DriverException | StoreException ex) {
        future.completeExceptionally(ex);
      }
    });
    return future;
  }

  public long retrieveMaxSeqNo(UUID sessionId) {
    long seqNo = 0;
    boundSelectMaxStatement.bind(sessionId);
    final ResultSet resultSet = session.execute(boundSelectMaxStatement);

    final List<Row> rows = resultSet.all();
    final Iterator<Row> iterator = rows.iterator();
    if (iterator.hasNext()) {
      Row row = iterator.next();
      seqNo = row.getLong(SEQ_NO_COLNAME);
    }
    return seqNo;
  }

  public void retrieveMessagesAsync(final MessageStoreResult result,
      Consumer<MessageStoreResult> consumer) {
    final long fromSeqNo = result.getFromSeqNo();
    boundSelectStatement.bind(result.getSessionId(), fromSeqNo,
        fromSeqNo + result.getCountRequested());
    final ResultSetFuture resultSetFuture = session.executeAsync(boundSelectStatement);

    Function<ResultSet, MessageStoreResult> rowFunction = resultSet -> {
      final RowConsumer action = new RowConsumer(result);
      resultSet.forEach(action);
      return result;
    };

    ListenableFuture<MessageStoreResult> queryFuture =
        Futures.transform(resultSetFuture, rowFunction);
    FutureCallback<MessageStoreResult> retrievalCallback =
        new FutureCallback<MessageStoreResult>() {

          public void onFailure(Throwable ex) {
            ex.printStackTrace();
          }

          public void onSuccess(MessageStoreResult result) {
            consumer.accept(result);
          }

        };
    Futures.addCallback(queryFuture, retrievalCallback, executor);
  }

  public boolean schemaExists() throws StoreException {
    if (session != null) {
      boolean exists = false;
      Select select = QueryBuilder.select().column("keyspace_name").from("system.schema_keyspaces");
      ResultSet results = session.execute(select);

      for (Row row : results) {
        String name = row.getString("keyspace_name");
        if (KEYSPACE_NAME.equals(name)) {
          exists = true;
          break;
        }
      }
      return exists;
    } else {
      throw new StoreException("Keyspaces not available; store not open");
    }
  }

  private BoundStatement prepareInsertStatement() {
    PreparedStatement statement =
        session.prepare("INSERT INTO " + TABLE_NAME + "(" + SESSION_ID_COLNAME + ", "
            + SEQ_NO_COLNAME + ", " + MESSAGE_COLNAME + ") VALUES (?,?,?);");
    return new BoundStatement(statement);
  }

  private BoundStatement prepareSelectMaxStatement() {
    PreparedStatement statement = session.prepare("SELECT " + SEQ_NO_COLNAME + " FROM " + TABLE_NAME
        + " WHERE " + SESSION_ID_COLNAME + "= ?" + " ORDER BY " + SEQ_NO_COLNAME + " DESC LIMIT 1");
    return new BoundStatement(statement);
  }

  private BoundStatement prepareSelectStatement() {
    PreparedStatement statement = session.prepare(
        "SELECT " + MESSAGE_COLNAME + " FROM " + TABLE_NAME + " WHERE " + SESSION_ID_COLNAME
            + "= ? AND " + SEQ_NO_COLNAME + " >= ? AND " + SEQ_NO_COLNAME + " <= ?");
    return new BoundStatement(statement);
  }

}
