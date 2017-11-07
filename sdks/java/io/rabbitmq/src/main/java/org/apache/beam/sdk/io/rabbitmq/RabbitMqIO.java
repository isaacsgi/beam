/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.rabbitmq;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A IO to publish or consume messages with a RabbitMQ broker.
 *
 * <h3>Consuming messages from RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Read} returns an unbounded {@link PCollection} containing RabbitMQ
 * messages body (as {@code byte[]}).
 *
 * <p>To configure a RabbitMQ source, you have to provide a RabbitMQ {@code URI} to connect
 * to a RabbitMQ broker. The following example illustrates various options for configuring the
 * source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(
 *    RabbitMqIO.read().withUri("amqp://user:password@localhost:5672/QUEUE")
 *
 * }</pre>
 *
 * <h3>Publishing messages to RabbitMQ server</h3>
 *
 * <p>{@link RabbitMqIO} {@link Write} can send {@code byte[]} to a RabbitMQ server queue.
 *
 * <p>As for the {@link Read}, the {@link Write} is configured with a RabbitMQ
 * {@link ConnectionConfig}.
 *
 * <p>For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<byte[]>
 *    .apply(RabbitMqIO.write()
 *      .withConnectionConfig(RabbitMqIO.ConnectionConfig.create("localhost", 5672, "/", "QUEUE")));
 *
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class RabbitMqIO {

  public static Read read() {
    return new AutoValue_RabbitMqIO_Read.Builder()
        .setConnectionConfig(ConnectionConfig.create())
        .setMaxReadTime(null).setMaxNumRecords(Long.MAX_VALUE).setUseCorrelationId(false).build();
  }

  public static Write write() {
    return new AutoValue_RabbitMqIO_Write.Builder()
        .setConnectionConfig(ConnectionConfig.create()).build();
  }

  private RabbitMqIO() {
  }

  /**
   * Describe a connection configuration to a RabbitMQ server.
   */
  @AutoValue
  public abstract static class ConnectionConfig implements Serializable {

    @Nullable abstract String uri();
    @Nullable abstract String queue();

    abstract int networkRecoveryInterval();
    abstract boolean automaticRecovery();
    abstract boolean topologyRecovery();

    abstract int connectionTimeout();
    abstract int requestedChannelMax();
    abstract int requestedFrameMax();
    abstract int requestedHeartbeat();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUri(String uri);
      abstract Builder setQueue(String queue);
      abstract Builder setNetworkRecoveryInterval(int networkRecoveryInterval);
      abstract Builder setAutomaticRecovery(boolean automaticRecovery);
      abstract Builder setTopologyRecovery(boolean topologyRecovery);
      abstract Builder setConnectionTimeout(int connectionTimeout);
      abstract Builder setRequestedChannelMax(int requestedChannelMax);
      abstract Builder setRequestedFrameMax(int requestedFrameMax);
      abstract Builder setRequestedHeartbeat(int requestedHeartbeat);
      abstract ConnectionConfig build();
    }

    public static ConnectionConfig create() {
      return new AutoValue_RabbitMqIO_ConnectionConfig.Builder()
          .setUri("amqp://localhost:5672")
          .setQueue("DEFAULT")
          .setAutomaticRecovery(true)
          .setTopologyRecovery(true)
          .setConnectionTimeout(60000)
          .setRequestedChannelMax(0)
          .setRequestedFrameMax(0)
          .setRequestedHeartbeat(60)
          .setNetworkRecoveryInterval(5000)
          .build();
    }

    /**
     * Create a RabbitMQ connection configuration.
     *
     * @param uri The RabbitMQ server URI.
     * @param queue The RabbitMQ queue destination.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public static ConnectionConfig create(String uri, String queue) {
      checkArgument(uri != null, "uri can not be null");
      return new AutoValue_RabbitMqIO_ConnectionConfig.Builder().setUri(uri)
          .setQueue(queue)
          .setAutomaticRecovery(true)
          .setTopologyRecovery(true)
          .setConnectionTimeout(60000)
          .setRequestedChannelMax(0)
          .setRequestedFrameMax(0)
          .setRequestedHeartbeat(60)
          .setNetworkRecoveryInterval(5000)
          .build();
    }

    /**
     * Define the RabbitMQ URI.
     *
     * @param uri The RabbitMQ URI.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return builder().setUri(uri).build();
    }

    /**
     * Define the queue destination.
     *
     * @param queue The queue name.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setQueue(queue).build();
    }

    /**
     * Define the RabbitMQ connection network recovery interval.
     *
     * @param networkRecoveryInterval The network recovery interval (in ms).
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withNetworkRecoveryInterval(int networkRecoveryInterval) {
      checkArgument(networkRecoveryInterval >= 0,
          "networkRecoveryInterval has to be positive or 0");
      return builder().setNetworkRecoveryInterval(networkRecoveryInterval).build();
    }

    /**
     * Define the RabbitMQ connection automatic recovery.
     *
     * @param automaticRecovery True to enable automatic recovery on the RabbitMQ connection,
     *                          false else.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withAutomaticRecovery(boolean automaticRecovery) {
      return builder().setAutomaticRecovery(automaticRecovery).build();
    }

    /**
     * Define the RabbitMQ connection topology recovery.
     *
     * @param topologyRecovery True to enable topology recovery on the RabbitMQ connection, false
     *                         else.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withTopologyRecovery(boolean topologyRecovery) {
      return builder().setTopologyRecovery(topologyRecovery).build();
    }

    /**
     * Define the RabbitMQ connection timeout.
     *
     * @param connectionTimeout The connection timeout in ms.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withConnectionTimeout(int connectionTimeout) {
      checkArgument(connectionTimeout >= 0,
          "connectionTimeout has to be positive or 0");
      return builder().setConnectionTimeout(connectionTimeout).build();
    }

    /**
     * Define the RabbitMQ requested channel max number.
     *
     * @param requestedChannelMax The max number of requested channel.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withRequestedChannelMax(int requestedChannelMax) {
      checkArgument(requestedChannelMax >= 0,
          "requestedChannelMax has to be positive or 0");
      return builder().setRequestedChannelMax(requestedChannelMax).build();
    }

    /**
     * Define the RabbitMQ requested frame max number.
     *
     * @param requestedFrameMax The max number of requested frame.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withRequestedFrameMax(int requestedFrameMax) {
      checkArgument(requestedFrameMax >= 0,
          "requestedFrameMax has to be positive or 0");
      return builder().setRequestedFrameMax(requestedFrameMax).build();
    }

    /**
     * Define the RabbitMQ requested heartbeat number.
     *
     * @param requestedHeartbeat The number of requested heartbeat to perform.
     * @return The corresponding {@link ConnectionConfig}.
     */
    public ConnectionConfig withRequestedHeartbeat(int requestedHeartbeat) {
      checkArgument(requestedHeartbeat >= 0,
          "requestedHeartbeat has to be positive or 0");
      return builder().setRequestedHeartbeat(requestedHeartbeat).build();
    }

    ConnectionFactory createConnectionFactory() throws URISyntaxException,
        NoSuchAlgorithmException, KeyManagementException {
      ConnectionFactory connectionFactory = new ConnectionFactory();
      connectionFactory.setUri(uri());

      connectionFactory.setAutomaticRecoveryEnabled(automaticRecovery());
      connectionFactory.setConnectionTimeout(connectionTimeout());
      connectionFactory.setNetworkRecoveryInterval(networkRecoveryInterval());
      connectionFactory.setRequestedHeartbeat(requestedHeartbeat());
      connectionFactory.setTopologyRecoveryEnabled(topologyRecovery());
      connectionFactory.setRequestedChannelMax(requestedChannelMax());
      connectionFactory.setRequestedFrameMax(requestedFrameMax());

      return connectionFactory;
    }

  }

  /**
   * A {@link PTransform} to consume messages from RabbitMQ server.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    @Nullable abstract ConnectionConfig connectionConfig();
    @Nullable abstract Boolean useCorrelationId();
    abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfig(ConnectionConfig connectionConfig);
      abstract Builder setUseCorrelationId(Boolean useCorrelationId);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    public Read withConnectionConfig(ConnectionConfig connectionConfig) {
      checkArgument(connectionConfig != null, "connectionConfig can not be null");
      return builder().setConnectionConfig(connectionConfig).build();
    }

    public Read withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return builder().setConnectionConfig(connectionConfig().withUri(uri)).build();
    }

    public Read withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setConnectionConfig(connectionConfig().withQueue(queue)).build();
    }


    /**
     * Define the max number of records received by the {@link Read}.
     * When this max number of records is lower than {@code Long.MAX_VALUE}, the {@link Read}
     * will provide a bounded {@link PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null,
          "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages.
     * When this max read time is not null, the {@link Read} will provide a bounded
     * {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxNumRecords() == Long.MAX_VALUE,
          "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<byte[]> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<byte[]> unbounded =
          org.apache.beam.sdk.io.Read.from(new RabbitMQSource(this));

      PTransform<PBegin, PCollection<byte[]>> transform = unbounded;

      if (maxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords());
      } else if (maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(connectionConfig() != null, "RabbitMqIO.read() requires a connection config "
          + "to be set via withConnectionConfig(config)");
    }

  }

  static class RabbitMQSource extends UnboundedSource<byte[], RabbitMQCheckpointMark> {

    final Read spec;

    public RabbitMQSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
      return ByteArrayCoder.of();
    }

    @Override
    public List<RabbitMQSource> split(int desiredNumSplits,
                                      PipelineOptions options) {
      // RabbitMQ uses queue, so, we can have several concurrent consumers as source
      List<RabbitMQSource> sources = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        sources.add(new RabbitMQSource(spec));
      }
      return sources;
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options,
                                                RabbitMQCheckpointMark checkpointMark) {
      return new UnboundedRabbitMqReader(this, checkpointMark);
    }

    @Override
    public Coder<RabbitMQCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(RabbitMQCheckpointMark.class);
    }
  }

  static class RabbitMQCheckpointMark
      implements UnboundedSource.CheckpointMark, Serializable {

    transient Channel channel;
    Instant oldestTimestamp;
    final List<String> correlationIds = new ArrayList<>();
    final List<Long> sessionIds = new ArrayList<>();

    @Override
    public void finalizeCheckpoint() throws IOException {
      for (Long sessionId : sessionIds) {
        channel.basicAck(sessionId, false);
      }
      channel.txCommit();
      oldestTimestamp = Instant.now();
      correlationIds.clear();
      sessionIds.clear();
    }

  }

  private static class UnboundedRabbitMqReader extends UnboundedSource.UnboundedReader<byte[]> {

    private final RabbitMQSource source;

    private byte[] current;
    private Connection connection;
    private Channel channel;
    private QueueingConsumer consumer;
    private Instant currentTimestamp;
    private RabbitMQCheckpointMark checkpointMark;

    public UnboundedRabbitMqReader(RabbitMQSource source,
                                   RabbitMQCheckpointMark checkpointMark) {
      this.source = source;
      this.current = null;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new RabbitMQCheckpointMark();
      }
    }

    @Override
    public Instant getWatermark() {
      return checkpointMark.oldestTimestamp;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return checkpointMark;
    }

    @Override
    public RabbitMQSource getCurrentSource() {
      return source;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (currentTimestamp == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public byte[] getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public boolean start() throws IOException {
      ConnectionFactory connectionFactory;
      try {
        connectionFactory =
            source.spec.connectionConfig().createConnectionFactory();
      } catch (Exception e) {
        throw new IOException(e);
      }
      try {
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        if (channel == null) {
          throw new IOException("No RabbitMQ channel available");
        }
        channel.queueDeclare(source.spec.connectionConfig().queue(), false, false, false, null);
        checkpointMark.channel = channel;
        consumer = new QueueingConsumer(channel);
        channel.txSelect();
        channel.basicConsume(source.spec.connectionConfig().queue(), false, consumer);
      } catch (Exception e) {
        throw new IOException(e);
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      try {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        if (source.spec.useCorrelationId()) {
          String correlationId = delivery.getProperties().getCorrelationId();
          if (correlationId == null) {
            throw new IOException("RabbitMqIO.Read uses message correlation ID, but received "
                + "message has a null correlation ID");
          }
          if (checkpointMark.correlationIds.contains(correlationId)) {
            // deduplication, we have already processed this message
            return true;
          }
          checkpointMark.correlationIds.add(correlationId);
        }
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        checkpointMark.sessionIds.add(deliveryTag);
        current = delivery.getBody();
        currentTimestamp = new Instant(delivery.getProperties().getTimestamp());
        if (currentTimestamp.isBefore(checkpointMark.oldestTimestamp)) {
          checkpointMark.oldestTimestamp = currentTimestamp;
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      return true;
    }

    @Override
    public void close() throws IOException {
      if (channel != null) {
        try {
          channel.close();
        } catch (Exception e) {
          // ignore
        }
      }
      if (connection != null) {
        connection.close();
      }
    }

  }

  /**
   * A {@link PTransform} to publish messages to a RabbitMQ server.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    @Nullable abstract ConnectionConfig connectionConfig();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfig(ConnectionConfig connectionConfig);
      abstract Write build();
    }

    /**
     * Define the {@link ConnectionConfig} to RabbitMQ server.
     */
    public Write withConnectionConfig(ConnectionConfig connectionConfig) {
      checkArgument(connectionConfig != null, "connectionConfig can not be null");
      return builder().setConnectionConfig(connectionConfig).build();
    }

    public Write withUri(String uri) {
      checkArgument(uri != null, "uri can not be null");
      return builder().setConnectionConfig(connectionConfig().withUri(uri)).build();
    }

    public Write withQueue(String queue) {
      checkArgument(queue != null, "queue can not be null");
      return builder().setConnectionConfig(connectionConfig().withQueue(queue)).build();
    }

    @Override
    public PDone expand(PCollection<byte[]> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriteFn extends DoFn<byte[], Void> {

      private final Write spec;

      private transient Connection connection;
      private transient Channel channel;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        ConnectionFactory connectionFactory = spec.connectionConfig().createConnectionFactory();
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        if (channel == null) {
          throw new IOException("No RabbitMQ channel available");
        }
        channel.queueDeclare(spec.connectionConfig().queue(), false, false, false, null);
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws IOException {
        byte[] element = processContext.element();
        channel.basicPublish("", spec.connectionConfig().queue(), null, element);
      }

      @Teardown
      public void teardown() throws Exception {
        if (channel != null) {
          channel.close();
        }
        if (connection != null) {
          connection.close();
        }
      }

    }

  }

}
