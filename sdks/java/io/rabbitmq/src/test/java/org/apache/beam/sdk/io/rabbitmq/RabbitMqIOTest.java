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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test of {@link RabbitMqIO}.
 */
public class RabbitMqIOTest {

  public int port;

  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Broker broker;

  @Before
  public void startBroker() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }

    broker = new Broker();
    BrokerOptions options = new BrokerOptions();
    options.setConfigProperty("qpid.amqp_port", String.valueOf(port));
    options.setConfigProperty("qpid.work_dir", temporaryFolder.newFolder().toString());
    options.setConfigProperty("qpid.home_dir", "src/test/qpid");
    broker.startup(options);
  }

  @After
  public void stopBroker() throws Exception {
    broker.shutdown();
  }

  @Test
  public void testRead() throws Exception {
    PCollection<byte[]> output = pipeline.apply(
        RabbitMqIO.read().withUri("amqp://guest:guest@localhost:" + port).withQueue("READ")
            .withMaxNumRecords(10));
    PAssert.that(output)
        .containsInAnyOrder("Test 0".getBytes(), "Test 1".getBytes(), "Test 2".getBytes(),
            "Test 3".getBytes(), "Test 4".getBytes(), "Test 5".getBytes(), "Test 6".getBytes(),
            "Test 7".getBytes(), "Test 8".getBytes(), "Test 9".getBytes());

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare("READ", false, false, false, null);
    for (int i = 0; i < 10; i++) {
      channel.basicPublish("", "READ", null, ("Test " + i).getBytes());
    }

    pipeline.run();

    channel.close();
    connection.close();
  }

  @Test
  public void testWrite() throws Exception {
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      data.add(("Test " + i).getBytes());
    }
    pipeline.apply(Create.of(data)).apply(RabbitMqIO.write()
        .withUri("amqp://guest:guest@localhost:" + port).withQueue("WRITE"));
    pipeline.run();

    List<String> received = new ArrayList<>();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("localhost");
    connectionFactory.setPort(port);
    connectionFactory.setVirtualHost("/");
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume("WRITE", true, consumer);
    for (int i = 0; i < 1000; i++) {
      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
      received.add(new String(delivery.getBody()));
    }

    assertEquals(1000, received.size());
    for (int i = 0; i < 1000; i++) {
      assertTrue(received.contains("Test " + i));
    }

    channel.close();
    connection.close();
  }

}
