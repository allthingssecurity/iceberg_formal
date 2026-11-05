/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.mc5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Level-1 timing assistance (ENVIRONMENT latency, toxiproxy-style): a plain TCP forwarder in front
 * of the broker, used only as the sink's {@code iceberg.kafka.bootstrap.servers}. While armed, every
 * NEW connection is held for {@code delayMillis} before it is connected upstream, i.e. a slow
 * bootstrap round-trip for the Kafka clients the sink creates (the Admin client of
 * CommitterImpl.hasLeaderPartition is created fresh on every open()/close()). Bytes are forwarded
 * unchanged; already established connections are never delayed. Inert (pure pass-through) when not
 * armed. The Connect runtime's own consumers do not use this address.
 */
class DelayProxy implements AutoCloseable {
  private final ServerSocket server;
  private final String targetHost;
  private final int targetPort;
  private volatile long delayMillis = 0L;
  private final AtomicInteger delayedConnections = new AtomicInteger();
  private final AtomicInteger connections = new AtomicInteger();
  private volatile boolean closed = false;

  DelayProxy(String targetHost, int targetPort) throws IOException {
    this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    this.targetHost = targetHost;
    this.targetPort = targetPort;
    Thread acceptor = new Thread(this::acceptLoop, "mc5-proxy-accept");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  int port() {
    return server.getLocalPort();
  }

  private volatile long armedUntil = 0L;

  void arm(long millis) {
    this.armedUntil = 0L;
    this.delayMillis = millis;
  }

  /** Delay only connections accepted before {@code untilEpochMillis}. */
  void armUntil(long millis, long untilEpochMillis) {
    this.armedUntil = untilEpochMillis;
    this.delayMillis = millis;
  }

  void disarm() {
    this.delayMillis = 0L;
    this.armedUntil = 0L;
  }

  private long currentDelay() {
    long delay = delayMillis;
    long until = armedUntil;
    if (delay <= 0 || (until > 0 && System.currentTimeMillis() >= until)) {
      return 0L;
    }

    return delay;
  }

  int delayedConnections() {
    return delayedConnections.get();
  }

  int connections() {
    return connections.get();
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket client = server.accept();
        long delay = currentDelay();
        connections.incrementAndGet();
        if (delay > 0) {
          delayedConnections.incrementAndGet();
        }

        Thread handler = new Thread(() -> handle(client, delay), "mc5-proxy-conn");
        handler.setDaemon(true);
        handler.start();
      } catch (IOException e) {
        if (!closed) {
          System.out.println("[MC5] proxy accept error: " + e);
        }
      }
    }
  }

  private void handle(Socket client, long delay) {
    try {
      if (delay > 0) {
        Thread.sleep(delay);
      }

      Socket upstream = new Socket(targetHost, targetPort);
      client.setTcpNoDelay(true);
      upstream.setTcpNoDelay(true);
      pump(client, upstream, "c2s");
      pump(upstream, client, "s2c");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeQuietly(client);
    } catch (IOException e) {
      closeQuietly(client);
    }
  }

  private static void pump(Socket from, Socket to, String dir) {
    Thread thread =
        new Thread(
            () -> {
              byte[] buffer = new byte[64 * 1024];
              try (InputStream in = from.getInputStream();
                  OutputStream out = to.getOutputStream()) {
                int read;
                while ((read = in.read(buffer)) >= 0) {
                  out.write(buffer, 0, read);
                  out.flush();
                }
              } catch (IOException e) {
                // connection closed
              } finally {
                closeQuietly(from);
                closeQuietly(to);
              }
            },
            "mc5-proxy-" + dir);
    thread.setDaemon(true);
    thread.start();
  }

  private static void closeQuietly(Socket socket) {
    try {
      socket.close();
    } catch (IOException e) {
      // ignore
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    server.close();
  }
}
