/*
 * Copyright (c) 2018, 2021 Oracle and/or its affiliates.
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
 */

package io.helidon.examples.quickstart.se;

import io.helidon.common.LogConfig;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.media.jsonp.JsonpSupport;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.tyrus.TyrusSupport;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.transport.ServerTransport.ConnectionAcceptor;
import io.rsocket.util.DefaultPayload;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.websocket.CloseReason;
import javax.websocket.Encoder;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Builder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Many;

/**
 * The application main class.
 */
public final class Main {

  /**
   * Cannot be instantiated.
   */
  private Main() {
  }

  /**
   * Application main entry point.
   *
   * @param args command line arguments.
   */
  public static void main(final String[] args) {
    startServer();
  }

  /**
   * Start the server.
   *
   * @return the created {@link WebServer} instance
   */
  static Single<WebServer> startServer() {

    // load logging configuration
    LogConfig.configureRuntime();

    // By default this will pick up application.yaml from the classpath
    Config config = Config.create();

    WebServer server = WebServer.builder(createRouting(config))
        .config(config.get("server"))
        .addMediaSupport(JsonpSupport.create())
        .build();

    Single<WebServer> webserver = server.start();

    // Try to start the server. If successful, print some info and arrange to
    // print a message at shutdown. If unsuccessful, print the exception.
    webserver.thenAccept(ws -> {
      System.out.println("WEB server is up! http://localhost:" + ws.port() + "/greet");
      ws.whenShutdown().thenRun(() -> System.out.println("WEB server is DOWN. Good bye!"));
    })
        .exceptionallyAccept(t -> {
          System.err.println("Startup failed: " + t.getMessage());
          t.printStackTrace(System.err);
        });

    return webserver;
  }

  public class MessageBoardEndpoint extends Endpoint {

    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
      session.addMessageHandler(new MessageHandler.Whole<String>() {
        @Override
        public void onMessage(String message) {
          System.out.println(message);
        }
      });
    }
  }

  /**
   * Creates new {@link Routing}.
   *
   * @param config configuration of this server
   * @return routing configured with JSON support, a health check, and a service
   */
  private static Routing createRouting(Config config) {

    MetricsSupport metrics = MetricsSupport.create();
    GreetService greetService = new GreetService(config);
    HealthSupport health = HealthSupport.builder()
        .addLiveness(HealthChecks.healthChecks())   // Adds a convenient set of checks
        .build();

    final ConnectionAcceptor connectionAcceptor = RSocketServer
        .create()
        .acceptor(SocketAcceptor.forRequestResponse(p -> {
          p.release();
          return Mono.just(DefaultPayload.create("hello world"));
        }))
        .asConnectionAcceptor();

//    final RSocketEndpoint rSocketEndpoint = new RSocketEndpoint(connectionAcceptor);
////
//    final TyrusSupport tyrusSupport = TyrusSupport.builder()
//        .register(MessageBoardEndpoint.class, "/test")
//        .build();

    // ---[ RSocket ] [Codecs] --< Out | Int >

    List<Class<? extends Encoder>> encoders =
        Collections.emptyList();

    final TyrusSupport tyrusSupport = TyrusSupport.builder().register(
        Builder.create(
            MessageBoardEndpoint.class, "/my").build())
        .build();

    return Routing.builder()
        .register("/websocket",
            TyrusSupport.builder().register(
                ServerEndpointConfig.Builder.create(
                    MessageBoardEndpoint.class, "/board").encoders(
                    encoders).build()).build())
        .build();
  }

  public static class HelidonDuplexConnection implements DuplexConnection {

    final Session session;
    final Sinks.Empty<Void> onCloseSink;

    HelidonDuplexConnection(Session session) {
      this.onCloseSink = Sinks.empty();
      this.session = session;
    }

    @Override
    public void sendFrame(int i, ByteBuf byteBuf) {
      try {
        final ByteBuf bb = Unpooled.copiedBuffer(byteBuf);
        session.getAsyncRemote().sendBinary(bb.nioBuffer());
      } finally {
        byteBuf.release();
      }
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException e) {
      final ByteBuf bb = ErrorFrameCodec.encode(UnpooledByteBufAllocator.DEFAULT, 0, e);
      session.getAsyncRemote().sendBinary(bb.nioBuffer());
    }

    @Override
    public Flux<ByteBuf> receive() {
      final Many<ByteBuf> sink = Sinks.<ByteBuf>unsafe().many().multicast().directBestEffort();
      session.addMessageHandler(ByteBuffer.class,
          message -> sink.emitNext(Unpooled.wrappedBuffer(message), EmitFailureHandler.FAIL_FAST));
      return sink.asFlux();
    }

    @Override
    public ByteBufAllocator alloc() {
      return UnpooledByteBufAllocator.DEFAULT;
    }

    @Override
    public SocketAddress remoteAddress() {
      return null;
    }

    @Override
    public Mono<Void> onClose() {
      return onCloseSink.asMono();
    }

    @Override
    public void dispose() {

    }
  }

  @ServerEndpoint("/")
  public static class RSocketEndpoint extends Endpoint {

    final ConnectionAcceptor connectionAcceptor;
    final Map<String, HelidonDuplexConnection> connections = new ConcurrentHashMap<>();

    public RSocketEndpoint(
        ConnectionAcceptor connectionAcceptor) {
      this.connectionAcceptor = connectionAcceptor;
    }


    @Override
    public void onOpen(Session session, EndpointConfig endpointConfig) {
      final HelidonDuplexConnection connection = new HelidonDuplexConnection(session);
      connections.put(session.getId(), connection);
      connectionAcceptor.apply(connection).subscribe();
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
      connections.get(session.getId()).onCloseSink.tryEmitEmpty();
    }
  }
}
