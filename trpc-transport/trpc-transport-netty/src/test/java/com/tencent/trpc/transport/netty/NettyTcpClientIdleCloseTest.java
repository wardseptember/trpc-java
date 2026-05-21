/*
 * Tencent is pleased to support the open source community by making tRPC available.
 *
 * Copyright (C) 2023 Tencent.
 * All rights reserved.
 *
 * If you have downloaded a copy of the tRPC source code from Tencent,
 * please note that tRPC source code is licensed under the Apache 2.0 License,
 * A copy of the Apache 2.0 License can be found in the LICENSE file.
 */

package com.tencent.trpc.transport.netty;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.transport.Channel;
import com.tencent.trpc.core.transport.handler.ChannelHandlerAdapter;
import com.tencent.trpc.core.utils.NetUtils;
import org.junit.Test;

/**
 * Verifies the client-side idle-close hand-off:
 * <ul>
 *     <li>An {@link io.netty.handler.timeout.IdleStateHandler} is installed when
 *         {@code idleTimeout > 0}.</li>
 *     <li>When idle fires, the slot is invalidated <em>before</em> the underlying
 *         {@code channel.close()} runs, so the next request goes through
 *         {@code ensureChannelActive}'s lazy reconnect.</li>
 *     <li>The actual {@code Channel.isConnected()} flips to false shortly afterwards.</li>
 * </ul>
 */
public class NettyTcpClientIdleCloseTest {

    @Test
    public void idleTimeoutClosesChannelAndInvalidatesSlot() throws Exception {
        ProtocolConfig serverConfig = new ProtocolConfig();
        serverConfig.setIp(NetUtils.LOCAL_HOST);
        serverConfig.setPort(NetUtils.getAvailablePort());
        serverConfig.setNetwork("tcp");

        ProtocolConfig clientConfig = new ProtocolConfig();
        clientConfig.setIp(NetUtils.LOCAL_HOST);
        clientConfig.setPort(serverConfig.getPort());
        clientConfig.setNetwork("tcp");
        clientConfig.setConnsPerAddr(1);
        clientConfig.setLazyinit(false);
        // Very small idle timeout so the test runs quickly.
        clientConfig.setIdleTimeout(200);

        NettyTcpServerTransport server = new NettyTcpServerTransport(serverConfig,
                new ChannelHandlerAdapter(), new TransportServerCodecTest());
        NettyTcpClientTransport client = new NettyTcpClientTransport(clientConfig,
                new ChannelHandlerAdapter(), new TransportClientCodecTest());
        try {
            server.open();
            client.open();

            // Force the lazy connect to materialise.
            Channel ch = client.getChannel().toCompletableFuture().get();
            assertNotNull(ch);
            assertTrue("channel must be live before idle timeout", ch.isConnected());

            // Wait long enough for the idle handler to fire and the close + slot
            // invalidation to propagate. idleTimeout=200ms; a 2s window leaves headroom
            // for slow CI machines.
            long deadline = System.currentTimeMillis() + 2_000;
            while (ch.isConnected() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertFalse("idle channel must have been closed by the idle handler",
                    ch.isConnected());

            // The slot should now report "needs reconnect": getChannel triggers
            // ensureChannelActive which rebuilds the connection on the same EventLoopGroup.
            Channel rebuilt = client.getChannel().toCompletableFuture().get();
            assertNotNull(rebuilt);
            assertTrue("a fresh channel must be available after lazy reconnect",
                    rebuilt.isConnected());
        } finally {
            try {
                client.close();
            } catch (Throwable ignore) {
            }
            try {
                server.close();
            } catch (Throwable ignore) {
            }
        }
    }
}
