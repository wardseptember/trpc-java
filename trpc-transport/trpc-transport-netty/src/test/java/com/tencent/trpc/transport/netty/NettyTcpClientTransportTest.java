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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.transport.handler.ChannelHandlerAdapter;
import com.tencent.trpc.core.utils.NetUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollChannelOption;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.Test;

/**
 * White-box coverage for {@link NettyTcpClientTransport}'s long-connection helpers:
 * {@code resolveIdleTimeoutMills}, {@code applyTcpKeepAliveTuning} and the pipeline
 * wiring driven by {@code idleTimeout}. The pipeline assertions go through a real
 * {@code doOpen} on an unbound bootstrap — no socket is ever opened.
 */
public class NettyTcpClientTransportTest {

    /**
     * {@code resolveIdleTimeoutMills} must return 0 for {@code null} / non-positive
     * configurations (legacy "disabled" semantics) and the raw value otherwise.
     */
    @Test
    public void testResolveIdleTimeoutMillsBranches() throws Exception {
        Method m = NettyTcpClientTransport.class.getDeclaredMethod("resolveIdleTimeoutMills");
        m.setAccessible(true);

        // Disabled — null / 0 / negative all collapse to 0.
        NettyTcpClientTransport tNull = newTransport(null);
        assertEquals(0L, ((Long) m.invoke(tNull)).longValue());
        tNull.close();

        NettyTcpClientTransport tZero = newTransport(0);
        assertEquals(0L, ((Long) m.invoke(tZero)).longValue());
        tZero.close();

        NettyTcpClientTransport tNeg = newTransport(-1);
        assertEquals(0L, ((Long) m.invoke(tNeg)).longValue());
        tNeg.close();

        // Enabled — raw value is returned.
        NettyTcpClientTransport tPos = newTransport(180_000);
        assertEquals(180_000L, ((Long) m.invoke(tPos)).longValue());
        tPos.close();
    }

    /**
     * Each TCP keepalive parameter is applied independently and only when strictly positive.
     * The test sets non-default values for all three and asserts they appear on the
     * bootstrap; a separate test below covers the no-op branches.
     */
    @Test
    public void testApplyTcpKeepAliveTuningSetsAllPositiveValues() throws Exception {
        ProtocolConfig config = newConfig(180_000);
        config.setTcpKeepAliveIdle(45);
        config.setTcpKeepAliveIntvl(15);
        config.setTcpKeepAliveCnt(7);

        NettyTcpClientTransport transport = new NettyTcpClientTransport(config,
                new ChannelHandlerAdapter(), null);
        try {
            Bootstrap bootstrap = new Bootstrap();
            invokeApplyTcpKeepAliveTuning(transport, bootstrap);
            Map<ChannelOption<?>, Object> opts = bootstrap.config().options();
            assertEquals(45, opts.get(EpollChannelOption.TCP_KEEPIDLE));
            assertEquals(15, opts.get(EpollChannelOption.TCP_KEEPINTVL));
            assertEquals(7, opts.get(EpollChannelOption.TCP_KEEPCNT));
        } finally {
            transport.close();
        }
    }

    /**
     * Non-positive / null values must NOT be propagated to the bootstrap — the kernel
     * default is preserved. This branch matters because mis-configured yamls (negative
     * values) would otherwise surface as netty option errors at connect time.
     */
    @Test
    public void testApplyTcpKeepAliveTuningSkipsNonPositive() throws Exception {
        ProtocolConfig config = newConfig(180_000);
        // Idle is positive and must be set; the other two are zero / negative and must NOT.
        config.setTcpKeepAliveIdle(30);
        config.setTcpKeepAliveIntvl(0);
        config.setTcpKeepAliveCnt(-1);

        NettyTcpClientTransport transport = new NettyTcpClientTransport(config,
                new ChannelHandlerAdapter(), null);
        try {
            Bootstrap bootstrap = new Bootstrap();
            invokeApplyTcpKeepAliveTuning(transport, bootstrap);
            Map<ChannelOption<?>, Object> opts = bootstrap.config().options();
            assertEquals(30, opts.get(EpollChannelOption.TCP_KEEPIDLE));
            assertNull("non-positive intvl must not be propagated",
                    opts.get(EpollChannelOption.TCP_KEEPINTVL));
            assertNull("negative cnt must not be propagated",
                    opts.get(EpollChannelOption.TCP_KEEPCNT));
        } finally {
            transport.close();
        }
    }

    /**
     * When {@code idleTimeout > 0}, {@code doOpen} must register both
     * {@code idleState} and {@code idleClose} pipeline handlers. Driven via a real TCP
     * connect against a throwaway {@link java.net.ServerSocket} so netty actually runs
     * the {@code ChannelInitializer} and the assertions inspect a populated pipeline.
     */
    @Test
    public void testDoOpenInstallsIdlePipelineWhenEnabled() throws Exception {
        ChannelPipeline pipeline = pipelineAfterRealConnect(180_000);
        assertNotNull("idleState handler must be installed when idleTimeout > 0",
                pipeline.get("idleState"));
        assertNotNull("idleClose handler must be installed when idleTimeout > 0",
                pipeline.get("idleClose"));
    }

    /**
     * When {@code idleTimeout <= 0}, {@code doOpen} must skip both idle-related handlers —
     * the legacy "disabled" mode continues to work for one-way RPC callers.
     */
    @Test
    public void testDoOpenSkipsIdlePipelineWhenDisabled() throws Exception {
        ChannelPipeline pipeline = pipelineAfterRealConnect(0);
        assertNull("idleState handler must NOT be installed when idleTimeout = 0",
                pipeline.get("idleState"));
        assertNull("idleClose handler must NOT be installed when idleTimeout = 0",
                pipeline.get("idleClose"));
    }

    /**
     * {@code useChannelPool} reflects {@code keepAlive}. Default {@code keepAlive=true} →
     * pool, explicit {@code false} → no pool.
     */
    @Test
    public void testUseChannelPoolFollowsKeepAlive() throws Exception {
        ProtocolConfig poolCfg = newConfig(180_000);
        poolCfg.setKeepAlive(true);
        NettyTcpClientTransport withPool = new NettyTcpClientTransport(poolCfg,
                new ChannelHandlerAdapter(), null);
        try {
            assertTrue(invokeUseChannelPool(withPool));
        } finally {
            withPool.close();
        }

        ProtocolConfig noPoolCfg = newConfig(180_000);
        noPoolCfg.setKeepAlive(false);
        NettyTcpClientTransport noPool = new NettyTcpClientTransport(noPoolCfg,
                new ChannelHandlerAdapter(), null);
        try {
            assertFalse(invokeUseChannelPool(noPool));
        } finally {
            noPool.close();
        }
    }

    /**
     * Helper: invoke private {@code applyTcpKeepAliveTuning(Bootstrap)}.
     */
    private static void invokeApplyTcpKeepAliveTuning(NettyTcpClientTransport t,
            Bootstrap bootstrap) throws Exception {
        Method m = NettyTcpClientTransport.class
                .getDeclaredMethod("applyTcpKeepAliveTuning", Bootstrap.class);
        m.setAccessible(true);
        m.invoke(t, bootstrap);
    }

    /**
     * Helper: invoke protected {@code useChannelPool()}.
     */
    private static boolean invokeUseChannelPool(NettyTcpClientTransport t) throws Exception {
        Method m = NettyTcpClientTransport.class.getDeclaredMethod("useChannelPool");
        m.setAccessible(true);
        return (boolean) m.invoke(t);
    }

    /**
     * Stand up a throwaway plain {@link java.net.ServerSocket}, point a {@link NettyTcpClientTransport}
     * at it with the given {@code idleTimeout}, force the lazy connect, and return the
     * resulting pipeline of the connected netty channel. The server socket immediately
     * accepts the connection and never writes anything, so READ_IDLE handlers (when
     * installed) would eventually fire — but the test inspects the pipeline before that.
     */
    private static ChannelPipeline pipelineAfterRealConnect(int idleTimeout) throws Exception {
        java.net.ServerSocket server = new java.net.ServerSocket(0);
        server.setSoTimeout(2000);
        Thread accept = new Thread(() -> {
            try {
                java.net.Socket s = server.accept();
                // Hold the socket; do not write anything. Closed implicitly when the
                // accept thread exits.
                Thread.sleep(1500);
                try {
                    s.close();
                } catch (Throwable ignore) {
                }
            } catch (Throwable ignore) {
                // Test may finish quickly and leave accept blocked — that's fine.
            }
        }, "test-accept");
        accept.setDaemon(true);
        accept.start();

        ProtocolConfig config = new ProtocolConfig();
        config.setIp("127.0.0.1");
        config.setPort(server.getLocalPort());
        config.setNetwork("tcp");
        config.setConnsPerAddr(1);
        config.setLazyinit(false);
        config.setKeepAlive(true);
        config.setIoThreadGroupShare(false);
        config.setIdleTimeout(idleTimeout);

        NettyTcpClientTransport client = new NettyTcpClientTransport(config,
                new ChannelHandlerAdapter(), new TransportClientCodecTest());
        try {
            client.open();
            // Drive the connect synchronously and capture the connected netty channel.
            com.tencent.trpc.core.transport.Channel wrapper =
                    client.getChannel().toCompletableFuture().get(2, java.util.concurrent.TimeUnit.SECONDS);
            assertNotNull(wrapper);
            assertTrue("channel must be connected", wrapper.isConnected());
            // Pull the underlying io.netty.channel.Channel via reflection so we can grab
            // the pipeline. NettyChannel keeps it as a private field "ioChannel".
            java.lang.reflect.Field ioChannelField = wrapper.getClass()
                    .getDeclaredField("ioChannel");
            ioChannelField.setAccessible(true);
            io.netty.channel.Channel ioChannel = (io.netty.channel.Channel) ioChannelField.get(wrapper);
            return ioChannel.pipeline();
        } finally {
            try {
                client.close();
            } catch (Throwable ignore) {
            }
            try {
                server.close();
            } catch (Throwable ignore) {
            }
            accept.interrupt();
        }
    }

    private static NettyTcpClientTransport newTransport(Integer idleTimeout) throws Exception {
        ProtocolConfig config = newConfig(idleTimeout);
        return new NettyTcpClientTransport(config, new ChannelHandlerAdapter(),
                new TransportClientCodecTest());
    }

    private static ProtocolConfig newConfig(Integer idleTimeout) {
        ProtocolConfig config = new ProtocolConfig();
        config.setIp(NetUtils.LOCAL_HOST);
        config.setPort(NetUtils.getAvailablePort());
        config.setNetwork("tcp");
        config.setConnsPerAddr(1);
        config.setLazyinit(true);
        config.setKeepAlive(true);
        // Independent EventLoopGroup so each test cleans up its own threads.
        config.setIoThreadGroupShare(false);
        // Always set idleTimeout explicitly: production default (Constants) is 180_000 and
        // the disabled-path tests need it overridden to 0 / negative.
        config.setIdleTimeout(idleTimeout == null ? -1 : idleTimeout);
        return config;
    }
}
