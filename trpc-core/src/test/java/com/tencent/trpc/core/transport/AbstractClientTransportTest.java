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

package com.tencent.trpc.core.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.exception.TransportException;
import com.tencent.trpc.core.transport.codec.ClientCodec;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class AbstractClientTransportTest {

    @Test
    public void testOpenException() throws Exception {
        ClientTransportTest test = new ClientTransportTest(TransporterTestUtils.newProtocolConfig(),
                TransporterTestUtils.newChannelHandler(), TransporterTestUtils.newClientCodec(), false);
        try {
            test.open();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(e instanceof TransportException && e.getCause() instanceof IllegalArgumentException);
        }

        ClientTransportTest test2 = new ClientTransportTest(TransporterTestUtils.newProtocolConfig(),
                TransporterTestUtils.newChannelHandler(), TransporterTestUtils.newClientCodec(), true);
        try {
            test2.open();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(e instanceof TransportException && e.getCause() == null);
        }
        test2.close();
        assertTrue(test2.isClosed());
        try {
            test2.getChannel();
        } catch (Exception e) {
            assertTrue(e instanceof TransportException && e.getCause() == null);
        }
        test2.toString();
    }

    /**
     * Thundering-herd regression: when a long connection has just disconnected and many
     * concurrent requests find the slot unavailable, the transport must rebuild the slot
     * exactly ONCE — not once per requesting thread. Without the in-lock double-check this
     * test would observe makeCount &gt; 1 and the peer would see a connect/disconnect storm.
     */
    @Test
    public void testEnsureChannelActiveDoesNotStorm() throws Exception {
        StormTransport transport = new StormTransport(TransporterTestUtils.newProtocolConfig(),
                TransporterTestUtils.newChannelHandler(), TransporterTestUtils.newClientCodec());
        // Pre-populate slot 0 with a "broken" item: future done but channel disconnected.
        CompletableFuture<Channel> dead = new CompletableFuture<>();
        dead.complete(new DisconnectedChannel());
        transport.installSlot(new AbstractClientTransport.ChannelFutureItem(dead, transport.getProtocolConfig()));

        int threads = 64;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Method m = AbstractClientTransport.class.getDeclaredMethod("ensureChannelActive", int.class);
        m.setAccessible(true);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    m.invoke(transport, 0);
                } catch (Exception ignore) {
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdownNow();

        // Exactly one rebuild — that's the whole point of the in-lock double check.
        assertEquals("slot must be rebuilt exactly once under thundering-herd",
                1, transport.makeCount.get());
        // The slot should now hold the freshly-built item.
        assertSame(transport.lastBuilt, transport.peekSlot(0).getChannelFuture());
    }

    /**
     * Regression for the idle-close hand-off path: when an idle handler invalidates a slot
     * <em>before</em> the actual {@code channel.close()}, the next request must observe
     * "needs reconnect" (slot is now {@code isNotYetConnect=true}) instead of routing onto
     * the about-to-be-closed channel. This shrinks the "request lands on a closing channel"
     * race window from "close completes" to "close is enqueued".
     */
    @Test
    public void testInvalidateChannelReplacesSlotWithBlankPlaceholder() throws Exception {
        StormTransport transport = new StormTransport(TransporterTestUtils.newProtocolConfig(),
                TransporterTestUtils.newChannelHandler(), TransporterTestUtils.newClientCodec());
        // Pre-populate slot 0 with a connected (but soon-to-be-stale) channel.
        ConnectedChannel live = new ConnectedChannel();
        CompletableFuture<Channel> alive = new CompletableFuture<>();
        alive.complete(live);
        transport.installSlot(new AbstractClientTransport.ChannelFutureItem(alive,
                transport.getProtocolConfig()));

        // Sanity: before invalidation the slot is "available", so a request would route
        // onto the live channel.
        AbstractClientTransport.ChannelFutureItem before = transport.peekSlot(0);
        assertSame(alive, before.getChannelFuture());

        // Idle handler hands off here.
        transport.invalidateChannel(live);

        AbstractClientTransport.ChannelFutureItem after = transport.peekSlot(0);
        // After invalidation: slot is a blank placeholder, the next ensureChannelActive
        // will treat it as needing a reconnect (isNotYetConnect=true).
        assertTrue("slot must be replaced (different item identity)", after != before);
        Method isNotYet = AbstractClientTransport.ChannelFutureItem.class
                .getDeclaredMethod("isNotYetConnect");
        isNotYet.setAccessible(true);
        assertTrue("invalidated slot must report isNotYetConnect=true",
                (boolean) isNotYet.invoke(after));
        // And the live channel must have been told to close so the EventLoop tears down
        // the underlying socket asynchronously.
        assertTrue("invalidated channel must have close() invoked", live.closeCalled);
    }

    /**
     * Channel stub used for {@link #testInvalidateChannelReplacesSlotWithBlankPlaceholder}.
     * Tracks whether {@code close()} has been invoked.
     */
    private static class ConnectedChannel implements Channel {

        volatile boolean closeCalled = false;

        @Override
        public boolean isConnected() {
            return !closeCalled;
        }

        @Override
        public boolean isClosed() {
            return closeCalled;
        }

        @Override
        public java.net.InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public java.net.InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public com.tencent.trpc.core.common.config.ProtocolConfig getProtocolConfig() {
            return null;
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> send(Object message) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> close() {
            closeCalled = true;
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * AbstractClientTransport subclass used by {@link #testEnsureChannelActiveDoesNotStorm}.
     * Exposes a deterministic {@code make()} that records how many times it has been called
     * and returns a never-completing future so connecting state is observable.
     */
    private static class StormTransport extends AbstractClientTransport {

        final AtomicInteger makeCount = new AtomicInteger(0);
        volatile CompletableFuture<Channel> lastBuilt;

        StormTransport(ProtocolConfig config, ChannelHandler handler, ClientCodec codec) {
            super(config, handler, codec);
        }

        void installSlot(ChannelFutureItem item) {
            // The channels list is initialized empty; pad to index 0.
            channels.add(item);
        }

        ChannelFutureItem peekSlot(int idx) {
            return channels.get(idx);
        }

        @Override
        public Set<Channel> getChannels() {
            return null;
        }

        @Override
        protected void doOpen() {
        }

        @Override
        protected CompletableFuture<Channel> make() {
            makeCount.incrementAndGet();
            CompletableFuture<Channel> f = new CompletableFuture<>();
            lastBuilt = f;
            // Never complete: leaves the new slot in "isConnecting" state, which is exactly
            // what the in-lock double-check must short-circuit on for late-arriving threads.
            return f;
        }

        @Override
        protected void doClose() {
        }

        @Override
        protected boolean useChannelPool() {
            return true;
        }
    }

    /**
     * Channel that is "done but disconnected" — i.e. the previous future has resolved but
     * {@code isConnected()} is false, which is what {@code ensureChannelActive} should treat
     * as needing a reconnect.
     */
    private static class DisconnectedChannel implements Channel {

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public java.net.InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public java.net.InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public com.tencent.trpc.core.common.config.ProtocolConfig getProtocolConfig() {
            return null;
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> send(Object message) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public java.util.concurrent.CompletionStage<Void> close() {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class ClientTransportTest extends AbstractClientTransport {

        private boolean isTransportException;

        ClientTransportTest(ProtocolConfig config, ChannelHandler channelHandler,
                ClientCodec clientCodec, boolean isTransportException) throws TransportException {
            super(config, channelHandler, clientCodec);
            this.isTransportException = isTransportException;
        }

        @Override
        public Set<Channel> getChannels() {
            return null;
        }

        @Override
        protected void doOpen() {
            if (isTransportException) {
                throw new TransportException("");
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        protected CompletableFuture<Channel> make() throws Exception {
            return null;
        }

        @Override
        protected void doClose() {
            throw new IllegalArgumentException();
        }

        @Override
        protected boolean useChannelPool() {
            return false;
        }

    }
}
