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

package com.tencent.trpc.core.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tencent.trpc.core.common.config.BackendConfig;
import com.tencent.trpc.core.common.config.ConsumerConfig;
import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.exception.TRpcException;
import com.tencent.trpc.core.rpc.CloseFuture;
import com.tencent.trpc.core.rpc.ConsumerInvoker;
import com.tencent.trpc.core.rpc.RpcClient;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RpcClusterClientManagerTest {

    @Before
    public void setUp() {
        // ensure clean state across tests (tests may have flipped CLOSED_FLAG)
        RpcClusterClientManager.reset();
    }

    @After
    public void tearDown() throws Exception {
        // Clear cluster cache to keep tests independent.
        Field field = RpcClusterClientManager.class.getDeclaredField("CLUSTER_MAP");
        field.setAccessible(true);
        ((Map<?, ?>) field.get(null)).clear();
        RpcClusterClientManager.reset();
    }

    @Test
    public void test() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException,
            SecurityException, InterruptedException {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setIdleTimeout(1);
        backendConfig.setNamingUrl("ip://127.0.0.1");
        ProtocolConfigTest config = new ProtocolConfigTest();
        RpcClient rpcClient = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        Assert.assertNotNull(rpcClient);
        Field field = RpcClusterClientManager.class.getDeclaredField("CLUSTER_MAP");
        field.setAccessible(true);
        Map<BackendConfig, Map> clusterMap = (Map<BackendConfig, Map>) field.get(null);
        assertEquals(1, clusterMap.get(backendConfig).size());
        // Long-connection mode: idle scanning is disabled, the client should still be cached after sleep.
        Thread.sleep(10);
        assertEquals(1, clusterMap.get(backendConfig).size());
        // Explicit shutdown should release the cached client.
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
        assertNull(clusterMap.get(backendConfig));
        BackendConfig backend = new BackendConfig();
        backend.setNamingUrl("ip://127.0.0.1:8081");
        RpcClusterClientManager.getOrCreateClient(backend, config);
        RpcClusterClientManager.shutdownBackendConfig(backend);
    }

    @Test
    public void testDebugLog() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setIdleTimeout(100000);
        backendConfig.setNamingUrl("ip://127.0.0.1:8082");
        ProtocolConfigTest config = new ProtocolConfigTest();
        RpcClient rpcClient = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        Assert.assertNotNull(rpcClient);
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    @Test
    public void testGetOrCreateClientTwice() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setIdleTimeout(100000);
        backendConfig.setNamingUrl("ip://127.0.0.1:8083");
        ProtocolConfigTest config = new ProtocolConfigTest();
        RpcClient rpcClient1 = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        RpcClient rpcClient2 = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        Assert.assertNotNull(rpcClient1);
        Assert.assertNotNull(rpcClient2);
        // Same key should return the same proxy instance (cache hit).
        Assert.assertSame(rpcClient1, rpcClient2);
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    @Test
    public void testClose() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setIdleTimeout(100000);
        backendConfig.setNamingUrl("ip://127.0.0.1:8084");
        ProtocolConfigTest config = new ProtocolConfigTest();
        RpcClient rpcClient = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        Assert.assertNotNull(rpcClient);
        RpcClusterClientManager.close();
        // close() is idempotent, second call should be a no-op.
        RpcClusterClientManager.close();
        RpcClusterClientManager.reset();
    }

    @Test
    public void testShutdownNonExistBackend() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9999");
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    @Test
    public void testScanWithEmptyCluster() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9998");
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /**
     * Triggers shutdownBackendConfig's catch branch: a client whose close() throws.
     */
    @Test
    public void testShutdownBackendConfigWhenCloseThrows() throws Exception {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9001");
        ProtocolConfigTest config = new ProtocolConfigTest();
        config.failOnClose = true;
        RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        // Should swallow exception and complete normally.
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
        Field field = RpcClusterClientManager.class.getDeclaredField("CLUSTER_MAP");
        field.setAccessible(true);
        Map<?, ?> map = (Map<?, ?>) field.get(null);
        assertNull(map.get(backendConfig));
    }

    /**
     * Triggers close()'s catch branch when the client throws on close.
     */
    @Test
    public void testCloseWhenClientCloseThrows() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9002");
        ProtocolConfigTest config = new ProtocolConfigTest();
        config.failOnClose = true;
        RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        // Should not propagate the exception out.
        RpcClusterClientManager.close();
        RpcClusterClientManager.reset();
    }

    /**
     * createRpcClientProxy: when open() throws, the partially-created proxy should be closed
     * to avoid resource leak, and the exception should propagate.
     */
    @Test
    public void testCreateClientWhenOpenThrows() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9003");
        ProtocolConfigTest config = new ProtocolConfigTest();
        config.failOnOpen = true;
        try {
            RpcClusterClientManager.getOrCreateClient(backendConfig, config);
            Assert.fail("expected exception");
        } catch (RuntimeException expected) {
            // expected
        }
    }

    /**
     * After CLOSED_FLAG is set, getOrCreateClient must reject new client creation.
     */
    @Test
    public void testGetOrCreateClientAfterClose() {
        RpcClusterClientManager.close();
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9004");
        ProtocolConfigTest config = new ProtocolConfigTest();
        try {
            RpcClusterClientManager.getOrCreateClient(backendConfig, config);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // expected
        } finally {
            RpcClusterClientManager.reset();
        }
    }

    /**
     * Direct invocation of observeHealth: client is healthy, failureCount stays at 0.
     */
    @Test
    public void testObserveHealthHealthyClient() throws Exception {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9005");
        ProtocolConfigTest config = new ProtocolConfigTest();
        final RpcClient client = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        invokeObserveHealth();
        // Healthy client must not be evicted.
        Field field = RpcClusterClientManager.class.getDeclaredField("CLUSTER_MAP");
        field.setAccessible(true);
        Map<BackendConfig, Map> clusterMap = (Map<BackendConfig, Map>) field.get(null);
        assertEquals(1, clusterMap.get(backendConfig).size());
        assertEquals(0, getFailureCount(client));
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /**
     * Direct invocation: client unavailable. failureCount accumulates across runs but the
     * observer must <b>not</b> close the underlying transport — closing would tear down the
     * shared Netty EventLoopGroup and abort in-flight long-connection requests. The transport's
     * lazy reconnect on the request path is the recovery mechanism.
     */
    @Test
    public void testObserveHealthUnavailableDoesNotEvict() throws Exception {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9006");
        ProtocolConfigTest config = new ProtocolConfigTest();
        config.available = false;
        final RpcClient client = RpcClusterClientManager.getOrCreateClient(backendConfig, config);

        Field field = RpcClusterClientManager.class.getDeclaredField("CLUSTER_MAP");
        field.setAccessible(true);
        Map<BackendConfig, Map> clusterMap = (Map<BackendConfig, Map>) field.get(null);
        assertEquals(1, clusterMap.get(backendConfig).size());

        // Run well past STUCK_UNAVAILABLE_THRESHOLD. The proxy must NOT be closed and must
        // remain in the cluster cache so long-connection traffic / lazy reconnect can resume.
        for (int i = 0; i < 8; i++) {
            invokeObserveHealth();
        }
        // failureCount is capped at STUCK_UNAVAILABLE_THRESHOLD (5) to avoid unbounded growth.
        assertEquals(5, getFailureCount(client));
        assertFalse("transport must not be closed by the observer", config.closed.get());
        assertEquals("client must remain cached for lazy reconnect",
                1, clusterMap.get(backendConfig).size());

        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /**
     * observeHealth must early-return when CLOSED_FLAG is true.
     */
    @Test
    public void testObserveHealthShortCircuitsOnClosed() throws Exception {
        RpcClusterClientManager.close();
        // Should not throw.
        invokeObserveHealth();
        RpcClusterClientManager.reset();
    }

    /**
     * The observer's per-proxy try/catch must keep the loop alive even when the underlying
     * proxy/state interactions throw. Since the observer no longer actively closes the
     * transport, this test simply verifies the observer does not propagate exceptions and
     * that failureCount keeps accumulating across iterations.
     */
    @Test
    public void testObserveHealthSwallowsCloseException() throws Exception {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9007");
        ProtocolConfigTest config = new ProtocolConfigTest();
        config.available = false;
        config.failOnClose = true;
        RpcClient client = RpcClusterClientManager.getOrCreateClient(backendConfig, config);

        for (int i = 0; i < 5; i++) {
            invokeObserveHealth();
        }
        // Must NOT throw out of the timer loop. Failure count should reach >= cap (5).
        assertTrue(getFailureCount(client) >= 5);
        // Observer must NOT have closed the transport.
        assertFalse(config.closed.get());
        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /**
     * Exercises the RpcClientProxy delegate methods: open / createInvoker / closeFuture /
     * isClosed / isAvailable / getProtocolConfig / equals / hashCode.
     */
    @Test
    public void testRpcClientProxyDelegation() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9008");
        ProtocolConfigTest config = new ProtocolConfigTest();
        config.invokerSupplier = () -> new StubConsumerInvoker();
        RpcClient proxy = RpcClusterClientManager.getOrCreateClient(backendConfig, config);

        assertTrue(proxy.isAvailable());
        assertFalse(proxy.isClosed());
        assertNotNull(proxy.closeFuture());
        assertNotNull(proxy.getProtocolConfig());
        // createInvoker delegates and wraps with ConsumerInvokerProxy.
        ConsumerConfig<Object> cc = new ConsumerConfig<>();
        ConsumerInvoker<Object> invoker = proxy.createInvoker(cc);
        // The wrapped invoker delegates getInterface / getConfig / getProtocolConfig / invoke.
        assertNotNull(invoker.getInterface());
        assertNotNull(invoker.getConfig());
        assertNotNull(invoker.getProtocolConfig());
        assertNotNull(invoker.invoke(null));

        // getOrCreateClient with the same key must return the cached proxy (same ref).
        RpcClient sameKey = RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        Assert.assertSame(proxy, sameKey);
        Assert.assertEquals(proxy.hashCode(), sameKey.hashCode());
        Assert.assertEquals(proxy, sameKey);
        Assert.assertNotEquals(proxy, null);
        Assert.assertNotEquals(proxy, "string");

        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /**
     * Exercises ConsumerInvokerProxy.equals/hashCode through the client-created invoker chain.
     */
    @Test
    public void testConsumerInvokerProxyEquality() {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9009");
        ProtocolConfigTest config = new ProtocolConfigTest();
        // Always wrap the SAME delegate so the two outer ConsumerInvokerProxy instances are equal.
        StubConsumerInvoker shared = new StubConsumerInvoker();
        config.invokerSupplier = () -> shared;
        RpcClient proxy = RpcClusterClientManager.getOrCreateClient(backendConfig, config);

        ConsumerConfig<Object> cc = new ConsumerConfig<>();
        ConsumerInvoker<Object> a = proxy.createInvoker(cc);
        ConsumerInvoker<Object> b = proxy.createInvoker(cc);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertEquals(a, a);
        Assert.assertNotEquals(a, null);
        Assert.assertNotEquals(a, "string");

        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /**
     * Stub ConsumerInvoker for delegation/equality tests.
     */
    private static class StubConsumerInvoker implements ConsumerInvoker<Object> {

        @Override
        public Class<Object> getInterface() {
            return Object.class;
        }

        @Override
        public java.util.concurrent.CompletionStage<com.tencent.trpc.core.rpc.Response> invoke(
                com.tencent.trpc.core.rpc.Request request) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        @Override
        public ConsumerConfig<Object> getConfig() {
            return new ConsumerConfig<>();
        }

        @Override
        public ProtocolConfig getProtocolConfig() {
            return new ProtocolConfig();
        }
    }

    /**
     * Lazy timer start: the first getOrCreateClient triggers ensureHealthObserverStarted; the
     * future field becomes non-null. Calling getOrCreateClient again must NOT replace it.
     */
    @Test
    public void testHealthObserverStartedLazilyAndOnce() throws Exception {
        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9010");
        ProtocolConfigTest config = new ProtocolConfigTest();
        RpcClusterClientManager.getOrCreateClient(backendConfig, config);

        Field f = RpcClusterClientManager.class.getDeclaredField("healthObserverFuture");
        f.setAccessible(true);
        Object first = f.get(null);
        assertNotNull("timer should be started", first);

        // Second call must not replace it.
        RpcClusterClientManager.getOrCreateClient(backendConfig, config);
        Object second = f.get(null);
        Assert.assertSame(first, second);

        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /* ---------------------- helpers ---------------------- */

    private static void invokeObserveHealth() throws Exception {
        Method m = RpcClusterClientManager.class.getDeclaredMethod("observeHealth");
        m.setAccessible(true);
        m.invoke(null);
    }

    private static int getFailureCount(RpcClient proxy) throws Exception {
        Field f = proxy.getClass().getDeclaredField("failureCount");
        f.setAccessible(true);
        return ((java.util.concurrent.atomic.AtomicInteger) f.get(proxy)).get();
    }

    /* ---------------------- mock ProtocolConfig ---------------------- */

    private static class ProtocolConfigTest extends ProtocolConfig {

        boolean available = true;
        boolean failOnOpen = false;
        boolean failOnClose = false;
        final AtomicBoolean closed = new AtomicBoolean(false);
        java.util.function.Supplier<ConsumerInvoker<?>> invokerSupplier;

        @Override
        public RpcClient createClient() {
            return new RpcClient() {

                private final CloseFuture<Void> closeFuture = new CloseFuture<>();

                @Override
                public void open() throws TRpcException {
                    if (failOnOpen) {
                        throw new RuntimeException("boom-open");
                    }
                }

                @Override
                public boolean isClosed() {
                    return closed.get();
                }

                @Override
                public boolean isAvailable() {
                    return available && !closed.get();
                }

                @Override
                public ProtocolConfig getProtocolConfig() {
                    return ProtocolConfigTest.this;
                }

                @Override
                public void close() {
                    closed.set(true);
                    if (failOnClose) {
                        // Still complete the future first so cache eviction proceeds.
                        closeFuture.complete(null);
                        throw new RuntimeException("boom-close");
                    }
                    closeFuture.complete(null);
                }

                @SuppressWarnings({"unchecked", "rawtypes"})
                @Override
                public <T> ConsumerInvoker<T> createInvoker(ConsumerConfig<T> consumerConfig) {
                    if (invokerSupplier != null) {
                        return (ConsumerInvoker<T>) invokerSupplier.get();
                    }
                    return null;
                }

                @Override
                public CloseFuture<Void> closeFuture() {
                    return closeFuture;
                }
            };
        }
    }
}
