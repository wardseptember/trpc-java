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

package com.tencent.trpc.proto.http;

import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.exception.TRpcException;
import com.tencent.trpc.proto.http.client.Http2RpcClient;
import com.tencent.trpc.proto.http.client.Http2cRpcClient;
import com.tencent.trpc.proto.http.client.HttpRpcClient;
import com.tencent.trpc.proto.http.client.HttpsRpcClient;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Pure unit tests for the long-connection extensions on the HTTP RpcClient hierarchy:
 * {@code markUsed}, the overridden {@code isAvailable}, and the idle-threshold heuristic.
 *
 * <p>No Jetty server is started; we manipulate the internal {@code lastUsedNanos} field via
 * reflection to drive the idle/active branches.</p>
 */
public class HttpRpcClientLongLinkTest {

    private static final long ELEVEN_MINUTES_NANOS = java.util.concurrent.TimeUnit.MINUTES.toNanos(11);

    /**
     * HttpRpcClient.isAvailable returns false when not started (lifecycle.isStarted() == false),
     * regardless of lastUsedNanos.
     */
    @Test
    public void testHttpRpcClientNotAvailableBeforeOpen() {
        ProtocolConfig pc = newProtocolConfig();
        HttpRpcClient client = new HttpRpcClient(pc);
        // Lifecycle has not been started, so super.isAvailable() returns false.
        Assert.assertFalse(client.isAvailable());
    }

    /**
     * HttpRpcClient.isAvailable returns true when started AND recently used.
     */
    @Test
    public void testHttpRpcClientAvailableWhenStartedAndFresh() {
        ProtocolConfig pc = newProtocolConfig();
        HttpRpcClient client = new HttpRpcClient(pc);
        client.open();
        try {
            client.markUsed(); // refresh
            Assert.assertTrue(client.isAvailable());
        } finally {
            client.close();
        }
    }

    /**
     * HttpRpcClient.isAvailable returns false when started but idle > 10 min.
     */
    @Test
    public void testHttpRpcClientNotAvailableWhenIdleTooLong() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        HttpRpcClient client = new HttpRpcClient(pc);
        client.open();
        try {
            // Force lastUsedNanos to "11 minutes ago".
            setField(client, "lastUsedNanos", System.nanoTime() - ELEVEN_MINUTES_NANOS);
            Assert.assertFalse(client.isAvailable());

            // Recovering via markUsed restores availability.
            client.markUsed();
            Assert.assertTrue(client.isAvailable());
        } finally {
            client.close();
        }
    }

    /**
     * Http2cRpcClient mirrors the same logic.
     */
    @Test
    public void testHttp2cRpcClientNotAvailableBeforeOpen() {
        ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        Assert.assertFalse(client.isAvailable());
    }

    @Test
    public void testHttp2cRpcClientAvailableWhenStartedAndFresh() {
        ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        client.open();
        try {
            client.markUsed();
            Assert.assertTrue(client.isAvailable());
        } finally {
            client.close();
        }
    }

    @Test
    public void testHttp2cRpcClientNotAvailableWhenIdleTooLong() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        client.open();
        try {
            setField(client, "lastUsedNanos", System.nanoTime() - ELEVEN_MINUTES_NANOS);
            Assert.assertFalse(client.isAvailable());
            client.markUsed();
            Assert.assertTrue(client.isAvailable());
        } finally {
            client.close();
        }
    }

    /**
     * doOpen wires the underlying httpClient. close() must release it without throwing.
     */
    @Test
    public void testHttpRpcClientOpenCloseReleasesResources() {
        ProtocolConfig pc = newProtocolConfig();
        HttpRpcClient client = new HttpRpcClient(pc);
        client.open();
        Assert.assertNotNull(client.getHttpClient());
        client.close();
        Assert.assertTrue(client.isClosed());
        // Idempotent close is safe.
        client.close();
    }

    /**
     * doOpen for Http2cRpcClient creates an httpAsyncClient and starts it.
     */
    @Test
    public void testHttp2cRpcClientOpenCloseReleasesResources() {
        ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        client.open();
        Assert.assertNotNull(client.getHttpAsyncClient());
        client.close();
        Assert.assertTrue(client.isClosed());
        client.close();
    }

    /**
     * Http2RpcClient inherits markUsed/isAvailable from Http2cRpcClient. We don't need a real
     * TLS context — we only verify the inherited methods behave the same on the subclass.
     * Use reflection to flip lifecycle state to STARTED so that super.isAvailable() returns true.
     */
    @Test
    public void testHttp2RpcClientInheritsIdleHeuristic() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        Http2RpcClient client = new Http2RpcClient(pc);
        forceLifecycleStarted(client);
        try {
            client.markUsed();
            Assert.assertTrue(client.isAvailable());
            setField(client, "lastUsedNanos", System.nanoTime() - ELEVEN_MINUTES_NANOS);
            Assert.assertFalse(client.isAvailable());
        } finally {
            forceLifecycleClosed(client);
        }
    }

    /**
     * HttpsRpcClient extends Http2RpcClient. Same inheritance check.
     */
    @Test
    public void testHttpsRpcClientInheritsIdleHeuristic() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        HttpsRpcClient client = new HttpsRpcClient(pc);
        forceLifecycleStarted(client);
        try {
            client.markUsed();
            Assert.assertTrue(client.isAvailable());
            setField(client, "lastUsedNanos", System.nanoTime() - ELEVEN_MINUTES_NANOS);
            Assert.assertFalse(client.isAvailable());
        } finally {
            forceLifecycleClosed(client);
        }
    }

    /**
     * Sustained backend failure: consecutive markFailure() crossing FAILURE_UNAVAILABLE_THRESHOLD
     * flips isAvailable() to false even while lastUsedNanos is fresh.
     */
    @Test
    public void testHttpRpcClientUnavailableOnConsecutiveFailures() {
        final ProtocolConfig pc = newProtocolConfig();
        HttpRpcClient client = new HttpRpcClient(pc);
        client.open();
        try {
            client.markUsed();
            for (int i = 0; i < 49; i++) {
                client.markFailure();
            }
            // 49 < threshold (50): still available.
            Assert.assertTrue(client.isAvailable());
            client.markFailure(); // 50 — flip
            Assert.assertFalse(client.isAvailable());
            // markSuccess resets counter — recovers immediately.
            client.markSuccess();
            Assert.assertTrue(client.isAvailable());
            Assert.assertEquals(0, client.getConsecutiveFailures());
        } finally {
            client.close();
        }
    }

    /**
     * Same eviction-on-consecutive-failure semantics on the H2 path.
     */
    @Test
    public void testHttp2cRpcClientUnavailableOnConsecutiveFailures() {
        final ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        client.open();
        try {
            client.markUsed();
            for (int i = 0; i < 49; i++) {
                client.markFailure();
            }
            Assert.assertTrue(client.isAvailable());
            client.markFailure();
            Assert.assertFalse(client.isAvailable());
            client.markSuccess();
            Assert.assertTrue(client.isAvailable());
            Assert.assertEquals(0, client.getConsecutiveFailures());
        } finally {
            client.close();
        }
    }

    /**
     * Concurrent markFailure / markSuccess from many business threads must converge to a
     * deterministic terminal state — the AtomicInteger contract guarantees no lost updates.
     */
    @Test
    public void testHttpRpcClientConsecutiveFailureCounterIsThreadSafe() throws Exception {
        final ProtocolConfig pc = newProtocolConfig();
        final HttpRpcClient client = new HttpRpcClient(pc);
        client.open();
        try {
            final int threads = 32;
            final int incrementsPerThread = 1000;
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                new Thread(() -> {
                    try {
                        start.await();
                        for (int j = 0; j < incrementsPerThread; j++) {
                            client.markFailure();
                        }
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            start.countDown();
            Assert.assertTrue(done.await(10, java.util.concurrent.TimeUnit.SECONDS));
            Assert.assertEquals(threads * incrementsPerThread, client.getConsecutiveFailures());
            client.markSuccess();
            Assert.assertEquals(0, client.getConsecutiveFailures());
        } finally {
            client.close();
        }
    }

    /**
     * KeepAliveStrategy: server returned no Keep-Alive header — fall back to the 5min ceiling.
     */
    @Test
    public void testResolveKeepAliveDurationNoHeader() {
        HttpResponse rsp = Mockito.mock(HttpResponse.class);
        Mockito.when(rsp.getFirstHeader("Keep-Alive")).thenReturn(null);
        long expected = TimeUnit.MINUTES.toMillis(5);
        Assert.assertEquals(expected, HttpRpcClient.resolveKeepAliveDuration(rsp, null));
    }

    /**
     * KeepAliveStrategy: server returned {@code Keep-Alive: timeout=120} — use server hint
     * because it's smaller than the fallback ceiling.
     */
    @Test
    public void testResolveKeepAliveDurationServerHintSmaller() {
        HttpResponse rsp = mockKeepAliveHeader("timeout", "120");
        // 120s = 120_000ms, fallback = 5min = 300_000ms → use 120_000.
        Assert.assertEquals(120_000L, HttpRpcClient.resolveKeepAliveDuration(rsp, null));
    }

    /**
     * KeepAliveStrategy: server returned {@code Keep-Alive: timeout=3600} — clamp at the
     * 5min fallback ceiling.
     */
    @Test
    public void testResolveKeepAliveDurationServerHintLargerClamped() {
        HttpResponse rsp = mockKeepAliveHeader("timeout", "3600");
        long expected = TimeUnit.MINUTES.toMillis(5);
        Assert.assertEquals(expected, HttpRpcClient.resolveKeepAliveDuration(rsp, null));
    }

    /**
     * KeepAliveStrategy: server sent {@code Keep-Alive: timeout=abc} — NumberFormatException
     * is swallowed and the fallback ceiling is returned.
     */
    @Test
    public void testResolveKeepAliveDurationMalformedTimeoutFallsBack() {
        HttpResponse rsp = mockKeepAliveHeader("timeout", "abc");
        long expected = TimeUnit.MINUTES.toMillis(5);
        Assert.assertEquals(expected, HttpRpcClient.resolveKeepAliveDuration(rsp, null));
    }

    /**
     * KeepAliveStrategy: server sent {@code Keep-Alive: max=100} (no timeout key) — fallback.
     */
    @Test
    public void testResolveKeepAliveDurationOtherKeyOnly() {
        HttpResponse rsp = mockKeepAliveHeader("max", "100");
        long expected = TimeUnit.MINUTES.toMillis(5);
        Assert.assertEquals(expected, HttpRpcClient.resolveKeepAliveDuration(rsp, null));
    }

    /**
     * doClose must swallow IOException from {@code httpClient.close()} (logged, not propagated).
     * Drives the catch branch in HttpRpcClient.doClose.
     */
    @Test
    public void testHttpRpcClientDoCloseSwallowsIoException() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        HttpRpcClient client = new HttpRpcClient(pc);
        client.open();
        // Replace the live httpClient with a mock that throws IOException on close.
        CloseableHttpClient throwing = Mockito.mock(CloseableHttpClient.class);
        Mockito.doThrow(new IOException("boom")).when(throwing).close();
        setField(client, "httpClient", throwing);
        // Must not propagate.
        client.close();
        Assert.assertTrue(client.isClosed());
    }

    /**
     * Same coverage on the H2 path: doClose swallows IOException from the async client.
     */
    @Test
    public void testHttp2cRpcClientDoCloseSwallowsIoException() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        client.open();
        CloseableHttpAsyncClient throwing = Mockito.mock(CloseableHttpAsyncClient.class);
        Mockito.doThrow(new IOException("boom")).when(throwing).close();
        setField(client, "httpAsyncClient", throwing);
        client.close();
        Assert.assertTrue(client.isClosed());
    }

    /**
     * Http2c doOpen must surface initialisation failure as a TRpcException so the lifecycle
     * moves to FAILED instead of leaving a half-built client cached. Drive the catch branch
     * by reflectively invoking {@code doOpen()} with a null protocolConfig — the builder NPEs
     * on {@code maxConns}, the catch wraps it.
     */
    @Test
    public void testHttp2cRpcClientDoOpenSurfacesFailure() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        Http2cRpcClient client = new Http2cRpcClient(pc);
        // Null out protocolConfig so doOpen()'s protocolConfig.getMaxConns() NPEs inside the
        // try-block, exercising the catch → TRpcException branch.
        setField(client, "protocolConfig", null);
        try {
            // Direct doOpen reflection bypasses the lifecycle's pre-flight null check.
            java.lang.reflect.Method m = Http2cRpcClient.class.getDeclaredMethod("doOpen");
            m.setAccessible(true);
            try {
                m.invoke(client);
                Assert.fail("doOpen must surface failure");
            } catch (java.lang.reflect.InvocationTargetException ite) {
                Throwable cause = ite.getCause();
                Assert.assertTrue("expected TRpcException, got " + cause,
                        cause instanceof TRpcException);
            }
        } finally {
            // Restore so close() can run cleanly via lifecycle.
            setField(client, "protocolConfig", pc);
        }
    }

    /**
     * Http2/HTTPS doOpen surfaces failure when the keystore path is missing/invalid.
     */
    @Test
    public void testHttp2RpcClientDoOpenSurfacesFailure() throws Exception {
        ProtocolConfig pc = newProtocolConfig();
        // No keystore configured at all → SSLContexts.loadTrustMaterial throws.
        pc.getExtMap().put("keyStorePath", "/no/such/path/keystore.jks");
        pc.getExtMap().put("keyStorePass", "wrong");
        Http2RpcClient client = new Http2RpcClient(pc);
        java.lang.reflect.Method m = Http2RpcClient.class.getDeclaredMethod("doOpen");
        m.setAccessible(true);
        try {
            m.invoke(client);
            Assert.fail("doOpen must surface failure");
        } catch (java.lang.reflect.InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            Assert.assertTrue("expected TRpcException, got " + cause,
                    cause instanceof TRpcException);
        }
    }

    /* ---------------------- helpers ---------------------- */

    /**
     * Build a mocked {@link HttpResponse} carrying {@code Keep-Alive: <key>=<value>}.
     */
    private static HttpResponse mockKeepAliveHeader(String key, String value) {
        HttpResponse rsp = Mockito.mock(HttpResponse.class);
        Header h = Mockito.mock(Header.class);
        HeaderElement el = Mockito.mock(HeaderElement.class);
        Mockito.when(el.getName()).thenReturn(key);
        Mockito.when(el.getValue()).thenReturn(value);
        Mockito.when(h.getElements()).thenReturn(new HeaderElement[]{el});
        Mockito.when(rsp.getFirstHeader("Keep-Alive")).thenReturn(h);
        return rsp;
    }

    private static ProtocolConfig newProtocolConfig() {
        ProtocolConfig pc = new ProtocolConfig();
        pc.setIp("127.0.0.1");
        pc.setPort(0);
        pc.setProtocol("http");
        pc.setNetwork("tcp");
        pc.setDefault();
        return pc;
    }

    /**
     * Bypass real doOpen — flip the embedded LifecycleObj to STARTED so isAvailable's
     * super.isAvailable() check passes without spinning up actual HTTP infrastructure.
     */
    private static void forceLifecycleStarted(Object client) throws Exception {
        Field lf = findField(client.getClass(), "lifecycleObj");
        lf.setAccessible(true);
        Object lifecycle = lf.get(client);
        Field state = findField(lifecycle.getClass(), "state");
        state.setAccessible(true);
        // LifecycleState enum: STARTED ordinal lookup via reflection (avoid hard dependency).
        Class<?> stateEnum = state.getType();
        Object started = stateEnum.getMethod("valueOf", String.class).invoke(null, "STARTED");
        state.set(lifecycle, started);
    }

    private static void forceLifecycleClosed(Object client) throws Exception {
        Field lf = findField(client.getClass(), "lifecycleObj");
        lf.setAccessible(true);
        Object lifecycle = lf.get(client);
        Field state = findField(lifecycle.getClass(), "state");
        state.setAccessible(true);
        Class<?> stateEnum = state.getType();
        Object stopped = stateEnum.getMethod("valueOf", String.class).invoke(null, "STOPPED");
        state.set(lifecycle, stopped);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = findField(target.getClass(), fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Field findField(Class<?> clazz, String name) throws NoSuchFieldException {
        Class<?> c = clazz;
        while (c != null) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }
}
