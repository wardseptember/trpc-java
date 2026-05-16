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
import com.tencent.trpc.proto.http.client.Http2RpcClient;
import com.tencent.trpc.proto.http.client.Http2cRpcClient;
import com.tencent.trpc.proto.http.client.HttpRpcClient;
import com.tencent.trpc.proto.http.client.HttpsRpcClient;
import java.lang.reflect.Field;
import org.junit.Assert;
import org.junit.Test;

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

    /* ---------------------- helpers ---------------------- */

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
