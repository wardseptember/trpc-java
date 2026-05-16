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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

import com.tencent.trpc.core.common.config.BackendConfig;
import com.tencent.trpc.core.common.config.ConsumerConfig;
import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.exception.TRpcException;
import com.tencent.trpc.core.rpc.CloseFuture;
import com.tencent.trpc.core.rpc.ConsumerInvoker;
import com.tencent.trpc.core.rpc.RpcClient;
import com.tencent.trpc.core.worker.WorkerPoolManager;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Drives the {@code catch (Throwable)} branch in
 * {@link RpcClusterClientManager#ensureReconnectCheckerStarted()}: when the shared scheduler
 * rejects the periodic task, the manager must swallow the exception and leave
 * {@code reconnectCheckerFuture} as {@code null}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerPoolManager.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.ws.*", "javax.net.*"})
public class RpcClusterSchedulerRejectTest {

    @Before
    public void setUp() throws Exception {
        RpcClusterClientManager.reset();
        clearReconnectCheckerFuture();
        clearClusterMap();
    }

    @After
    public void tearDown() throws Exception {
        clearReconnectCheckerFuture();
        clearClusterMap();
        RpcClusterClientManager.reset();
    }

    @Test
    public void testSchedulerExceptionCatchBranch() throws Exception {
        ScheduledExecutorService rejecting = Mockito.mock(ScheduledExecutorService.class);
        when(rejecting.scheduleAtFixedRate(Mockito.any(Runnable.class), Mockito.anyLong(),
                Mockito.anyLong(), Mockito.any())).thenThrow(new RejectedExecutionException("rejected"));

        PowerMockito.mockStatic(WorkerPoolManager.class);
        PowerMockito.when(WorkerPoolManager.getShareScheduler()).thenReturn(rejecting);

        BackendConfig backendConfig = new BackendConfig();
        backendConfig.setNamingUrl("ip://127.0.0.1:9101");
        // Must NOT propagate the RejectedExecutionException.
        RpcClient client = RpcClusterClientManager.getOrCreateClient(backendConfig,
                new StubProtocolConfig());
        assertNotNull(client);

        // Catch branch leaves reconnectCheckerFuture as null.
        Field f = RpcClusterClientManager.class.getDeclaredField("reconnectCheckerFuture");
        f.setAccessible(true);
        assertNull("scheduler rejected → future must remain null", f.get(null));

        RpcClusterClientManager.shutdownBackendConfig(backendConfig);
    }

    /* ---------------- helpers ---------------- */

    private static void clearClusterMap() throws Exception {
        Field f = RpcClusterClientManager.class.getDeclaredField("CLUSTER_MAP");
        f.setAccessible(true);
        ((Map<?, ?>) f.get(null)).clear();
    }

    private static void clearReconnectCheckerFuture() throws Exception {
        Field f = RpcClusterClientManager.class.getDeclaredField("reconnectCheckerFuture");
        f.setAccessible(true);
        Object cur = f.get(null);
        if (cur != null) {
            try {
                ((java.util.concurrent.ScheduledFuture<?>) cur).cancel(true);
            } catch (Throwable ignore) {
                // ignore
            }
            f.set(null, null);
        }
    }

    /* ---------------- stubs ---------------- */

    private static class StubProtocolConfig extends ProtocolConfig {

        volatile boolean available = true;
        final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public RpcClient createClient() {
            return new StubRpcClient(this);
        }
    }

    private static class StubRpcClient implements RpcClient {

        private final StubProtocolConfig owner;
        private final CloseFuture<Void> closeFuture = new CloseFuture<>();

        StubRpcClient(StubProtocolConfig owner) {
            this.owner = owner;
        }

        @Override
        public void open() throws TRpcException {
        }

        @Override
        public boolean isClosed() {
            return owner.closed.get();
        }

        @Override
        public boolean isAvailable() {
            return owner.available && !owner.closed.get();
        }

        @Override
        public ProtocolConfig getProtocolConfig() {
            return owner;
        }

        @Override
        public void close() {
            owner.closed.set(true);
            closeFuture.complete(null);
        }

        @Override
        public <T> ConsumerInvoker<T> createInvoker(ConsumerConfig<T> consumerConfig) {
            return null;
        }

        @Override
        public CloseFuture<Void> closeFuture() {
            return closeFuture;
        }
    }
}
