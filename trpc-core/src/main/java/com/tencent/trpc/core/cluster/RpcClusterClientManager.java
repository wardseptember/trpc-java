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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.tencent.trpc.core.common.config.BackendConfig;
import com.tencent.trpc.core.common.config.ConsumerConfig;
import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.exception.TRpcException;
import com.tencent.trpc.core.logger.Logger;
import com.tencent.trpc.core.logger.LoggerFactory;
import com.tencent.trpc.core.rpc.CloseFuture;
import com.tencent.trpc.core.rpc.ConsumerInvoker;
import com.tencent.trpc.core.rpc.Request;
import com.tencent.trpc.core.rpc.Response;
import com.tencent.trpc.core.rpc.RpcClient;
import com.tencent.trpc.core.worker.WorkerPoolManager;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to manage the list mapping of point-to-point clients generated through BackendConfig.
 * <p>
 * Long-connection mode:
 * <ul>
 *     <li>Clients are kept alive for the lifetime of the {@link BackendConfig}; no idle timeout
 *         scanner closes idle connections.</li>
 *     <li>A lightweight background timer periodically (every
 *         {@value #RECONNECT_CHECK_PERIOD_SECONDS}s) scans cached clients. If a client is found
 *         to be unavailable, its failure counter is incremented; once the counter reaches
 *         {@value #MAX_RECONNECT_FAILURES}, the client is closed and evicted, freeing memory and
 *         allowing the next request to rebuild a fresh long connection.</li>
 *     <li>When the underlying {@link RpcClient} closes itself (transport error or our own timer),
 *         the {@link RpcClient#closeFuture()} callback removes the cache entry.</li>
 *     <li>{@link #shutdownBackendConfig(BackendConfig)} / {@link #close()} still release clients
 *         explicitly.</li>
 * </ul>
 */
public class RpcClusterClientManager {

    private static final Logger logger = LoggerFactory.getLogger(RpcClusterClientManager.class);

    /**
     * Period of the reconnect-check timer in seconds.
     */
    private static final int RECONNECT_CHECK_PERIOD_SECONDS = 30;
    /**
     * Maximum number of consecutive reconnect-check failures tolerated before the client is
     * closed and evicted from the cache.
     */
    private static final int MAX_RECONNECT_FAILURES = 5;

    /**
     * Cluster map, {@code Map<BackendConfig, Map<String, RpcClientProxy>>}
     */
    private static final Map<BackendConfig, Map<String, RpcClientProxy>> CLUSTER_MAP = Maps.newConcurrentMap();
    /**
     * Is close flag
     */
    private static final AtomicBoolean CLOSED_FLAG = new AtomicBoolean(false);

    /**
     * Reconnect-check timer handle. Started lazily on first {@link #getOrCreateClient}.
     */
    private static volatile ScheduledFuture<?> reconnectCheckerFuture;

    /**
     * Shutdown a cluster.
     *
     * @param backendConfig the configuration for the backend
     */
    public static void shutdownBackendConfig(BackendConfig backendConfig) {
        Optional.ofNullable(CLUSTER_MAP.remove(backendConfig))
                .ifPresent(proxyMap -> proxyMap.forEach((k, v) -> {
                    try {
                        v.close();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Shutdown client:{} backendConfig:{} success", k,
                                    backendConfig.toSimpleString());
                        }
                    } catch (Exception ex) {
                        logger.error("Shutdown client:{} backendConfig:{},exception", k, backendConfig.toSimpleString(),
                                ex);
                    }
                }));
    }

    /**
     * Get RpcClient based on BackendConfig. If RpcClient does not exist, create a new one and cache it.
     * <p>The created client is a long-lived connection. To prevent memory leak, when the
     * underlying client is closed (by itself or by the reconnect-check timer below), its entry
     * in the cache is removed via the {@link RpcClient#closeFuture()} callback.</p>
     *
     * @param bConfig BackendConfig, configuration for the backend
     * @param pConfig ProtocolConfig, configuration for the protocol
     * @return RpcClient instance based on BackendConfig and ProtocolConfig
     */
    public static RpcClient getOrCreateClient(BackendConfig bConfig, ProtocolConfig pConfig) {
        Preconditions.checkNotNull(bConfig, "backendConfig can't not be null");
        ensureReconnectCheckerStarted();
        Map<String, RpcClientProxy> map = CLUSTER_MAP.computeIfAbsent(bConfig, k -> new ConcurrentHashMap<>());
        String uniqId = pConfig.toUniqId();
        RpcClientProxy rpcClientProxy = map.computeIfAbsent(uniqId,
                k -> {
                    RpcClientProxy proxy = createRpcClientProxy(pConfig);
                    // When the underlying rpcClient closes (transport error or reconnect-check
                    // eviction), remove it from the cache to avoid memory leak. The next call
                    // will rebuild a new long connection on demand.
                    proxy.closeFuture().whenComplete((r, e) -> {
                        Map<String, RpcClientProxy> clusterMap = CLUSTER_MAP.get(bConfig);
                        if (clusterMap != null) {
                            // Only remove if the cached proxy is still the same instance.
                            clusterMap.remove(k, proxy);
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("RpcClient closed, removed from cluster cache, backendConfig={}, client={}",
                                    bConfig.toSimpleString(), proxy.getProtocolConfig().toSimpleString());
                        }
                    });
                    return proxy;
                });
        return rpcClientProxy;
    }

    private static RpcClientProxy createRpcClientProxy(ProtocolConfig protocolConfig) {
        Preconditions.checkArgument(!CLOSED_FLAG.get(), "Closed, can't create client");
        RpcClientProxy createdClient = new RpcClientProxy(protocolConfig.createClient());
        boolean isSucceeded = false;
        try {
            createdClient.open();
            isSucceeded = true;
            return createdClient;
        } finally {
            if (!isSucceeded) {
                createdClient.close();
            }
        }
    }

    /**
     * Lazily start the reconnect-check timer on first usage. Idempotent and thread-safe.
     */
    private static void ensureReconnectCheckerStarted() {
        if (reconnectCheckerFuture != null || CLOSED_FLAG.get()) {
            return;
        }
        synchronized (RpcClusterClientManager.class) {
            if (reconnectCheckerFuture != null || CLOSED_FLAG.get()) {
                return;
            }
            try {
                reconnectCheckerFuture = WorkerPoolManager.getShareScheduler().scheduleAtFixedRate(
                        RpcClusterClientManager::checkAndReconnect,
                        RECONNECT_CHECK_PERIOD_SECONDS,
                        RECONNECT_CHECK_PERIOD_SECONDS,
                        TimeUnit.SECONDS);
            } catch (Throwable ex) {
                logger.warn("Start reconnect-check timer failed, will fall back to lazy reconnect "
                        + "on request path only", ex);
            }
        }
    }

    /**
     * Periodic check: walk every cached client; for each one
     * that is currently unavailable, increment its failure counter; once the counter reaches
     * {@link #MAX_RECONNECT_FAILURES} the client is closed (which triggers
     * {@code closeFuture} → cache eviction). Healthy clients have their counter reset.
     * <p>The check itself does not actively send a heartbeat: it simply observes the current
     * connection state. The transport's existing lazy reconnect (triggered by request path or
     * by Netty's channelInactive event) takes care of re-establishing the long connection.</p>
     */
    static void checkAndReconnect() {
        if (CLOSED_FLAG.get()) {
            return;
        }
        CLUSTER_MAP.forEach((bConfig, clusterMap) -> clusterMap.forEach((key, proxy) -> {
            try {
                if (proxy.isAvailable()) {
                    proxy.failureCount.set(0);
                    return;
                }
                int fails = proxy.failureCount.incrementAndGet();
                if (logger.isDebugEnabled()) {
                    logger.debug("Reconnect-check: client {} not available, failureCount={}",
                            proxy.getProtocolConfig().toSimpleString(), fails);
                }
                if (fails >= MAX_RECONNECT_FAILURES) {
                    logger.warn("Reconnect-check: client {} unavailable for {} consecutive checks "
                                    + "(~{}s), closing and evicting from cache",
                            proxy.getProtocolConfig().toSimpleString(), fails,
                            fails * RECONNECT_CHECK_PERIOD_SECONDS);
                    try {
                        proxy.close();
                    } catch (Throwable ex) {
                        logger.error("Close stale client {} failed",
                                proxy.getProtocolConfig().toSimpleString(), ex);
                    }
                }
            } catch (Throwable ex) {
                logger.error("Reconnect-check on client {} threw", key, ex);
            }
        }));
    }

    /**
     * Close client
     */
    public static void close() {
        if (CLOSED_FLAG.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
            try {
                ScheduledFuture<?> f = reconnectCheckerFuture;
                if (f != null) {
                    f.cancel(true);
                    reconnectCheckerFuture = null;
                }
            } catch (Exception ex) {
                logger.error("Cancel reconnect-check timer failed", ex);
            }
            CLUSTER_MAP.forEach((config, clientProxyMap) -> clientProxyMap
                    .forEach((key, clientProxy) -> {
                        try {
                            clientProxy.close();
                        } catch (Exception ex) {
                            logger.error("Close clusterConfig{}, client {} exception:", config.toSimpleString(), key,
                                    ex);
                        }
                    }));
            CLUSTER_MAP.clear();
        }
    }

    public static synchronized void reset() {
        CLOSED_FLAG.set(false);
    }

    private static class ConsumerInvokerProxy<T> implements ConsumerInvoker<T> {

        private ConsumerInvoker<T> delegate;
        private RpcClientProxy rpcClient;

        ConsumerInvokerProxy(ConsumerInvoker<T> delegate, RpcClientProxy rpcClient) {
            super();
            this.delegate = delegate;
            this.rpcClient = rpcClient;
        }

        @Override
        public Class<T> getInterface() {
            return delegate.getInterface();
        }

        @Override
        public CompletionStage<Response> invoke(Request request) {
            return delegate.invoke(request);
        }

        @Override
        public ConsumerConfig<T> getConfig() {
            return delegate.getConfig();
        }

        @Override
        public ProtocolConfig getProtocolConfig() {
            return delegate.getProtocolConfig();
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ConsumerInvokerProxy other = (ConsumerInvokerProxy) obj;
            return Objects.equals(delegate, other.delegate);
        }
    }

    private static class RpcClientProxy implements RpcClient {

        private final RpcClient delegate;
        /**
         * Consecutive failure count observed by the reconnect-check timer; reset to 0 whenever
         * the client is observed available.
         */
        final AtomicInteger failureCount = new AtomicInteger(0);

        RpcClientProxy(RpcClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws TRpcException {
            delegate.open();
        }

        @Override
        public <T> ConsumerInvoker<T> createInvoker(ConsumerConfig<T> consumerConfig) {
            return new ConsumerInvokerProxy<T>(delegate.createInvoker(consumerConfig), this);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public CloseFuture<Void> closeFuture() {
            return delegate.closeFuture();
        }

        @Override
        public boolean isAvailable() {
            return delegate.isAvailable();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public ProtocolConfig getProtocolConfig() {
            return delegate.getProtocolConfig();
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RpcClientProxy other = (RpcClientProxy) obj;
            return Objects.equals(delegate, other.delegate);
        }
    }

}
