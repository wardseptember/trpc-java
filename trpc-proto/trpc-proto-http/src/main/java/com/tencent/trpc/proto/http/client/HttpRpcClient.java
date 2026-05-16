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

package com.tencent.trpc.proto.http.client;

import com.tencent.trpc.core.common.config.ConsumerConfig;
import com.tencent.trpc.core.common.config.ProtocolConfig;
import com.tencent.trpc.core.logger.Logger;
import com.tencent.trpc.core.logger.LoggerFactory;
import com.tencent.trpc.core.rpc.AbstractRpcClient;
import com.tencent.trpc.core.rpc.ConsumerInvoker;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * HTTP protocol client.
 * <p>Long-connection mode: connections are pooled by Apache {@link PoolingHttpClientConnectionManager}
 * and reused across requests via HTTP/1.1 keep-alive. Two safeguards are enabled by default to
 * keep the pool healthy in long-running processes:
 * <ul>
 *     <li>{@code validateAfterInactivity}: re-check a connection's liveness before reuse if it
 *         has been idle for a short period (avoids the classic "stale connection / NoHttpResponseException"
 *         when the server has half-closed an idle keep-alive connection);</li>
 *     <li>{@code evictIdleConnections}: a small background thread evicts connections that have
 *         been idle longer than the configured limit, freeing OS file descriptors.</li>
 * </ul>
 */
public class HttpRpcClient extends AbstractRpcClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpRpcClient.class);

    /**
     * Validate a pooled connection before reuse if it has been idle for at least this many
     * milliseconds. Cheap heuristic that catches most server-side half-closed keep-alive sockets.
     */
    private static final int VALIDATE_AFTER_INACTIVITY_MS = 2000;
    /**
     * Evict pooled connections that have been idle for longer than this duration.
     */
    private static final long EVICT_IDLE_CONNECTIONS_SECONDS = 60L;
    /**
     * If this client has not been used by any RPC for longer than this window, the periodic
     * scanner in {@code RpcClusterClientManager} will treat it as unavailable. After
     * a few consecutive unavailable observations the client gets closed and evicted from the
     * cluster cache, which is how we reclaim {@link HttpRpcClient} instances orphaned by backend
     * IP rotation (e.g. K8s pod IP drift). The window is intentionally large so that any
     * actively-used client is never affected.
     */
    private static final long IDLE_UNAVAILABLE_THRESHOLD_NANOS =
            java.util.concurrent.TimeUnit.MINUTES.toNanos(10);

    private CloseableHttpClient httpClient;
    /**
     * Timestamp (System.nanoTime()) of the most recent RPC sent through this client. Updated by
     * {@link HttpConsumerInvoker} on each send.
     */
    private volatile long lastUsedNanos = System.nanoTime();

    public HttpRpcClient(ProtocolConfig config) {
        setConfig(config);
    }

    @Override
    protected void doOpen() {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        int maxConns = protocolConfig.getMaxConns();
        // Set the maximum number of connections.
        cm.setMaxTotal(maxConns);
        // If there is only one route, the maximum number of connections for a single route is the same
        // as the maximum number of connections for the entire connection pool.
        cm.setDefaultMaxPerRoute(maxConns);
        // Re-validate idle pooled connections before reuse so we do not send a request through a
        // socket the server has already half-closed.
        cm.setValidateAfterInactivity(VALIDATE_AFTER_INACTIVITY_MS);
        httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                // Background eviction of stale & long-idle connections; keeps the pool tidy in
                // long-running processes without affecting hot connections.
                .evictExpiredConnections()
                .evictIdleConnections(EVICT_IDLE_CONNECTIONS_SECONDS, TimeUnit.SECONDS)
                .build();
    }

    @Override
    protected void doClose() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                logger.error("close httpClient of " + protocolConfig.getIp() + ":"
                        + protocolConfig.getPort() + " failed", e);
            }
        }
    }

    @Override
    public <T> ConsumerInvoker<T> createInvoker(ConsumerConfig<T> consumerConfig) {
        return new HttpConsumerInvoker<>(this, consumerConfig, protocolConfig);
    }

    /**
     * Record that this client just served (or is about to serve) an RPC. Called by
     * {@link HttpConsumerInvoker} on every request.
     */
    public void markUsed() {
        lastUsedNanos = System.nanoTime();
    }

    /**
     * Reports the client as unavailable if it has been idle longer than
     * {@link #IDLE_UNAVAILABLE_THRESHOLD_NANOS}. This lets the cluster manager's periodic
     * reconnect-check timer eventually evict orphaned clients (e.g. after backend IP rotation)
     * even though Apache HttpClient itself has no notion of "remote permanently gone".
     */
    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        return (System.nanoTime() - lastUsedNanos) <= IDLE_UNAVAILABLE_THRESHOLD_NANOS;
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }
}
