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
import com.tencent.trpc.core.exception.ErrorCode;
import com.tencent.trpc.core.exception.TRpcException;
import com.tencent.trpc.core.logger.Logger;
import com.tencent.trpc.core.logger.LoggerFactory;
import com.tencent.trpc.core.rpc.AbstractRpcClient;
import com.tencent.trpc.core.rpc.ConsumerInvoker;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

/**
 * HTTP/2 cleartext (h2c) protocol client.
 *
 * <p><b>Long-connection mode</b>. Built on Apache HttpClient 5.x async + HTTP/2 multiplexing —
 * a single TCP connection carries many concurrent RPC streams. The connection manager is
 * tuned identically in spirit to {@link HttpRpcClient}:</p>
 * <ul>
 *     <li>{@code maxConnTotal} / {@code maxConnPerRoute} sized from
 *         {@code protocolConfig.getMaxConns()} so the pool never silently caps at the tiny
 *         HttpClient defaults;</li>
 *     <li>{@code validateAfterInactivity}: {@value #VALIDATE_AFTER_INACTIVITY_MS}ms re-check
 *         on idle connections before reuse;</li>
 *     <li>{@code evictExpired} + {@code evictIdle}: daemon-thread cleanup at
 *         {@value #EVICT_IDLE_CONNECTIONS_SECONDS}s;</li>
 *     <li>{@code SO_KEEPALIVE} enabled on the IOReactor so the OS itself surfaces dead peers
 *         on platforms where it is configured (Linux ~2h default, far quicker with kernel
 *         tuning);</li>
 *     <li>{@code timeToLive}: hard ceiling at {@value #CONNECTION_TTL_MINUTES}min, recovers
 *         from backend IP rotation in bounded time.</li>
 * </ul>
 *
 * <p><b>Health signalling to the cluster manager</b> mirrors {@link HttpRpcClient}:
 * the client reports unavailable when (a) it has been idle &gt;
 * {@value #IDLE_UNAVAILABLE_THRESHOLD_MINUTES}min, or (b) it has accumulated &ge;
 * {@value #FAILURE_UNAVAILABLE_THRESHOLD} consecutive failures since the last success.</p>
 */
public class Http2cRpcClient extends AbstractRpcClient {

    private static final Logger logger = LoggerFactory.getLogger(Http2cRpcClient.class);

    private static final int VALIDATE_AFTER_INACTIVITY_MS = 2000;
    private static final long EVICT_IDLE_CONNECTIONS_SECONDS = 60L;
    private static final int CONNECTION_TTL_MINUTES = 10;

    /**
     * If this client has not been used by any RPC for longer than this window, the periodic
     * health observer in {@code RpcClusterClientManager} will treat it as unavailable and
     * eventually close &amp; evict it. The window is intentionally large so that any
     * actively-used client is never affected. See {@link HttpRpcClient} for the same mechanism
     * on the HTTP/1.1 path.
     */
    static final int IDLE_UNAVAILABLE_THRESHOLD_MINUTES = 10;
    private static final long IDLE_UNAVAILABLE_THRESHOLD_NANOS =
            TimeUnit.MINUTES.toNanos(IDLE_UNAVAILABLE_THRESHOLD_MINUTES);
    /**
     * Number of consecutive failed RPCs that flips this client to unavailable. Reset to 0 on
     * the next successful RPC.
     */
    static final int FAILURE_UNAVAILABLE_THRESHOLD = 50;

    /**
     * Asynchronous HTTP client
     */
    protected CloseableHttpAsyncClient httpAsyncClient;
    /**
     * Timestamp ({@link System#nanoTime()}) of the most recent RPC sent through this client.
     * Updated by {@link Http2ConsumerInvoker} on each request.
     */
    private volatile long lastUsedNanos = System.nanoTime();
    /**
     * Number of consecutive failed RPCs since the last success. See {@link HttpRpcClient}.
     */
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    public Http2cRpcClient(ProtocolConfig config) {
        setConfig(config);
    }

    /**
     * Configure and start the client. The pool is sized from {@code maxConns}; idle / expired
     * connections are reaped in the background; dead-peer detection happens via SO_KEEPALIVE
     * plus a {@value #CONNECTION_TTL_MINUTES}-minute hard TTL.
     *
     * @throws TRpcException if the underlying HttpClient fails to start; surfacing this lets
     *         {@link AbstractRpcClient#open()} mark the lifecycle FAILED instead of leaving a
     *         half-built client cached.
     */
    @Override
    protected void doOpen() throws TRpcException {
        try {
            int maxConns = protocolConfig.getMaxConns();
            PoolingAsyncClientConnectionManager cm = PoolingAsyncClientConnectionManagerBuilder
                    .create()
                    .setMaxConnTotal(maxConns)
                    .setMaxConnPerRoute(maxConns)
                    .setConnPoolPolicy(PoolReusePolicy.LIFO)
                    .setValidateAfterInactivity(TimeValue.ofMilliseconds(VALIDATE_AFTER_INACTIVITY_MS))
                    .setConnectionTimeToLive(TimeValue.ofMinutes(CONNECTION_TTL_MINUTES))
                    .build();

            httpAsyncClient = HttpAsyncClients.custom()
                    .setConnectionManager(cm)
                    // Enable SO_KEEPALIVE on every socket so the OS eventually reaps dead peers
                    // even when no idle / TTL eviction has fired.
                    .setIOReactorConfig(IOReactorConfig.custom()
                            .setSoKeepAlive(true)
                            .setSoTimeout(Timeout.ofSeconds(0))
                            .build())
                    .evictExpiredConnections()
                    .evictIdleConnections(TimeValue.ofSeconds(EVICT_IDLE_CONNECTIONS_SECONDS))
                    .setVersionPolicy(org.apache.hc.core5.http2.HttpVersionPolicy.FORCE_HTTP_2)
                    .build();
            httpAsyncClient.start();
        } catch (Exception e) {
            // Surface the failure so the lifecycle moves to FAILED and the cached cluster slot
            // is not populated with a half-built client.
            String desc = protocolConfig != null ? protocolConfig.toSimpleString() : "<null>";
            throw TRpcException.newFrameException(ErrorCode.TRPC_CLIENT_CONNECT_ERR,
                    "open http2c client (" + desc + ") failed", e);
        }
    }

    /**
     * Close the client.
     */
    @Override
    protected void doClose() {
        if (httpAsyncClient != null) {
            try {
                httpAsyncClient.close();
            } catch (IOException e) {
                logger.error("close httpClient of " + protocolConfig.getIp() + ":"
                        + protocolConfig.getPort() + " failed", e);
            }
        }
    }

    /**
     * Generate an invoker and hand it over to the proxy to generate a proxy object.
     * The chain processing of the invoker is wrapped outside.
     *
     * @param consumerConfig the configuration related to the interface set by the method invoker,
     * such as timeout duration, filter configuration, etc.
     */
    @Override
    public <T> ConsumerInvoker<T> createInvoker(ConsumerConfig<T> consumerConfig) {
        return new Http2ConsumerInvoker<>(this, consumerConfig, protocolConfig);
    }

    /**
     * Record that this client just served (or is about to serve) an RPC. Called by
     * {@link Http2ConsumerInvoker} on every request entry.
     */
    public void markUsed() {
        lastUsedNanos = System.nanoTime();
    }

    /**
     * Record a successful RPC. Resets the consecutive-failure counter so an isolated earlier
     * failure does not contribute to eviction.
     */
    public void markSuccess() {
        consecutiveFailures.set(0);
    }

    /**
     * Record a failed RPC (exception during {@code execute} or non-2xx response).
     */
    public void markFailure() {
        consecutiveFailures.incrementAndGet();
    }

    /**
     * Reports the client as unavailable if its lifecycle is no longer started, or if it has
     * been idle longer than {@value #IDLE_UNAVAILABLE_THRESHOLD_MINUTES}min, or if at least
     * {@value #FAILURE_UNAVAILABLE_THRESHOLD} consecutive RPC failures have piled up since the
     * last success.
     */
    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        if (consecutiveFailures.get() >= FAILURE_UNAVAILABLE_THRESHOLD) {
            return false;
        }
        return (System.nanoTime() - lastUsedNanos) <= IDLE_UNAVAILABLE_THRESHOLD_NANOS;
    }

    public CloseableHttpAsyncClient getHttpAsyncClient() {
        return httpAsyncClient;
    }

    /**
     * Visible for tests / observability: current consecutive-failure counter snapshot.
     */
    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }
}
