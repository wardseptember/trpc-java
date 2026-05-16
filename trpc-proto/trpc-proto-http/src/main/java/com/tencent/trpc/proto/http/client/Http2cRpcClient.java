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
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;

/**
 * Http2c protocol client.
 */
public class Http2cRpcClient extends AbstractRpcClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpRpcClient.class);

    /**
     * If this client has not been used by any RPC for longer than this window, the periodic
     * scanner in {@code RpcClusterClientManager} will treat it as unavailable and eventually
     * close & evict it. The window is intentionally large so that any actively-used client is
     * never affected. See {@link HttpRpcClient} for the same mechanism on the HTTP/1.1 path.
     */
    private static final long IDLE_UNAVAILABLE_THRESHOLD_NANOS = TimeUnit.MINUTES.toNanos(10);

    /**
     * Asynchronous HTTP client
     */
    protected CloseableHttpAsyncClient httpAsyncClient;
    /**
     * Timestamp (System.nanoTime()) of the most recent RPC sent through this client. Updated by
     * {@link Http2ConsumerInvoker} on each request.
     */
    private volatile long lastUsedNanos = System.nanoTime();

    public Http2cRpcClient(ProtocolConfig config) {
        setConfig(config);
    }

    /**
     * Configure and start the client
     */
    @Override
    protected void doOpen() {
        httpAsyncClient = HttpAsyncClients.customHttp2().build();
        httpAsyncClient.start();
    }

    /**
     * Close the client
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
     * {@link Http2ConsumerInvoker} on every request.
     */
    public void markUsed() {
        lastUsedNanos = System.nanoTime();
    }

    /**
     * Reports the client as unavailable if it has been idle longer than
     * {@link #IDLE_UNAVAILABLE_THRESHOLD_NANOS}. This lets the cluster manager's periodic
     * reconnect-check timer eventually evict orphaned clients (e.g. after backend IP rotation).
     */
    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        return (System.nanoTime() - lastUsedNanos) <= IDLE_UNAVAILABLE_THRESHOLD_NANOS;
    }

    public CloseableHttpAsyncClient getHttpAsyncClient() {
        return httpAsyncClient;
    }
}
