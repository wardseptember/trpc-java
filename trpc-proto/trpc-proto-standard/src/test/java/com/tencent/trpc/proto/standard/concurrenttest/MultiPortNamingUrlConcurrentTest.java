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

package com.tencent.trpc.proto.standard.concurrenttest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.tencent.trpc.core.common.ConfigManager;
import com.tencent.trpc.core.common.config.BackendConfig;
import com.tencent.trpc.core.common.config.ProviderConfig;
import com.tencent.trpc.core.common.config.ServiceConfig;
import com.tencent.trpc.core.rpc.RpcClientContext;
import com.tencent.trpc.core.rpc.RpcServerContext;
import com.tencent.trpc.proto.standard.common.HelloRequestProtocol.HelloRequest;
import com.tencent.trpc.proto.standard.common.HelloRequestProtocol.HelloResponse;
import com.tencent.trpc.proto.support.DefResponseFutureManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link BackendConfig#setNamingUrl(String)} configured with a comma-separated list
 * of {@code ip:port} entries fan-outs requests to all backends (random load balance) under heavy
 * concurrency.
 *
 * <p>Setup: 10 standalone tRPC servers on consecutive ports; one shared {@link BackendConfig}
 * whose namingUrl lists all 10 endpoints; 100 concurrent threads × 1000 requests each.</p>
 *
 * <p>Each server impl echoes back its own listening port so the client can group responses by the
 * actually-served port. Final assertions:
 * <ul>
 *     <li>every request succeeds with the exact echoed payload,</li>
 *     <li>all 10 backend ports get hit at least once (proving namingUrl actually distributes
 *         traffic, not pinning to one endpoint),</li>
 *     <li>distribution is roughly balanced — random selector over N=10 with R=100000 requests
 *         gives an expected 10000 per server; we tolerate {@code [2000, 20000]} per server which
 *         is a generous bound far above any realistic random outlier.</li>
 * </ul>
 */
public class MultiPortNamingUrlConcurrentTest {

    private static final int BASE_TCP_PORT = 12500;
    private static final int SERVER_COUNT = 10;
    private static final int THREAD_COUNT = 100;
    private static final int CYCLE_PER_THREAD = 1000;

    private final List<ServiceConfig> serviceConfigs = new ArrayList<>(SERVER_COUNT);

    @Before
    public void before() {
        ConfigManager.stopTest();
        ConfigManager.startTest();
        startServers();
    }

    @After
    public void stop() {
        for (ServiceConfig serviceConfig : serviceConfigs) {
            try {
                serviceConfig.unExport();
            } catch (Exception ignore) {
                // ignore
            }
        }
        serviceConfigs.clear();
        ConfigManager.stopTest();
    }

    @Test
    public void testMultiPortNamingUrlConcurrent() throws InterruptedException {
        // Build the comma-separated namingUrl: "ip://127.0.0.1:p1,127.0.0.1:p2,..."
        StringBuilder urlBuilder = new StringBuilder("ip://");
        for (int i = 0; i < SERVER_COUNT; i++) {
            if (i > 0) {
                urlBuilder.append(',');
            }
            urlBuilder.append("127.0.0.1:").append(BASE_TCP_PORT + i);
        }
        String namingUrl = urlBuilder.toString();

        BackendConfig backendConfig = new BackendConfig();
        DefResponseFutureManager.reset();
        backendConfig.setNamingUrl(namingUrl);
        // One long connection per backend addr is enough; keeps the test deterministic.
        backendConfig.setConnsPerAddr(5);
        backendConfig.setNetwork("tcp");
        // Generous client-side timeout so a slow JIT warm-up can't fail individual calls.
        backendConfig.setRequestTimeout(60_000);

        final ConcurrentTestServiceApi proxy = backendConfig.getProxy(ConcurrentTestServiceApi.class);

        // Per-port hit counter aggregated across all threads.
        ConcurrentHashMap<Integer, AtomicInteger> portHits = new ConcurrentHashMap<>();
        for (int i = 0; i < SERVER_COUNT; i++) {
            portHits.put(BASE_TCP_PORT + i, new AtomicInteger(0));
        }

        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        List<TestResult> results = new ArrayList<>(THREAD_COUNT);
        for (int t = 0; t < THREAD_COUNT; t++) {
            final TestResult r = new TestResult();
            results.add(r);
            final int threadIndex = t;
            new Thread(() -> {
                try {
                    for (int i = 0; i < CYCLE_PER_THREAD; i++) {
                        String reqPayload = "req-" + threadIndex + "-" + i;
                        RpcClientContext context = new RpcClientContext();
                        HelloResponse response = proxy.sayHello(context, HelloRequest.newBuilder()
                                .setMessage(ByteString.copyFromUtf8(reqPayload))
                                .build());
                        // Server impl returns "<echoedPayload>|port=<actualPort>"
                        String message = response.getMessage().toStringUtf8();
                        int sep = message.lastIndexOf("|port=");
                        assertTrue("response missing port marker: " + message, sep > 0);
                        String echoed = message.substring(0, sep);
                        int port = Integer.parseInt(message.substring(sep + "|port=".length()));
                        assertEquals("echoed payload must match request", reqPayload, echoed);
                        AtomicInteger counter = portHits.get(port);
                        assertTrue("response from unexpected port: " + port, counter != null);
                        counter.incrementAndGet();
                    }
                    r.succ = true;
                } catch (Throwable ex) {
                    r.succ = false;
                    r.ex = ex;
                    ex.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }, "concurrent-caller-" + t).start();
        }
        // 200s upper bound; full run on a laptop usually finishes in a few seconds.
        boolean done = latch.await(200, TimeUnit.SECONDS);
        assertTrue("concurrent calls timed out before completion", done);

        for (int i = 0; i < results.size(); i++) {
            TestResult r = results.get(i);
            assertTrue("worker thread " + i + " failed: "
                    + (r.ex == null ? "<no exception>" : r.ex.toString()), r.succ);
        }

        // ---- final aggregate assertions ----
        int totalRequests = THREAD_COUNT * CYCLE_PER_THREAD;
        int sum = 0;
        Set<Integer> hitPorts = new HashSet<>();
        for (int i = 0; i < SERVER_COUNT; i++) {
            int port = BASE_TCP_PORT + i;
            int hits = portHits.get(port).get();
            sum += hits;
            if (hits > 0) {
                hitPorts.add(port);
            }
            // Random over 10 with 100000 trials → expected 10000/server.
            // Lower bound 2000 / upper bound 20000 leaves >>3-sigma headroom; CI-safe.
            assertTrue("port " + port + " never received a request", hits > 0);
            assertTrue("port " + port + " too few hits: " + hits, hits >= 2000);
            assertTrue("port " + port + " too many hits: " + hits, hits <= 20000);
        }
        assertEquals("total responses should equal total requests", totalRequests, sum);
        assertEquals("all 10 backend ports must be hit", SERVER_COUNT, hitPorts.size());
    }

    private void startServers() {
        for (int i = 0; i < SERVER_COUNT; i++) {
            int port = BASE_TCP_PORT + i;
            ProviderConfig<ConcurrentTestService> providerConfig = new ProviderConfig<>();
            providerConfig.setRef(new PortAwareEchoServiceImpl(port));

            ServiceConfig serviceConfig = new ServiceConfig();
            serviceConfig.setIp("127.0.0.1");
            serviceConfig.setNetwork("tcp");
            serviceConfig.setPort(port);
            serviceConfig.setEnableLinkTimeout(true);
            // Generous server-side timeout to avoid spurious timeouts on slow CI.
            serviceConfig.setRequestTimeout(60_000);
            serviceConfig.addProviderConfig(providerConfig);
            serviceConfig.export();
            serviceConfigs.add(serviceConfig);
        }
    }

    /**
     * Service impl that tags every response with its own listening port so the test can verify
     * the actual server that handled each request.
     */
    private static class PortAwareEchoServiceImpl implements ConcurrentTestService {

        private final int port;

        PortAwareEchoServiceImpl(int port) {
            this.port = port;
        }

        @Override
        public HelloResponse sayHello(RpcServerContext context, HelloRequest request) {
            String echoed = request.getMessage().toStringUtf8();
            String tagged = echoed + "|port=" + port;
            return HelloResponse.newBuilder()
                    .setMessage(ByteString.copyFromUtf8(tagged))
                    .build();
        }
    }

    private static class TestResult {

        boolean succ;
        Throwable ex;
    }
}
