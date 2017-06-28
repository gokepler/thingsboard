/**
 * Copyright © 2016-2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.thingsboard.install.tools.db;

import com.datastax.driver.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.thingsboard.install.tools.ThingsboardEnvironment;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class CassandraDatabaseTool implements DatabaseTool {

    private static final String SCHEMA_CQL = "schema.cql";
    private static final String SYSTEM_DATA_CQL = "system_data.cql";
    private static final String DEMO_DATA_CQL = "demo_data.cql";


    private static final String COMMA = ",";
    private static final String COLON = ":";

    private final ThingsboardEnvironment environment;

    private String clusterName;
    private String keyspaceName;
    private String url;
    private String compression;
    private Boolean ssl;
    private Boolean jmx;
    private Boolean metrics;
    private Boolean credentials;
    private String username;
    private String password;
    private long initTimeout;
    private long initRetryInterval;
    private SocketOptions socketOptions;
    private QueryOptions queryOptions;

    private String readConsistencyLevel;
    private String writeConsistencyLevel;

    private ConsistencyLevel defaultReadConsistencyLevel;
    private ConsistencyLevel defaultWriteConsistencyLevel;

    private Cluster cluster;
    private Session session;

    public CassandraDatabaseTool(ThingsboardEnvironment environment) {
        this.environment = environment;

        this.clusterName = environment.getProperty("cassandra.cluster_name");
        this.keyspaceName = environment.getProperty("cassandra.keyspace_name");
        this.url = environment.getProperty("cassandra.url");
        this.compression = environment.getProperty("cassandra.compression");
        this.ssl = environment.getProperty("cassandra.ssl", Boolean.class);
        this.jmx = environment.getProperty("cassandra.jmx", Boolean.class);
        this.metrics = environment.getProperty("cassandra.metrics", Boolean.class);
        this.credentials = environment.getProperty("cassandra.credentials", Boolean.class);
        this.username = environment.getProperty("cassandra.username");
        this.password = environment.getProperty("cassandra.password");
        this.initTimeout = environment.getProperty("cassandra.init_timeout_ms", long.class);
        this.initRetryInterval = environment.getProperty("cassandra.init_retry_interval_ms", long.class);
        this.socketOptions = getSocketOptions(environment);
        this.queryOptions = getQueryOptions(environment);
        this.readConsistencyLevel = environment.getProperty("cassandra.query.read_consistency_level");
        this.writeConsistencyLevel = environment.getProperty("cassandra.query.write_consistency_level");

        initCluster();
    }

    @Override
    public void createDbSchema() {

    }

    @Override
    public void loadSystemData() {

    }

    @Override
    public void loadDemoData() {

    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private void initCluster() {
        long endTime = System.currentTimeMillis() + initTimeout;
        while (System.currentTimeMillis() < endTime) {
            try {
                Cluster.Builder builder = Cluster.builder()
                        .addContactPointsWithPorts(getContactPoints(url))
                        .withClusterName(clusterName)
                        .withSocketOptions(socketOptions)
                        .withPoolingOptions(new PoolingOptions()
                                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                                .setMaxRequestsPerConnection(HostDistance.REMOTE, 32768));
                builder.withQueryOptions(this.queryOptions);
                builder.withCompression(StringUtils.isEmpty(compression) ? ProtocolOptions.Compression.NONE : ProtocolOptions.Compression.valueOf(compression.toUpperCase()));
                if (ssl) {
                    builder.withSSL();
                }
                if (!jmx) {
                    builder.withoutJMXReporting();
                }
                if (!metrics) {
                    builder.withoutMetrics();
                }
                if (credentials) {
                    builder.withCredentials(username, password);
                }
                cluster = builder.build();
                cluster.init();
                session = cluster.connect(keyspaceName);
                break;
            } catch (Exception e) {
                log.warn("Failed to initialize cassandra cluster due to {}. Will retry in {} ms", e.getMessage(), initRetryInterval);
                try {
                    Thread.sleep(initRetryInterval);
                } catch (InterruptedException ie) {
                    log.warn("Failed to wait until retry", ie);
                }
            }
        }
    }

    private SocketOptions getSocketOptions(ThingsboardEnvironment environment) {
        int connectTimeoutMillis = environment.getProperty("cassandra.socket.connect_timeout", int.class);
        int readTimeoutMillis = environment.getProperty("cassandra.socket.read_timeout", int.class);
        Boolean keepAlive = environment.getProperty("cassandra.socket.keep_alive", Boolean.class);
        Boolean reuseAddress = environment.getProperty("cassandra.socket.reuse_address", Boolean.class);
        Integer soLinger = environment.getProperty("cassandra.socket.so_linger", Integer.class);
        Boolean tcpNoDelay = environment.getProperty("cassandra.socket.tcp_no_delay", Boolean.class);
        Integer receiveBufferSize = environment.getProperty("cassandra.socket.receive_buffer_size", Integer.class);
        Integer sendBufferSize = environment.getProperty("cassandra.socket.send_buffer_size", Integer.class);

        SocketOptions opts = new SocketOptions();
        opts.setConnectTimeoutMillis(connectTimeoutMillis);
        opts.setReadTimeoutMillis(readTimeoutMillis);
        if (keepAlive != null) {
            opts.setKeepAlive(keepAlive);
        }
        if (reuseAddress != null) {
            opts.setReuseAddress(reuseAddress);
        }
        if (soLinger != null) {
            opts.setSoLinger(soLinger);
        }
        if (tcpNoDelay != null) {
            opts.setTcpNoDelay(tcpNoDelay);
        }
        if (receiveBufferSize != null) {
            opts.setReceiveBufferSize(receiveBufferSize);
        }
        if (sendBufferSize != null) {
            opts.setSendBufferSize(sendBufferSize);
        }
        return opts;
    }

    private QueryOptions getQueryOptions(ThingsboardEnvironment environment) {
        Integer defaultFetchSize = environment.getProperty("cassandra.query.default_fetch_size", Integer.class);
        QueryOptions opts = new QueryOptions();
        opts.setFetchSize(defaultFetchSize);
        return opts;
    }

    private List<InetSocketAddress> getContactPoints(String url) {
        List<InetSocketAddress> result;
        if (StringUtils.isBlank(url)) {
            result = Collections.emptyList();
        } else {
            result = new ArrayList<>();
            for (String hostPort : url.split(COMMA)) {
                String host = hostPort.split(COLON)[0];
                Integer port = Integer.valueOf(hostPort.split(COLON)[1]);
                result.add(new InetSocketAddress(host, port));
            }
        }
        return result;
    }


    private ConsistencyLevel getDefaultReadConsistencyLevel() {
        if (defaultReadConsistencyLevel == null) {
            if (readConsistencyLevel != null) {
                defaultReadConsistencyLevel = ConsistencyLevel.valueOf(readConsistencyLevel.toUpperCase());
            } else {
                defaultReadConsistencyLevel = ConsistencyLevel.ONE;
            }
        }
        return defaultReadConsistencyLevel;
    }

    private ConsistencyLevel getDefaultWriteConsistencyLevel() {
        if (defaultWriteConsistencyLevel == null) {
            if (writeConsistencyLevel != null) {
                defaultWriteConsistencyLevel = ConsistencyLevel.valueOf(writeConsistencyLevel.toUpperCase());
            } else {
                defaultWriteConsistencyLevel = ConsistencyLevel.ONE;
            }
        }
        return defaultWriteConsistencyLevel;
    }

}
