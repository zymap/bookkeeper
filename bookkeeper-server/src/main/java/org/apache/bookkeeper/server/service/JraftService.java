/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.server.service;

import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.Endpoint;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.JraftYamlUtil;

/**
 * A {@link ServerLifecycleComponent} that runs jraft service.
 */
@Slf4j
public class JraftService extends ServerLifecycleComponent {

    public static final String NAME = "jraft-service";

    private JraftNode jraftNode;

    public JraftService(String configPath,
                        BookieConfiguration conf,
                        StatsLogger statsLogger) {
        super(NAME, conf, statsLogger);
        RheaKVStoreOptions options = JraftYamlUtil.readConfig(configPath);
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured()
                .withFake(options.getPlacementDriverOptions().isFake()) // use a fake pd
                .config();
        String ip = options.getStoreEngineOptions().getServerAddress().getIp();
        int port = options.getStoreEngineOptions().getServerAddress().getPort();
        final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured.newConfigured() //
                .withStorageType(options.getStoreEngineOptions().getStorageType())
                .withRocksDBOptions(RocksDBOptionsConfigured.newConfigured().withDbPath(options
                        .getStoreEngineOptions().getRocksDBOptions().getDbPath()).config())
                .withRaftDataPath(options.getStoreEngineOptions().getRaftDataPath())
                .withServerAddress(new Endpoint(ip, port))
                .config();
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
                .withClusterName(options.getClusterName()) //
                .withInitialServerList(options.getInitialServerList())
                .withStoreEngineOptions(storeOpts) //
                .withPlacementDriverOptions(pdOpts) //
                .config();
        System.out.println(opts);
        this.jraftNode = new JraftNode(opts);
        System.out.println("server start OK! ip : " + ip + " port : " + port);
    }

    @Override
    protected void doStart() {
        jraftNode.start();
        DistributedLock<byte[]> lock = jraftNode.getRheaKVStore().getDistributedLock("init-cluster", 30, TimeUnit.SECONDS);
        if (lock.tryLock()) {
            try (MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(URI.create(conf.getServerConf().getMetadataServiceUri()))) {
                driver.initialize(conf.getServerConf(), null, statsLogger);
                driver.getRegistrationManager()
                        .initNewCluster();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    protected void doStop() {
        jraftNode.stop();
    }

    @Override
    protected void doClose() throws IOException {
        jraftNode.stop();
    }

    @Override
    public void publishInfo(ComponentInfoPublisher componentInfoPublisher) {
    }
}
