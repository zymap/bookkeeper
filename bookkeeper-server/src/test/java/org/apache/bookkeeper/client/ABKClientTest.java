/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class ABKClientTest extends BookKeeperClusterTestCase {
    public ABKClientTest() {
        super(1);
    }

    @Override
    public void setUp() throws Exception {
        baseConf.setNumAddWorkerThreads(0);
        baseConf.setNumReadWorkerThreads(0);
        baseConf.setNumHighPriorityWorkerThreads(0);
        super.setUp();
    }

    @Test
    public void test() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setUseV2WireProtocol(true);
        BookKeeper bkc = new BookKeeper(conf);

        final long[] ledgerId = {-1};

        final CountDownLatch latch = new CountDownLatch(1);
        bkc.asyncCreateLedger(1, 1, BookKeeper.DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8), new AsyncCallback.CreateCallback() {
            @Override
            public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                System.out.println("Create complete " + lh.getId());
                    ledgerId[0] = lh.getId();
                    lh.asyncAddEntry("hello".getBytes(), new AsyncCallback.AddCallback() {
                        @Override
                        public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {

                            System.out.println("Add complete " + rc);
                            if (rc == BKException.Code.OK) {
                                bkc.asyncOpenLedger(ledgerId[0], BookKeeper.DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8), new AsyncCallback.OpenCallback() {
                                    @Override
                                    public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                                        System.out.println("bk returned code " + rc + " : " + BKException.getMessage(rc));
                                        if (rc != BKException.Code.OK) {
                                            System.out.println("error");
                                        }
                                        latch.countDown();
                                    }
                                }, null);
//                                lh.closeAsync()
//                                    .whenComplete((unused, throwable) -> {
//                                        System.out.println("Close complete");
//                                        if (throwable == null) {
//                                            System.out.println("Open ledger again " + ledgerId[0]);
//                                            bkc.asyncOpenLedger(ledgerId[0], BookKeeper.DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8), new AsyncCallback.OpenCallback() {
//                                                @Override
//                                                public void openComplete(int rc, LedgerHandle lh, Object ctx) {
//                                                    System.out.println("bk returned code " + rc);
//                                                    if (rc != BKException.Code.OK) {
//                                                        System.out.println("error");
//                                                    }
//                                                    latch.countDown();
//                                                }
//                                            }, null);
//                                        }
//                                    });
                            }
                        }
                    }, null);
            }
        }, null);

        latch.await();
    }
}
