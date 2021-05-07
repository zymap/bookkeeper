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

package org.apache.bookkeeper.meta.store.driver;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

/**
 * Generate 64-bit ledger ids from a bucket.
 *
 * <p>The most significant 8 bits is used as bucket id. The remaining 56 bits are
 * used as the id generated per bucket.
 */
@Slf4j
class MetadataStore64bitIdGenerator implements LedgerIdGenerator {

    static final long MAX_ID_PER_BUCKET = 0x00ffffffffffffffL;
    static final long BUCKET_ID_MASK = 0xff00000000000000L;
    static final int BUCKET_ID_SHIFT = 56;
    static final int NUM_BUCKETS = 0x80;
    private static final AtomicIntegerFieldUpdater<MetadataStore64bitIdGenerator> nextBucketIdUpdater =
            AtomicIntegerFieldUpdater.newUpdater(MetadataStore64bitIdGenerator.class, "nextBucketId");
    private final String scope;
    private final MetadataStoreExtended store;
    private volatile int nextBucketId;

    MetadataStore64bitIdGenerator(MetadataStoreExtended store, String scope) {
        this.store = store;
        this.scope = scope;
        this.nextBucketId = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
    }

    static int getBucketId(long lid) {
        return (int) ((lid & BUCKET_ID_MASK) >>> BUCKET_ID_SHIFT);
    }

    static long getIdInBucket(long lid) {
        return lid & MAX_ID_PER_BUCKET;
    }

    int nextBucketId() {
        while (true) {
            int bucketId = nextBucketIdUpdater.incrementAndGet(this);
            if (bucketId >= NUM_BUCKETS) {
                if (nextBucketIdUpdater.compareAndSet(this, bucketId, 0)) {
                    bucketId = 0;
                } else {
                    // someone has been updated bucketId, try it again.
                    continue;
                }
            }
            return bucketId;
        }
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        int bucketId = nextBucketId();
        checkArgument(bucketId >= 0 && bucketId < NUM_BUCKETS,
                "Invalid bucket id : " + bucketId);

        String bucketKey = MetadataStoreUtils.getBucketPath(scope, bucketId);
        this.store.put(bucketKey, new byte[0], Optional.empty())
                .whenComplete((stat, throwable) -> {
                    if (throwable != null) {
                        cb.operationComplete(Code.UnexpectedConditionException, null);
                    } else {
                        if (stat.getVersion() > MAX_ID_PER_BUCKET) {
                            log.warn("Etcd bucket '{}' is overflowed", bucketKey);
                            // the bucket is overflowed, moved to next bucket.
                            generateLedgerId(cb);
                        } else {
                            long version = stat.getVersion();
                            long lid = ((((long) bucketId) << BUCKET_ID_SHIFT) & BUCKET_ID_MASK)
                                    | (version & MAX_ID_PER_BUCKET);
                            cb.operationComplete(Code.OK, lid);
                        }
                    }
                });
    }

    @Override
    public void close() {
        // no-op
    }
}
