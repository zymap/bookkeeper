package org.apache.bookkeeper.meta.store.driver;

import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.getLedgerKey;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.getLedgersPath;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.msResult;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.parseLedgerId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.meta.store.api.GetResult;
import org.apache.bookkeeper.meta.store.api.Notification;
import org.apache.bookkeeper.meta.store.api.NotificationType;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;

@Slf4j
public class MetadataStoreLedgerManager implements LedgerManager, Consumer<Notification> {
    private final LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
    private final MetadataStoreExtended store;
    private final String scope;
    private final Map<Long, Set<BookkeeperInternalCallbacks.LedgerMetadataListener>> ledgerMetadataListeners = new HashMap<>();
    private final String ledgersPath;

    public MetadataStoreLedgerManager(MetadataStoreExtended store, String scope) {
        this.store = store;
        this.scope = scope;
        this.store.registerListener(this);
        this.ledgersPath = getLedgersPath(scope);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId, LedgerMetadata metadata) {
        byte[] values;
        try {
            values = serDe.serialize(metadata);
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        return store.put(getLedgerKey(scope, ledgerId), values, Optional.of(-1L))
                .thenApply(stat -> new Versioned<>(metadata, new LongVersion(stat.getVersion())));
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        if (Version.NEW == version || !(version instanceof LongVersion)) {
            return FutureUtils.exception(new BKException.BKMetadataVersionException());
        }
        LongVersion lv = (LongVersion) version;
        return store.delete(getLedgerKey(scope, ledgerId), Optional.of(lv.getLongVersion()));
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        return store.get(getLedgerKey(scope, ledgerId))
                .thenApply(op -> {
                    if (op.isPresent()) {
                        GetResult r = op.get();
                        try {
                            LedgerMetadata metadata = serDe.parseConfig(r.getValue(),
                                    ledgerId, Optional.of(r.getStat().getCreationTimestamp()));
                            return new Versioned<>(metadata, new LongVersion(r.getStat().getVersion()));
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        }
                    } else {
                        throw new CompletionException(new BKException.BKNoSuchLedgerExistsException());
                    }
                });
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, Version currentVersion) {
        if (Version.NEW == currentVersion || !(currentVersion instanceof LongVersion)) {
            return FutureUtils.exception(new BKException.BKMetadataVersionException());
        }
        final LongVersion lv = (LongVersion) currentVersion;
        byte[] values;
        try {
            values = serDe.serialize(metadata);
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
        return store.put(getLedgerKey(scope, ledgerId), values, Optional.of(lv.getLongVersion()))
                .thenApply(stat -> new Versioned<>(metadata, new LongVersion(stat.getVersion())));
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, BookkeeperInternalCallbacks.LedgerMetadataListener listener) {
        log.info("registered ledger metadata listener for [{}]", ledgerId);
        synchronized (this) {
            this.ledgerMetadataListeners.computeIfAbsent(ledgerId, id -> Collections.newSetFromMap(new IdentityHashMap<>()))
                    .add(listener);
        }
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, BookkeeperInternalCallbacks.LedgerMetadataListener listener) {
        log.info("unregistered ledger metadata listener for [{}]", ledgerId);
        synchronized (this) {
            this.ledgerMetadataListeners.computeIfPresent(ledgerId, (id, set) -> {
                set.remove(listener);
                return set;
            });
        }
    }

    private List<BookkeeperInternalCallbacks.LedgerMetadataListener> getLedgerMetadataListeners(long ledgerId) {
        synchronized (this) {
            return ImmutableList.copyOf(this.ledgerMetadataListeners.getOrDefault(ledgerId, Collections.emptySet()));
        }
    }

    @Override
    public void asyncProcessLedgers(BookkeeperInternalCallbacks.Processor<Long> processor, AsyncCallback.VoidCallback finalCb, Object context, int successRc, int failureRc) {
        LedgerRangeIterator iterator = getLedgerRanges(0);

        try {
            while (iterator.hasNext()) {
                LedgerRange range = iterator.next();
                for (Long ledger : range.getLedgers()) {
                    processor.process(ledger, finalCb);
                }
            }
            finalCb.processResult(successRc, null, context);
        } catch (IOException e) {
            finalCb.processResult(failureRc, null, context);
        }
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeOutMs) {
        try {
            List<String> ledgers = msResult(store.getChildren(getLedgersPath(scope)));
            Set<Long> ids = ledgers.stream()
                    .map(MetadataStoreUtils::parseLedgerKey)
                    .map(UUID::getLeastSignificantBits)
                    .collect(Collectors.toSet());

            return new LedgerRangeIterator() {
                private boolean read = false;

                @Override
                public boolean hasNext() throws IOException {
                    return !read;
                }

                @Override
                public LedgerRange next() throws IOException {
                    Preconditions.checkState(hasNext());
                    read = true;
                    return new LedgerRange(ids);
                }
            };
        } catch (BookieException.MetadataStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            this.ledgerMetadataListeners.clear();
        }
    }

    @Override
    public void accept(Notification notification) {
        if (!notification.getPath().startsWith(ledgersPath) && notification.getPath().length() > ledgersPath.length() + 1) {
            return;
        }
        if (notification.getType() != NotificationType.Modified) {
            return;
        }
        long ledgerId = parseLedgerId(notification.getPath());
        List<BookkeeperInternalCallbacks.LedgerMetadataListener> listeners = getLedgerMetadataListeners(ledgerId);
        if (!listeners.isEmpty()) {
            readLedgerMetadata(ledgerId)
                    .whenComplete((versioned, throwable) -> {
                        listeners.forEach(listener -> listener.onChanged(ledgerId, versioned));
                    });
        }
    }
}
