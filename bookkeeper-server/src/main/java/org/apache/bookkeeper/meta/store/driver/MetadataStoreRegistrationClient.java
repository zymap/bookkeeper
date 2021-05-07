package org.apache.bookkeeper.meta.store.driver;

import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.ioResult;

import java.io.IOException;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

@Slf4j
public class MetadataStoreRegistrationClient implements RegistrationClient {
    private final MetadataStoreExtended store;
    private final String scope;
    private final ScheduledExecutorService scheduler;
    private final Map<RegistrationListener, ScheduledFuture<?>> listenerFutures = new IdentityHashMap<>();

    public MetadataStoreRegistrationClient(MetadataStoreExtended store, String scope, ScheduledExecutorService scheduler) {
        this.store = store;
        this.scope = scope;
        this.scheduler = scheduler;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getWritableBookies() {
        return store.getChildren(MetadataStoreUtils.getWritableBookiesPath(scope))
                .thenApply(list -> new Versioned<>(list.stream().map(BookieId::parse)
                        .collect(Collectors.toSet()), Version.NEW));
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
        return getWritableBookies();
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
        return CompletableFuture.completedFuture(new Versioned<>(new HashSet<>(), Version.NEW));
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        if (listener == null) {
            return null;
        }
        listenerFutures.computeIfAbsent(listener, key -> scheduler.scheduleWithFixedDelay(() -> {
            try {
                Versioned<Set<BookieId>> bookies = ioResult(getWritableBookies());
                listener.onBookiesChanged(bookies);
            } catch (IOException e) {
                log.warn("Failed to poll bookies", e);
            }
        }, 0, 5000, TimeUnit.MICROSECONDS));
        return getWritableBookies().thenAccept(listener::onBookiesChanged);
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener listener) {
        listenerFutures.computeIfPresent(listener, (l, future) -> {
            future.cancel(false);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener listener) {

    }
}
