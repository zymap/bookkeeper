package org.apache.bookkeeper.meta.store.driver;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

public abstract class AbstractMetadataStoreClientDriver extends MetadataStoreDriverBase implements MetadataClientDriver {
    protected ScheduledExecutorService scheduler;
    protected StatsLogger statsLogger;

    @Override
    public MetadataClientDriver initialize(ClientConfiguration conf, ScheduledExecutorService scheduler, StatsLogger statsLogger, Optional<Object> ctx) throws MetadataException {
        super.initialize(conf);
        this.scheduler = scheduler;
        this.statsLogger = statsLogger;
        return this;
    }

    @Override
    public RegistrationClient getRegistrationClient() {
        return new MetadataStoreRegistrationClient(store, scope, scheduler);
    }

    @Override
    public void close() {

    }

    @Override
    public void setSessionStateListener(SessionStateListener sessionStateListener) {

    }
}
