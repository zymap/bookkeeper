package org.apache.bookkeeper.meta.store.driver;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

public abstract class AbstractMetadataStoreBookieDriver extends MetadataStoreDriverBase implements MetadataBookieDriver {
    protected StatsLogger statsLogger;

    @Override
    public MetadataBookieDriver initialize(ServerConfiguration conf, RegistrationManager.RegistrationListener listener, StatsLogger statsLogger) throws MetadataException {
        super.initialize(conf);
        this.statsLogger = statsLogger;
        return this;
    }

    @Override
    public RegistrationManager getRegistrationManager() {
        return new MetadataStoreRegistrationManager(store, scope);
    }

    @Override
    public void close() {

    }
}
