package org.apache.bookkeeper.meta.store.driver;

import java.io.IOException;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;

public class MetadataStoreLedgerManagerFactory implements LedgerManagerFactory {
    static final int VERSION = 0;

    private String scope;
    private MetadataStoreExtended store;

    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf, LayoutManager layoutManager, int factoryVersion) throws IOException {
        MetadataStoreLayoutManager metadataStoreLayoutManager = (MetadataStoreLayoutManager) layoutManager;
        if (VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        try {
            ServiceURI uri = ServiceURI.create(conf.getMetadataServiceUri());
            this.scope = uri.getServicePath();
        } catch (ConfigurationException e) {
            throw new IOException("Invalid metadata service uri", e);
        }
        this.store = metadataStoreLayoutManager.getStore();
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        return new MetadataStore64bitIdGenerator(store, scope);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new MetadataStoreLedgerManager(store, scope);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager() throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void format(AbstractConfiguration<?> conf, LayoutManager lm) throws InterruptedException, KeeperException, IOException {

    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager lm) throws InterruptedException, KeeperException, IOException {
        return false;
    }
}
