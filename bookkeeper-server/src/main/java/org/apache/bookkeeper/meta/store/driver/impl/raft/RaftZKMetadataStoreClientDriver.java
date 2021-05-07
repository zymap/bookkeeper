package org.apache.bookkeeper.meta.store.driver.impl.raft;

import java.net.URI;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.store.api.MetadataStoreConfig;
import org.apache.bookkeeper.meta.store.api.MetadataStoreException;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.meta.store.driver.AbstractMetadataStoreClientDriver;

public class RaftZKMetadataStoreClientDriver extends AbstractMetadataStoreClientDriver implements RaftMetadataConstants {
    static {
        MetadataDrivers.registerClientDriver(SCHEME, RaftZKMetadataStoreClientDriver.class);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    protected MetadataStoreExtended createMetadataStore(URI metadataServiceURI) throws MetadataStoreException {
        return new RaftMetadataStore(metadataServiceURI.getHost(), MetadataStoreConfig.builder().build());
    }
}
