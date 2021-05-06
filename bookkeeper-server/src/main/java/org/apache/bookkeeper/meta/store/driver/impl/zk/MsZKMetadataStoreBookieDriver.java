package org.apache.bookkeeper.meta.store.driver.impl.zk;

import java.net.URI;

import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.store.api.MetadataStoreConfig;
import org.apache.bookkeeper.meta.store.api.MetadataStoreException;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.meta.store.driver.AbstractMetadataStoreBookieDriver;
import org.apache.bookkeeper.meta.store.impl.ZKMetadataStore;

public class MsZKMetadataStoreBookieDriver extends AbstractMetadataStoreBookieDriver implements ZKMetadataConstants {
    static {
        MetadataDrivers.registerBookieDriver(SCHEME, MsZKMetadataStoreBookieDriver.class);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    protected MetadataStoreExtended createMetadataStore(URI metadataServiceURI) throws MetadataStoreException {
        return new MSMetadataStore(metadataServiceURI.getHost(), MetadataStoreConfig.builder().build());
//        return new ZKMetadataStore(metadataServiceURI.getHost(), MetadataStoreConfig.builder().build());
    }
}
