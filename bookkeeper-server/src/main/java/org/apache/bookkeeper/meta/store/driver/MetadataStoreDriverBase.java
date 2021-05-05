package org.apache.bookkeeper.meta.store.driver;

import java.io.IOException;
import java.net.URI;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.store.api.MetadataStoreException;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.commons.configuration.ConfigurationException;

public abstract class MetadataStoreDriverBase implements AutoCloseable {
    protected AbstractConfiguration<?> conf;
    protected MetadataStoreExtended store;
    protected String scope;
    protected LayoutManager layoutManager;
    protected LedgerManagerFactory ledgerManagerFactory;

    @Override
    public void close() throws Exception {
        if (this.store != null) {
            this.store.close();
        }
    }

    protected void initialize(AbstractConfiguration<?> conf) throws MetadataException {
        this.conf = conf;
        try {
            URI metadataServiceURI = URI.create(conf.getMetadataServiceUri());
            this.scope = metadataServiceURI.getPath();
            this.store = createMetadataStore(metadataServiceURI);

            this.layoutManager = new MetadataStoreLayoutManager(store, scope);
            this.ledgerManagerFactory = new MetadataStoreLedgerManagerFactory();
            this.ledgerManagerFactory.initialize(conf, layoutManager, MetadataStoreLedgerManagerFactory.VERSION);
        } catch (ConfigurationException e) {
            throw new MetadataException(Code.INVALID_METADATA_SERVICE_URI, e);
        } catch (IOException e) {
            throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
        }
    }

    protected abstract MetadataStoreExtended createMetadataStore(URI metadataServiceURI) throws MetadataStoreException;

    public LayoutManager getLayoutManager() {
        return layoutManager;
    }

    public LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
        return ledgerManagerFactory;
    }
}
