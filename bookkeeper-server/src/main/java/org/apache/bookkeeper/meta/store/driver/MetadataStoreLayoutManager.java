package org.apache.bookkeeper.meta.store.driver;

import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.ioResult;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.ioResult;

import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.meta.store.api.GetResult;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;

@Slf4j
@Getter(AccessLevel.PACKAGE)
public class MetadataStoreLayoutManager implements LayoutManager {
    private final MetadataStoreExtended store;
    private final String scope;
    private final String layoutKey;

    public MetadataStoreLayoutManager(MetadataStoreExtended store, String scope) {
        this.store = store;
        this.scope = scope;
        this.layoutKey = MetadataStoreUtils.getLayoutKey(scope);
    }

    @Override
    public LedgerLayout readLedgerLayout() throws IOException {
        Optional<GetResult> response = ioResult(this.store.get(this.layoutKey));
        if (!response.isPresent()) {
            return null;
        }
        return LedgerLayout.parseLayout(response.get().getValue());
    }

    @Override
    public void storeLedgerLayout(LedgerLayout layout) throws IOException {
        ioResult(this.store.put(this.layoutKey, layout.serialize(), Optional.empty()));
    }

    @Override
    public void deleteLedgerLayout() throws IOException {
        ioResult(this.store.delete(this.layoutKey, Optional.empty()));
    }
}
