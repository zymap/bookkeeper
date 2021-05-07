package org.apache.bookkeeper.meta.store.driver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.getBookiesPath;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.getClusterInstanceIdPath;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.getCookiePath;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.getWritableBookiePath;
import static org.apache.bookkeeper.meta.store.driver.MetadataStoreUtils.msResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerLayout;
import org.apache.bookkeeper.meta.store.api.GetResult;
import org.apache.bookkeeper.meta.store.api.extended.MetadataStoreExtended;
import org.apache.bookkeeper.meta.store.util.ObjectMapperFactory;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

@Slf4j
public class MetadataStoreRegistrationManager implements RegistrationManager {
    private final MetadataStoreExtended store;
    private final String scope;
    private final RegistrationManager.RegistrationListener registrationListener;

    public MetadataStoreRegistrationManager(MetadataStoreExtended store, String scope, RegistrationListener registrationListener) {
        this.store = store;
        this.scope = scope;
        this.registrationListener = registrationListener;
    }

    @Override
    public void close() {

    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        Optional<GetResult> resp = msResult(store.get(getClusterInstanceIdPath(scope)));
        if (resp.isPresent()) {
            return new String(resp.get().getValue());
        } else {
            throw new BookieException.MetadataStoreException("BookKeeper is not initialized under '" + scope + "' yet");
        }
    }

    @Override
    public void registerBookie(BookieId bookieId, boolean readOnly, BookieServiceInfo serviceInfo) throws BookieException {
        if (readOnly) {
            throw new UnsupportedOperationException();
        } else {
            doRegisterBookie(getWritableBookiePath(scope, bookieId), serviceInfo);
        }
    }

    private void doRegisterBookie(String writableBookiePath, BookieServiceInfo serviceInfo) {
        try {
            msResult(store.put(writableBookiePath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(serviceInfo), Optional.empty()));
        } catch (BookieException.MetadataStoreException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unregisterBookie(BookieId bookieId, boolean readOnly) throws BookieException {
        if (readOnly) {
            throw new UnsupportedOperationException();
        } else {
            doUnregisterBookie(getWritableBookiePath(scope, bookieId));
        }
    }

    private void doUnregisterBookie(String writableBookiePath) {
        try {
            msResult(store.delete(writableBookiePath, Optional.empty()));
        } catch (BookieException.MetadataStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isBookieRegistered(BookieId bookieId) throws BookieException {
        return msResult(store.exists(getWritableBookiePath(scope, bookieId)));
    }

    @Override
    public void writeCookie(BookieId bookieId, Versioned<byte[]> cookieData) throws BookieException {
        String path = getCookiePath(scope, bookieId);
        if (cookieData.getVersion() == Version.NEW) {
            msResult(store.put(path, cookieData.getValue(), Optional.of(-1L)));
        } else {
            LongVersion lv = (LongVersion) cookieData.getVersion();
            msResult(store.put(path, cookieData.getValue(), Optional.of(lv.getLongVersion())));
        }
    }

    @Override
    public Versioned<byte[]> readCookie(BookieId bookieId) throws BookieException {
        Optional<GetResult> resp = msResult(store.get(getCookiePath(scope, bookieId)));
        if (resp.isPresent()) {
            return new Versioned<>(resp.get().getValue(), new LongVersion(resp.get().getStat().getVersion()));
        } else {
            throw new BookieException.CookieNotFoundException(bookieId.toString());
        }
    }

    @Override
    public void removeCookie(BookieId bookieId, Version version) throws BookieException {
        msResult(store.delete(getCookiePath(scope, bookieId), Optional.empty()));
    }

    @Override
    public boolean prepareFormat() throws Exception {
        return msResult(store.exists(scope));
    }

    @Override
    public boolean initNewCluster() throws Exception {
        return initNewCluster(store, scope);
    }

    static boolean initNewCluster(MetadataStoreExtended store, String scope) throws Exception {
        String rootScopeKey = scope;
        String instanceId = UUID.randomUUID().toString();
        LedgerLayout layout = new LedgerLayout(
                MetadataStoreLedgerManagerFactory.class.getName(),
                MetadataStoreLedgerManagerFactory.VERSION
        );

        if (msResult(store.exists(rootScopeKey))) {
            log.info("cluster at scope [{}] already exists");
            return false;
        }

        // `${scope}`
        msResult(store.put(rootScopeKey, MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/layout`
        msResult(store.put(MetadataStoreUtils.getLayoutKey(scope), layout.serialize(), Optional.empty()));
        // `${scope}/instanceid`
        msResult(store.put(MetadataStoreUtils.getClusterInstanceIdPath(scope), instanceId.getBytes(UTF_8), Optional.empty()));
        // `${scope}/cookies`
        msResult(store.put(MetadataStoreUtils.getCookiesPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/bookies`
        msResult(store.put(getBookiesPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/bookies/writable`
        msResult(store.put(MetadataStoreUtils.getWritableBookiesPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/bookies/readonly`
        msResult(store.put(MetadataStoreUtils.getReadonlyBookiesPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/ledgers`
        msResult(store.put(MetadataStoreUtils.getLedgersPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/buckets`
        msResult(store.put(MetadataStoreUtils.getBucketsPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        // `${scope}/underreplication`
        msResult(store.put(MetadataStoreUtils.getUnderreplicationPath(scope), MetadataStoreConstants.EMPTY_BYTES, Optional.empty()));
        return true;
    }

    @Override
    public boolean format() throws Exception {
        return format(store, scope);
    }

    static boolean format(MetadataStoreExtended store, String scope) throws Exception {
        String rootScopeKey = scope;
        Optional<GetResult> resp = msResult(store.get(rootScopeKey));
        if (!resp.isPresent()) {
            // cluster doesn't exist
            return initNewCluster(store, scope);
        } else if (nukeExistingCluster(store, scope)) { // cluster exists and has successfully nuked it
            return initNewCluster(store, scope);
        } else {
            return false;
        }
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        return nukeExistingCluster(store, scope);
    }

    static boolean nukeExistingCluster(MetadataStoreExtended store, String scope) throws Exception {
        String rootScopeKey = scope;
        Optional<GetResult> resp = msResult(store.get(rootScopeKey));
        if (!resp.isPresent()) {
            log.info("There is no existing cluster with under scope '{}' in Etcd, "
                    + "so exiting nuke operation", scope);
            return true;
        }
        log.warn("To nuclear {}", scope);
        return true;
    }
}
