package org.apache.bookkeeper.tools.cli.commands.bookie;

import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LogMark;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.DiskChecker;

import java.io.File;

public class InitCommand extends BookieCommand<CliFlags> {

    private static final String NAME = "init";
    private static final String DESC = "Initialize new Bookie.";

    public InitCommand() {
        super(CliSpec.newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, RegistrationManager rm, CliFlags cmdFlags) {
        LedgerDirsManager dirsManager = new LedgerDirsManager(
            conf, conf.getJournalDirs(),
            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        File[] journalDirs = conf.getJournalDirs();

        for (int idx = 0; idx < journalDirs.length; idx++) {
            Journal journal = new Journal(idx, journalDirs[idx], conf, dirsManager);
            LogMark lastLogMark = journal.getLastLogMark().getCurMark();
            System.out.println("LastLogMark : Journal Id - " + lastLogMark.getLogFileId() + "("
                + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                + lastLogMark.getLogFileOffset());
        }
        return true;
    }
}
