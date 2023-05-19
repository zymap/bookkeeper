/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.beust.jcommander.Parameter;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.base64.Base64Encoder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.ldb.EntryLocationIndex;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageRocksDB;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.collections4.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadLedgerAndEntryFromFileCommand
    extends BookieCommand<ReadLedgerAndEntryFromFileCommand.ReadLedgerFromFileFlag> {

    public ReadLedgerAndEntryFromFileCommand() {
        super(CliSpec.<ReadLedgerFromFileFlag>newBuilder().withName("read")
            .withFlags(new ReadLedgerFromFileFlag()).build());
    }

    static final Logger LOG = LoggerFactory.getLogger(ReadLedgerAndEntryFromFileCommand.class);


    @Accessors(fluent = true)
    @Setter
    public static class ReadLedgerFromFileFlag extends CliFlags {

        @Parameter(names = {"-d", "--ledger-dir"})
        private String ledgerDir = "";

        @Parameter(names = {"-l", "--ledger"}, required = true)
        private long ledgerId;

        @Parameter(names = {"-e", "--entry"}, required = true)
        private long entryId;

        @Parameter(names = {"--list"}, description = "list all the ledger in the rocksDb")
        private boolean list;

        @Parameter(names = {"--list-limit"}, description = "list a fixed size of ledgers")
        private int listLimit;

        @Parameter(names = {"--ledger-file"})
        private String ledgerFile;

        @Parameter(names = {"--output-file"})
        private String entryOutputFile;

        @Parameter(names = {"--scan"})
        private boolean scan;

        @Parameter(names = {"--scan-start"})
        private long scanStart;

        @Parameter(names = {"--scan-end"})
        private long scanEnd;
    }

    @Override
    public boolean apply(ServerConfiguration conf, ReadLedgerFromFileFlag cmdFlags) {
        LOG.info("Running command");
        try {
            if (cmdFlags.scan) {
                scanMissingLedgers(conf, cmdFlags.ledgerFile, cmdFlags.entryOutputFile,
                    cmdFlags.scanStart, cmdFlags.scanEnd);
                return true;
            }
            if (cmdFlags.list) {
                listLedgers(conf, 0, Long.MAX_VALUE, cmdFlags.listLimit);
                return true;
            } else {
                internalRead(conf, cmdFlags.ledgerId, cmdFlags.entryId);
                return true;
            }
        } catch (Exception e) {
            LOG.error("Failed to run the command", e);
            throw new RuntimeException(e);
        }
    }

    private List<Long> ledgerList(String filepath) throws IOException {
        ArrayList<Long> ledgers = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String line = "";
            while (true) {
                line = reader.readLine();
                if (line == null) {
                    break;
                }
                line = line.replace(",", "").replace("\"", "").trim();
                try {
                    Long ledgerId = Long.parseLong(line);
                    ledgers.add(ledgerId);
                } catch (Exception e) {
                    LOG.warn("Failed to parse the the line, the content is {}", line);
                }
            }
        }
        return ledgers;
    }

    private void listLedgers(ServerConfiguration conf, long startLedger, long endLedger, int limit)
        throws Exception {
        System.out.println("Starting command");
        LOG.info("List ledger under the db path {}", conf.getLedgerDirs()[0].getPath() + "/current");
        try (LedgerMetadataIndex ledgerMetadataIndex = new LedgerMetadataIndex(conf,
            KeyValueStorageRocksDB.factory, conf.getLedgerDirs()[0].getPath() + "/current", NullStatsLogger.INSTANCE)) {
            int count = limit;
            int ledgerCount = 0;
            ArrayList<Long> ledgers = new ArrayList<>();
            for (Long ledgerId: ledgerMetadataIndex.getActiveLedgersInRange(startLedger, endLedger)) {
                ledgers.add(ledgerId);
                if (limit != -1) {
                    LOG.info("Ledger id : {}", ledgerId);
                    if (count-- == 0) {
                        break;
                    }
                } else {
                    ledgerCount++;
                }
            }
            LOG.info("This directory has {} ledgers", ledgerCount);
            LOG.info("Ledger list is {}", ledgers);
        }
    }

    private void internalRead(ServerConfiguration conf, long ledgerId, long entryId) throws Exception {
        try (EntryLocationIndex entryLocationIndex = new EntryLocationIndex(conf,
                 KeyValueStorageRocksDB.factory, conf.getLedgerDirs()[0].getPath() + "/current", NullStatsLogger.INSTANCE)) {
            EntryLogger entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
            long location = entryLocationIndex.getLocation(ledgerId, entryId);
            ByteBuf buf = entryLogger.readEntry(ledgerId, entryId, location);
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            LOG.info("Read out message : {}", new String(bytes, StandardCharsets.UTF_8));
            LOG.info("The base64 of message {}", Base64.getEncoder().encodeToString(bytes));
        }
    }


    private void scanMissingLedgers(ServerConfiguration conf, String missingLedgerFile, String outputPath,
                                    long start, long end) throws Exception {
        List<Long> ledgers = ledgerList(missingLedgerFile);
        try (EntryLocationIndex entryLocationIndex = new EntryLocationIndex(conf,
            KeyValueStorageRocksDB.factory, conf.getLedgerDirs()[0].getPath() + "/current", NullStatsLogger.INSTANCE)) {

            EntryLogger entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));

            for (Long ledgerId : ledgers) {
                long lastEntryId = -1;
                try {
                    lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
                } catch (Exception e) {
                    LOG.error("Failed to get the last entry for the ledger {}, skip it", ledgerId);
                    continue;
                }

                long entryId = 0;
                LOG.info("Writing ledger {} entries, the last entry is {}", ledgerId, lastEntryId);
                while (entryId < lastEntryId) {
                    long location = entryLocationIndex.getLocation(ledgerId, entryId);
                    ByteBuf buf;
                    try {
                        buf = entryLogger.readEntry(ledgerId, entryId, location);
                    } catch (Exception e) {
                        LOG.error("Failed to get the entry id {} for ledger {}", entryId, ledgers);
                        continue;
                    }
                    byte[] bytes = new byte[buf.readableBytes()];
                    buf.readBytes(bytes);
                    buf.release();
                    writeEntry(ledgerId, entryId,
                        Base64.getEncoder().encodeToString(bytes), outputPath);
                    entryId++;
                }

                BufferedWriter writer = writeMap.get(ledgerId);
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
            }
        } finally {
            writeMap.values().forEach(w -> {
                try {
                    w.close();
                } catch (IOException e) {
                    LOG.error("Failed to close the writer");
                }
            });
        }
    }

    private static final Map<Long, BufferedWriter> writeMap = new HashedMap<>();

    private void writeEntry(long ledgerId, long entryId, String msg, String basePath) throws IOException{
        LOG.info("Writing the ledger {}, entry {} to the file", ledgerId, entryId);
        BufferedWriter writer = writeMap.get(ledgerId);
        if (writer == null) {
            Path p = Paths.get(basePath + "/" + ledgerId);
            p.toFile().createNewFile();
            writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(p)));
            writeMap.put(ledgerId, writer);
        }

        writer.write(entryId + " : " + msg);
        writer.newLine();
    }

    public static void main(String[] args) throws Exception {
//        Long.MAX_VALUE
//        ServerConfiguration conf = new ServerConfiguration();
//        ReadLedgerAndEntryFromFileCommand cmd = new ReadLedgerAndEntryFromFileCommand();
//        conf.setLedgerDirNames(new String[]{"/Users/yongzhang/work/release/pulsar/apache-pulsar-2.11.0/data/standalone/bookkeeper0"});
//        cmd.scanMissingLedgers(conf, "/Users/yongzhang/work/release/pulsar/apache-pulsar-2.11.0/testmiss",
//            "/Users/yongzhang/work/release/pulsar/apache-pulsar-2.11.0/missingledgers");
//        cmd.listLedgers(conf, 0, 10,100);
//        cmd.internalRead(conf, 0, 1);
//        List<Long> ledgers = cmd.ledgerList("/Users/yongzhang/work/release/pulsar/apache-pulsar-2.11.0/ledgerlist");
//        System.out.println(ledgers.size());
//        String basePath = "/Users/yongzhang/work/release/pulsar/apache-pulsar-2.11.0/data/standalone/bookkeeper0/current";
//        LedgerMetadataIndex ledgerMetadataIndex = new LedgerMetadataIndex(conf,
//            KeyValueStorageRocksDB.factory, basePath, NullStatsLogger.INSTANCE);
//        EntryLocationIndex entryLocationIndex = new EntryLocationIndex(conf,
//            KeyValueStorageRocksDB.factory, basePath, NullStatsLogger.INSTANCE);
//
//        List<Long> list = new ArrayList<>();
//        for (Long aLong : ledgerMetadataIndex.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
//            list.add(aLong);
//        }
//
//        EntryLogger entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs(),
//            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
//        long location = entryLocationIndex.getLocation(40, 0);
//        ByteBuf buf = entryLogger.readEntry(40, 0, location);
//        byte[] bytes = new byte[buf.readableBytes()];
//        buf.readBytes(bytes);
//        System.out.println(new String(bytes));
//
//        ledgerMetadataIndex.close();
//        entryLocationIndex.close();
    }
}
