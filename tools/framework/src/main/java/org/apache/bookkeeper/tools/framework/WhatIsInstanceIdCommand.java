//package org.apache.bookkeeper.tools.framework;
//
//
//import org.apache.bookkeeper.bookie.BookieException;
//import org.apache.bookkeeper.conf.ServerConfiguration;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;
//
///**
// * WhatIsInstanceIdCommand command to show the instanceid of the cluster.
// */
//class WhatIsInstanceIdCommand<GlobalFlagsT extends CliFlags, CommandFlagsT extends CliFlags> implements Command {
//
//    static final Logger LOG = LoggerFactory.getLogger(WhatIsInstanceIdCommand.class);
//
//    final ServerConfiguration bkConf = new ServerConfiguration();
//
//    private final Cli<CommandFlagsT> cli;
//
//    WhatIsInstanceIdCommand(Cli<CommandFlagsT> cli) {
//        this.cli = cli;
//    }
//
//    @Override
//    public String name() {
//        return "whatisinstanceid";
//    }
//
//    @Override
//    public String description() {
//        return "Print the instanceid of the cluster";
//    }
//
//    @Override
//    public Boolean apply(CliFlags cliFlags, String[] args) throws Exception {
//        if (args.length == 0) {
//            cli.usage();
//            return true;
//        }
//
//        runFunctionWithRegistrationManager(bkConf, rm -> {
//            String readInstanceId = null;
//            try {
//                readInstanceId = rm.getClusterInstanceId();
//            } catch (BookieException e) {
//                e.printStackTrace();
//            }
//            LOG.info("Metadata Service Uri: {} InstanceId: {}", bkConf.getMetadataServiceUriUnchecked(), readInstanceId);
//            return null;
//        });
//        return true;
//    }
//
//    @Override
//    public void usage() {
//        cli.console("What is instance id.");
//        CommandUtils.printUsage(cli.console(), cli.cmdPath() + "whatisinstanceid");
//    }
//}
