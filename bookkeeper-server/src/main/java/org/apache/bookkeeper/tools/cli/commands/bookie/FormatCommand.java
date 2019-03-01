package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.Cookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.versioning.Versioned;

import java.io.PrintStream;

public class FormatCommand extends BookieCommand<FormatCommand.FormatFlags> {

    private static final String NAME = "format";
    private static final String DESC = "Format the current server contents.";

//    @Override
//    public boolean apply(ServerConfiguration conf, CliFlags cmdFlags) {
//        return true;
//    }

    /**
     * Flags to
     */
    @Accessors(fluent = true)
    @Setter
    public static class FormatFlags extends CliFlags {

        @Parameter(
                names = {"-n", "--nonInteractive"},
                required = true)
        private boolean nonInteractive;

        @Parameter(
                names = {"-f", "--force"},
                required = true
        )
        private boolean force;

        @Parameter(
                names = {"-d", "--deletecookie"},
                required = true
        )
        private boolean deleteCookie;

    }

    public FormatCommand() {
        this(new FormatFlags());
    }

    protected FormatCommand(PrintStream console){
        this(new FormatFlags(), console);
    }

    public FormatCommand(FormatFlags flags) {
        this(flags, System.out);
    }

    public FormatCommand(FormatFlags flags, PrintStream console) {
        super(CliSpec.<FormatFlags>newBuilder().
            withName(NAME).
            withDescription(DESC).
            withFlags(flags).
            withConsole(console).
            build());
    }

    @Override
    public boolean apply(ServerConfiguration conf, RegistrationManager rm, FormatFlags cmdFlags) {
        boolean interactive = cmdFlags.nonInteractive;
        boolean force = cmdFlags.force;
        boolean deleteCookie = cmdFlags.deleteCookie;

        boolean result = Bookie.format(conf, interactive, force);
        //delete cookie
        if (deleteCookie) {
            try {
                Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, conf);
                cookie.getValue().deleteFromRegistrationManager(rm, conf, cookie.getVersion());
            } catch (BookieException.CookieNotFoundException e) {

            } catch (BookieException e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
        }

        return false;
    }
}
