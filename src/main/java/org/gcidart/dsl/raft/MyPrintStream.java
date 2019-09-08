package org.gcidart.dsl.raft;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

class MyPrintStream extends PrintStream {

    public MyPrintStream(File out) throws FileNotFoundException {
        super(out);
    }

    @Override
    public void println(String string) {
        super.println("[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")) + "] " + string);
    }
}
