package org.uiuc.cs.distributed.grep;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uiuc.cs.distributed.grep.util.TestLogs;

public class RemoteGrepApplicationTest
{

    private final static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    static RemoteGrepApplication               remoteGrepApplication;
    private static List<String>                ipAndPorts = Arrays.asList("130.126.112.146:4444",
                                                                  "130.126.112.148:4444");

    @BeforeClass
    public static void initialize()
    {
        remoteGrepApplication = RemoteGrepApplication.getInstance();
        System.setOut(new PrintStream(outContent));
        TestLogs.createLogFile(5,100);

        // Create tasks for each machine
        for (String ipAndPort : ipAndPorts)
        {
            RemoteGrepApplication.addTaskForNode(new Node(ipAndPort));
        }
    }

    @AfterClass
    public static void cleanUp()
    {
        System.setOut(null);
    }

    @Test
    public void grepForError()
    {
        RemoteGrepApplication.runGrepTasks("error");
        RemoteGrepApplication.joinGrepTasks();
        Assert.assertEquals("errors", outContent.toString());
    }

    @Test
    public void promptsUserForInput()
    {
        Assert.assertEquals(">>", outContent.toString());
    }

    @Test
    public void executesGrepCommand()
    {
        outContent.reset();

        ByteArrayInputStream in = new ByteArrayInputStream("1".getBytes());
        // "grep 'ERROR' machine.1.log".getBytes());
        System.setIn(in);
        Assert.assertEquals("/tmp/machine.1.log:1:14:53 [ERROR] Cannot read machine code.", outContent.toString());
    }

    // @Test
    // public void listensOnSocket() {
    // remoteGrepApplication.startServer();
    // Assert.assertTrue(false);
    // }
}
