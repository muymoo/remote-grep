package org.uiuc.cs.distributed.grep;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uiuc.cs.distributed.grep.util.TestLogs;

/**
 * This class test the application to ensure that distributed grep is returning the correct values.
 * Usage:</br>
 * Before running tests, ensure you have created the log file on each of the nodes
 * 
 * <pre>
 * make createLogs
 * </pre>
 * 
 * and have started the application on the other two nodes (the ones that are not running the test)
 * 
 * <pre>
 * make run
 * </pre>
 * 
 * Then to run the unit tests:
 * 
 * <pre>
 * make runtests
 * </pre>
 * 
 * @author matt
 */
public class ApplicationTest
{

    private final static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    static Application               application;

    @BeforeClass
    public static void initialize()
    {
        application = Application.getInstance();
        application.startGrepServer();

        System.setOut(new PrintStream(outContent));
        TestLogs.createLogFile(5, 100);
    }

    @AfterClass
    public static void cleanUp()
    {
        System.setOut(null);
    }

    @Before
    public void setUp()
    {
        application.addDefaultNodes();
        /*
         * If running locally, you can uncomment this line and comment out one to add default nodes.
         * Application.addTaskForNode(new Node("localhost:4444"));
         */
    }

    @Test
    public void grepForDoubleLine()
    {
        application.runGrepTasks("'There should be 2 of me.'");
        application.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("There should be 2 of me.\nThere should be 2 of me.\n"));
    }

    @Test
    public void grepWithFlags()
    {
        application.runGrepTasks("-rni '2 of me'");
        application.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("101:There should be 2 of me."));
    }

    @Test
    public void grepSevere()
    {
        application.runGrepTasks("-rni severe");
        application.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("SEVERE"));
    }

    @Test
    public void grepWarning()
    {
        application.runGrepTasks("-rni warning");
        application.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("WARNING"));
    }

    @Test
    public void grepRegex()
    {
        application.runGrepTasks("'[1-9]\\+ of me'");
        application.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("There should be 2 of me.\nThere should be 2 of me.\n"));
    }
}
