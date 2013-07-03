

# the main compilation target
all:
	mkdir -p bin/test
	javac src/org/uiuc/cs/distributed/grep/Grep.java src/org/uiuc/cs/distributed/grep/Application.java src/org/uiuc/cs/distributed/grep/GrepTask.java src/org/uiuc/cs/distributed/grep/GrepServer.java src/org/uiuc/cs/distributed/grep/Node.java src/org/uiuc/cs/distributed/grep/GroupClient.java src/org/uiuc/cs/distributed/grep/GroupServer.java src/org/uiuc/cs/distributed/grep/FailureDetectorServer.java src/org/uiuc/cs/distributed/grep/FailureDetectorClient.java src/org/uiuc/cs/distributed/grep/GroupMembership.java src/org/uiuc/cs/distributed/grep/DistributedFileSystem.java -d bin
	javac -cp 3p/jars/junit-4.11.jar:3p/jars/hamcrest-core-1.3.jar:bin test/org/uiuc/cs/distributed/grep/NodeTest.java test/org/uiuc/cs/distributed/grep/AllTests.java test/org/uiuc/cs/distributed/grep/ApplicationTest.java test/org/uiuc/cs/distributed/grep/util/TestLogs.java -d bin/test

# the main driver for the program
# needs to be run on each node
run:
	java -cp bin org.uiuc.cs.distributed.grep.Application

# this target runs the unit tests
# The other two nodes need to be up and running before unit tests can run ('make run' on those severs first)
runtests:
	java -cp 3p/jars/junit-4.11.jar:3p/jars/hamcrest-core-1.3.jar:bin:bin/test org.junit.runner.JUnitCore org.uiuc.cs.distributed.grep.AllTests
	

clean:
	rm -rf bin

# To create logs of different sizes, you can add args the java -cp ...TestLogs line
# TestLogs <machine index [5-7]> <number of lines [100000=10MB, 1000000=100MB, 10000000=1GB]>
# Default: 5 10000
createLogs:
	mkdir -p /tmp/cs425_momontbowling2/logs
	rm -f /tmp/cs425_momontbowling2/*.log
	java -cp bin/test org.uiuc.cs.distributed.grep.util.TestLogs

info:
	echo "Java 1.6, Junit 4.11, by Evan Bowling and Matt Momont"
