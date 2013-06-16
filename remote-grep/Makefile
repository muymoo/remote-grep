

# the main compilation target
# TODO:   all target
# it would be nice if we could reference all the java files with a *.java
#  the command:  javac -sourcepath src/org/uiuc/cs/distributed/grep/*.java 
# was only compiling RemoteGrep.class for some reason...
all:
	mkdir -p classes
	mkdir -p testclasses
	javac src/org/uiuc/cs/distributed/grep/Grep.java src/org/uiuc/cs/distributed/grep/RemoteGrepApplication.java src/org/uiuc/cs/distributed/grep/GrepTask.java src/org/uiuc/cs/distributed/grep/GrepServer.java src/org/uiuc/cs/distributed/grep/Node.java src/org/uiuc/cs/distributed/grep/util/TestLogs.java -d classes
	javac -cp 3p/jars/junit-4.11.jar:3p/jars/hamcrest-core-1.3.jar:classes test/org/uiuc/cs/distributed/grep/NodeTest.java test/org/uiuc/cs/distributed/grep/AllTests.java test/org/uiuc/cs/distributed/grep/RemoteGrepApplicationTest.java test/org/uiuc/cs/distributed/grep/RemoteGrepTest.java -d testclasses

# the main driver for the program
# needs to be run on each node
run:
	mkdir -p /tmp/cs425_momontbowling/logs
	java -cp classes org.uiuc.cs.distributed.grep.RemoteGrepApplication

# this target runs the unit tests
runtests:
	java -cp 3p/jars/junit-4.11.jar:3p/jars/hamcrest-core-1.3.jar:classes:testclasses org.junit.runner.JUnitCore test org.uiuc.cs.distributed.grep.AllTests
	

clean:
	rm -rf classes testclasses

createLogs:
	mkdir -p /tmp/cs425_momontbowling/logs
	rm -f /tmp/cs425_momontbowling/*.log
	java -cp classes org.uiuc.cs.distributed.grep.util.TestLogs

info:
	echo "TODO: list Java version, Junit version, etc."
