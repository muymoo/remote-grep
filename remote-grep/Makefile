
# it would be nice if we could reference all the java files with a *.java
#  the command:  javac -sourcepath src/org/uiuc/cs/distributed/grep/*.java 
# was only compiling RemoteGrep.class for some reason...
all:
	mkdir -p classes
	javac src/org/uiuc/cs/distributed/grep/Grep.java src/org/uiuc/cs/distributed/grep/RemoteGrepApplication.java src/org/uiuc/cs/distributed/grep/GrepTask.java src/org/uiuc/cs/distributed/grep/GrepServer.java -d classes

run:
	mkdir -p /tmp/cs425_momontbowling/logs
	java -cp classes org.uiuc.cs.distributed.grep.RemoteGrepApplication

test:
	#java -cp 3p/jars/junit-4.11.jar ...
	echo "TODO: complete junit test call"
	

clean:
	rm -rf classes

info:
	echo "TODO: list Java version, Junit version, etc."
