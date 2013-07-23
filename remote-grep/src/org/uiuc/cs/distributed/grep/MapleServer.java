package org.uiuc.cs.distributed.grep;

import java.util.Arrays;
import java.util.List;

public class MapleServer extends Thread {
	private String executable;
	private String intermediateFilePrefix;
	private List<String> sdfsSourceFiles;

	public MapleServer(String[] commands) {
		super("MapleServerThread");
		executable = commands[1];
		intermediateFilePrefix = commands[2];
		sdfsSourceFiles = Arrays.asList(commands).subList(3, commands.length);
	}

	@Override
	public void run() {
		int numberOfNodes = Application.getInstance().group.list.size();
		int index = 0;

		for (String sdfsSourceFile : sdfsSourceFiles) {
			// Divide tasks evenly across nodes
			Node nodeToRunOn = Application.getInstance().group.list.get(index
					% numberOfNodes);
			
			MapleTask mapleTask = new MapleTask(executable,
					intermediateFilePrefix, sdfsSourceFile, nodeToRunOn);

			mapleTask.start();
			index++;
		}
	}
}
