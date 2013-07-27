package org.uiuc.cs.distributed.grep;

public class MapleJuiceNode {

	public String mapleExecutableSdfsKey;
	public String intermediateFilePrefix;
	public String sdfsSourceFile;
	public String mapleExe = "maple.jar";
	private boolean isComplete;
	
	public MapleJuiceNode(String _mapleExecutableSdfsKey, String _intermediateFilePrefix,
							String _sdfsSourceFile, String _MapleExe)
	{
		this.mapleExecutableSdfsKey = _mapleExecutableSdfsKey;
		this.intermediateFilePrefix = _intermediateFilePrefix;
		this.sdfsSourceFile = _sdfsSourceFile;
		this.mapleExe = _MapleExe;
		this.isComplete = false;
	}
	
	public synchronized void setComplete(boolean value)
	{
		this.isComplete = value;
	}
	
	public synchronized boolean isComplete()
	{
		return this.isComplete;
	}
			
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
