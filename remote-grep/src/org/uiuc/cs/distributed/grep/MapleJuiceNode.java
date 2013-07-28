package org.uiuc.cs.distributed.grep;

public class MapleJuiceNode {

	public String executableSdfsKey;
	public String intermediateFilePrefix;
	public String sdfsSourceFile;
	
	public MapleJuiceNode(String _executableSdfsKey, String _intermediateFilePrefix,
							String _sdfsSourceFile)
	{
		this.executableSdfsKey = _executableSdfsKey;
		this.intermediateFilePrefix = _intermediateFilePrefix;
		this.sdfsSourceFile = _sdfsSourceFile;
	}
}
