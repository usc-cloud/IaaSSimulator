package edu.usc.pgroup.cloud.floesimulator.appmodel;

public class PelletEdge {
	private Pellet srcNode;
	private Pellet destNode;
	
	public PelletEdge(Pellet s,Pellet dest)
	{
		srcNode =  s;
		destNode  = dest;
	}
	
	public int getSrcNodeId()
	{
		return srcNode.getId();
	}
	
	public int getDestNodeId()
	{
		return destNode.getId();
	}

	public Pellet getSourceNode() {
		return srcNode;
	}

	public Pellet getDestNode() {
		return destNode;
	}
}

