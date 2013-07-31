package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

import java.util.Properties;

import edu.usc.pgroup.cloud.iaassim.ProcessingElement;

public abstract class DataSource implements Cloneable {


	DataflowProcessingElement sinkPE;
	boolean started;
	public DataSource(DataflowProcessingElement pe)
	{
		this.sinkPE = pe;
		started = false;
	}
	
	public boolean start(float time)
	{
		started = true;
		return true;
	}
	
	//returns the number of messages generated since "last call"
	public abstract int getMessages(float time) throws Exception;

	public DataflowProcessingElement getAttactedPE() {
		return sinkPE;
	}

	public abstract float distributeData(int oldNumInstance, int newNumInstance);

	public abstract DataSource clone(DataflowProcessingElement newPe);

	public boolean started() {
		return started;
	}
	
}
