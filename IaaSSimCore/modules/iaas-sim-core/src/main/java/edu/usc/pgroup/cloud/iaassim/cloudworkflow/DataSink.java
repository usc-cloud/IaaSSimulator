package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

public abstract class DataSink implements Cloneable{

		
		DataflowProcessingElement sourcePE;
		public DataSink(DataflowProcessingElement pe)
		{			
			this.sourcePE = pe;
		}
		
		public abstract boolean start(float time);
		
		//returns the number of messages generated since "last call"
		public abstract int getMessages(float time) throws Exception;

		public DataflowProcessingElement getAttactedPE() {
			return sourcePE;
		}
}
