package edu.usc.pgroup.cloud.floesimulator.appmodel;

import edu.usc.pgroup.cloud.iaassim.ExecutionTrigger;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;

public class Alternate extends DataflowProcessingElement{

	float value;
	
	public Alternate(int id, float standardCoreSecondPerMessage, int numCores,
			boolean isContinuous, ExecutionTrigger trigger, float selectivity, float value) {
		super(id, standardCoreSecondPerMessage, numCores, isContinuous, trigger,
				selectivity);
		
		this.value = value;
	}

	public float getValue() {
		return value;
	}

}
