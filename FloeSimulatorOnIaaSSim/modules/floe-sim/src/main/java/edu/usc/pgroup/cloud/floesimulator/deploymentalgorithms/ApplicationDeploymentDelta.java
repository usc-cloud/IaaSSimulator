package edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms;

import java.util.ArrayList;
import java.util.List;

import edu.usc.pgroup.cloud.floesimulator.appmodel.ConcretePellet;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;

public class ApplicationDeploymentDelta {

	private List<DataflowProcessingElement> toAddList = new ArrayList<>();
	private List<DataflowProcessingElement> toRemoveList = new ArrayList<>();
	private List<ConcretePellet> toChangeAlternateList = new ArrayList<>();

	public void setToAdd(List<DataflowProcessingElement> toAddList) {
		this.toAddList = toAddList;
	}

	public List<DataflowProcessingElement> getToAddList() {
		return toAddList;
	}

	public void setToRemoveList(List<DataflowProcessingElement> toRemoveList) {
		this.toRemoveList = toRemoveList;
	}
	
	public List<DataflowProcessingElement> getToRemoveList() {
		return toRemoveList;
	}

	public void setToChangeAlternatesList(List<ConcretePellet> toChangeAlternate) {
		this.toChangeAlternateList = toChangeAlternate;
	}
	
	public List<ConcretePellet> getToChangeAlternateList() {
		return toChangeAlternateList;
	}
}
