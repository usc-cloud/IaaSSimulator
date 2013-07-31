package edu.usc.pgroup.cloud.iaassim.pescheduler;

import java.util.HashMap;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.vm.VM;

/**
 * 
 * @author kumbhare
 *
 * This is one per VM.. it maintains a list of all PEs running on this VM. 
 */
public abstract class PEScheduler {
	
	/**
	 * The VM it is managing
	 */
	protected VM vm;
	
	protected Map<ProcessingElement,Float> peMap;
	
	public PEScheduler() {	
		peMap = new HashMap<>();
	}
	
	public abstract boolean PESubmit(ProcessingElement cl, float currentTime);	

	//return true if there are pes to process otherwise return false;
	public abstract boolean processPEs(float currentTime);
	
	public abstract boolean isFinishedCloudlets();

	public abstract ProcessingElement getNextFinishedPE();
	
	public abstract boolean isDataAvailableForTransfer();
	
	public abstract void scheduleDataTransfer(SimulationEntity owner);
	
	public void setVM(VM vm) {
		this.vm = vm;
	}
	
	public VM getVm() {
		return vm;
	}

	public abstract void perfLog();

	public abstract boolean PERemove(ProcessingElement pe);
}
