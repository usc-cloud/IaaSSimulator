package edu.usc.pgroup.cloud.iaassim.pescheduler;

import java.util.ArrayList;
import java.util.List;

import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.utils.Log;

public class SingleExecutionScheduler extends PEScheduler{

	private boolean isFinishedPEs = false;
	private List<ProcessingElement> finishedPEs = new ArrayList<>();
	
	@Override
	public boolean PESubmit(ProcessingElement pe, float currentTime) {
		
		processPEs(currentTime);
		
		if(getVm().isCoreAvailable() && !peMap.containsKey(pe))
		{
			getVm().allocateCore(pe.getNodeId(), pe.getInstanceId());
			peMap.put(pe,currentTime);
			Log.printLine("Starting pe: " + pe.getInstanceId() + " at time:" + currentTime);
			return true;
		}
		return false;
	}


	@Override
	public boolean processPEs(float currentTime) {
		boolean pesProcessed = false;
		List<ProcessingElement> toRemove = new ArrayList<>();
		for(ProcessingElement pe: peMap.keySet())
		{
			float previousTime = peMap.get(pe);
			peMap.put(pe,currentTime);
			float duration = currentTime - previousTime;
			if(duration <= 0) continue;
			
			float coreCoeff = vm.getCurrentCoreCoeff();
			float processed = coreCoeff * duration;
			
			pe.setRemainingCoreSeconds(Math.max(pe.getRemainingCoreSeconds() - processed,0));
			
			Log.printLine("Processed pe: " + pe.getInstanceId() + "=> " + processed + " now remaining: " + pe.getRemainingCoreSeconds());
			
			if(pe.getRemainingCoreSeconds() <= 0)
			{
				if(pe.isContinuous())
				{
					pe.reset();
					//TODO: Send event for data transfer
				}
				else
				{
					finishedPEs.add(pe);			
					toRemove.add(pe);
				}
					
			}
			pesProcessed = true;
		}
		
		for(ProcessingElement pe: toRemove)
		{
			peMap.remove(pe);
		}
		return pesProcessed;
	}
	
	@Override
	public boolean isFinishedCloudlets() {
		return !finishedPEs.isEmpty();
	}

	@Override
	public ProcessingElement getNextFinishedPE() {
		return finishedPEs.remove(0);
	}


	@Override
	public boolean isDataAvailableForTransfer() {
		return false;
	}


	@Override
	public void scheduleDataTransfer(SimulationEntity owner) {
		return;
	}


	@Override
	public void perfLog() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public boolean PERemove(ProcessingElement pe) {
		//TODO.. 
		return false;
	}
}
