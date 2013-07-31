package edu.usc.pgroup.cloud.iaassim.pescheduler;

import java.util.ArrayList;
import java.util.List;

import edu.usc.pgroup.cloud.iaassim.ExecutionTrigger;
import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.Edge;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.utils.Log;
import edu.usc.pgroup.cloud.iaassim.utils.Utils;

public class DataflowExecutionScheduler extends PEScheduler{

	private boolean isFinishedPEs = false;
	private List<DataflowProcessingElement> finishedPEs = new ArrayList<>();
	
	
	@Override
	public boolean PESubmit(ProcessingElement pe, float currentTime) {
		
		processPEs(currentTime);
		
		if(getVm().isCoreAvailable() && !peMap.containsKey((DataflowProcessingElement)pe))
		{
			getVm().allocateCore(pe.getNodeId(), pe.getInstanceId());
			peMap.put((DataflowProcessingElement)pe,currentTime);
			Log.printLine("Starting pe: " + pe.getInstanceId() + " at time:" + currentTime);
			return true;
		}
		return false;
	}

	@Override
	public boolean PERemove(ProcessingElement pe) {
		if(peMap.containsKey((DataflowProcessingElement)pe))
		{
			peMap.remove((DataflowProcessingElement)pe);
			getVm().freeCore(pe.getNodeId(),((DataflowProcessingElement)pe).getInstanceId());
			return true;
		}
		return false;
	}

	
	@Override
	public boolean processPEs(float currentTime) {
		boolean pesProcessed = false;
		List<ProcessingElement> toRemove = new ArrayList<>();
		for(ProcessingElement pe_temp: peMap.keySet())
		{
			DataflowProcessingElement pe = (DataflowProcessingElement) pe_temp; 
			float previousTime = peMap.get(pe);
			peMap.put(pe,currentTime);
			float duration = (float)Utils.roundDoubleValue((double)(currentTime - previousTime));
			if(duration <= 0) continue;
			
			float coreCoeff = vm.getCurrentCoreCoeff();
			float processed = coreCoeff * duration;
			
			
			if(pe.getTrigger() == ExecutionTrigger.Message && pe.getState() == ProcessingElement.WAITING) continue;
			
			pe.setRemainingCoreSeconds(Math.max(pe.getRemainingCoreSeconds() - processed,0));
			
			
			Log.printLine("Processed pe: " + pe.getInstanceId() + "=> " + processed + " now remaining: " + pe.getRemainingCoreSeconds());
			
			if(pe.getRemainingCoreSeconds() <= 0)
			{
				if(pe.getTrigger() == ExecutionTrigger.Message) pe.dequeueMessage();
				
				pe.setMessagesProcessedCurrentCycle(pe.getMessagesProcessedCurrentCycle()+1);
				
				
				if(pe.getMessagesProcessedCurrentCycle() == pe.getM())
				{
					//Send out n messages on all edges and round robin on fibers
					pe.putMessagesInOutQueue(pe.getN());
					pe.setMessagesProcessedCurrentCycle(0);
				}
				
				if(pe.isContinuous())
				{
					pe.reset();
				}
				else
				{
					finishedPEs.add((DataflowProcessingElement)pe);			
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
		for(ProcessingElement pe: peMap.keySet())
		{
			if(pe.isDataAvailableForTransfer()) return true;
		}
		return false;
	}

	@Override
	public void scheduleDataTransfer(SimulationEntity owner) {
		for(ProcessingElement pe: peMap.keySet())
		{
			if(pe.isDataAvailableForTransfer())
			{
				pe.scheduleDataTransfer(owner);
			}
		}
		
	}

	@Override
	public void perfLog() {
		for(ProcessingElement pe: peMap.keySet())
		{
			DataflowProcessingElement dpe = (DataflowProcessingElement) pe;
			dpe.perfLog();
		}
	}
	
}
