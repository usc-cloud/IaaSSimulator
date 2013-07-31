package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.network.NetworkTopology;

public class Edge {

	List<DataflowProcessingElement> downstreamPEs;
	int edgeId;
	int downPeId;
	
	int currentDownStreamPEIndex;
	
	Map<DataflowProcessingElement,Integer> outQueuePerPE;
	private ProcessingElement srcPe;
	
	
	public Edge(int id, int downPeId,ProcessingElement srcPe) {
		this.edgeId = id;
		this.srcPe = srcPe;
		this.downPeId = downPeId;
		downstreamPEs = new ArrayList<>();
		currentDownStreamPEIndex = 0;
		outQueuePerPE = new HashMap<>();
	}

	public void addFiber(DataflowProcessingElement destPe) {
		downstreamPEs.add(destPe);
	}

	public void removeFiber(DataflowProcessingElement dpe) {
		downstreamPEs.remove(dpe);
	}

	public void putMessagesInOutQueue(float n) {
		for(int i = 0; i < n; i++)
		{
			//TODO: MAKE THIS WEIGHTED RR (Active load balancing)
			DataflowProcessingElement pe = selectDownStreamPEWeighted(downstreamPEs); 
			
			
			//downstreamPEs.get((currentDownStreamPEIndex)%downstreamPEs.size());
			
			Integer currentInQueue = outQueuePerPE.get(pe);

			//put in the out queue
			outQueuePerPE.put(pe, currentInQueue == null ? 1 : currentInQueue + 1);
			
			currentDownStreamPEIndex++;
		}
	}
	
	private DataflowProcessingElement selectDownStreamPEWeighted(
			List<DataflowProcessingElement> downstreamPEs) {
		long minQLength = Long.MAX_VALUE;
		DataflowProcessingElement bestPE = null; 
		for(DataflowProcessingElement pe: downstreamPEs)
		{
			long queueLength = pe.getInQLength();
			
			/*if(pe.getNodeId() == 2)
			{
				System.out.println(IaaSSimulator.getClock() + ", " + pe.getInstanceId() + ", " + queueLength);
			}*/
			
			if(queueLength <= minQLength)
			{
				minQLength = queueLength;
				bestPE = pe;
			}
		}
		
		
		return bestPE;
	}

	public boolean isDataAvailableForTransfer()
	{
		for(Integer queues: outQueuePerPE.values())
		{
			if(queues > 0) return true;
		}
		
		return false;
	}

	public void scheduleDataTransfer(SimulationEntity owner) {
		for(Entry<DataflowProcessingElement, Integer> queues: outQueuePerPE.entrySet())
		{
			ProcessingElement destPe = queues.getKey();
			int numMessages = queues.getValue();
			
			
			float delay = NetworkTopology.getMessageTransferDelay(srcPe.getVmId(), destPe.getVmId());
			
			//TODO.. check transfer rate.. maybe we do some in parallel.. 
			for(int i = 1; i <= numMessages; i++)
			{			
				NetworkTopology.initiateTransfer(owner,delay*i,srcPe,destPe);				
			}
			
			//All message transfers have been scheduled.
			queues.setValue(0);
		}
	}

	public int getOutQLength() {
		int outQ = 0;
		for(Entry<DataflowProcessingElement, Integer> queues: outQueuePerPE.entrySet())
		{
			outQ += queues.getValue();
		}
		return outQ;
	}
	
	public List<DataflowProcessingElement> getDownstreamPEs() {
		return downstreamPEs;
	}

	public int getDownPeId() {
		return downPeId;
	}
}
