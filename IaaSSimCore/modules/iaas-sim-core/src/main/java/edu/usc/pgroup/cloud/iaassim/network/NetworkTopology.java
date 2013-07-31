package edu.usc.pgroup.cloud.iaassim.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.IaaSBroker;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventTag;
import edu.usc.pgroup.cloud.iaassim.vm.VM;

public class NetworkTopology {

	private static Map<Integer,Map<Integer, Float>> latency = new HashMap<>();
	private static Map<Integer,Map<Integer, Float>> bandwidth = new HashMap<>();
	private static float defaultLatency = 0.2f;
	private static float defaultBandwidth = 1000.0f;

	public static float getMessageTransferDelay(int srcVMId, int destVMId) {
		return getMessageTransferDelay(srcVMId, destVMId, 500);
	}

	public static float getMessageTransferDelay(int srcVMId, int destVMId, float msgSize /*in KB*/) {
		
		if(srcVMId == -1 || destVMId == -1) 
		{
			System.err.println("Not all pellets have been allocated");
			System.exit(1);
		}
		
		if(srcVMId > VM.PERFECTLY_STANDARD_VM) return 0;
		
		if(srcVMId == destVMId) 
			return 0.1f;
		else
		{
			float time = getLatency(srcVMId,destVMId) + msgSize/getAvailableBandwidth(srcVMId, destVMId)/1000.0f;
			return time;
		}
	}

	public static void initiateTransfer(SimulationEntity owner, float delay, ProcessingElement srcPe, ProcessingElement destPe){
		Object data[] = new Object[3];
		data[0] = destPe;
		data[1] = 1; //? check
		owner.schedule(owner.getId(), delay, SimulationEventTag.INCOMING_DATA, data);
		//TODO: update available bandwidth
	}

	public static List<VM> getConnectedVMs(VM vm) {
		return NetworkMonitor.getConnectedVMs();
	}

	public static float getLatency(int srcVMId, int destVMId)
	{
		if(latency.get(srcVMId) == null || latency.get(srcVMId).get(destVMId) == null)
		{
			//System.out.println(srcVMId + " " + destVMId + " " + IaaSSimulator.getClock());
			return defaultLatency ;
		}
		return latency.get(srcVMId).get(destVMId);
	}

	public static float getAvailableBandwidth(int srcVMId, int destVMId)
	{
		if(latency.get(srcVMId) == null || latency.get(srcVMId).get(destVMId) == null)
		{
			//System.out.println(srcVMId + " " + destVMId + " " + IaaSSimulator.getClock());
			return defaultBandwidth ;
		}
		return bandwidth.get(srcVMId).get(destVMId);
	}

	public static void updateTopologyStats(List<VM> vmList) {
		List<VM> allVMs = vmList;
		for(VM vm1: allVMs)
		{
			if(!latency.containsKey(vm1.getId()))
				latency.put(vm1.getId(), new HashMap<Integer,Float>());
			
			if(!bandwidth.containsKey(vm1.getId()))
				bandwidth.put(vm1.getId(), new HashMap<Integer,Float>());
			
			
			Map<Integer, Float> latencyMap = latency.get(vm1.getId());
			Map<Integer, Float> bandwidthMap = bandwidth.get(vm1.getId());
					
			
			latencyMap.clear();
			bandwidthMap.clear();
			
			for(VM vm2: allVMs)
			{
				latencyMap.put(vm2.getId(), NetworkMonitor.getLatency(vm1.getId(), vm2.getId(), IaaSSimulator.getClock()));
				bandwidthMap.put(vm2.getId(), NetworkMonitor.getAvailableBandwidth(vm1.getId(), vm2.getId(), IaaSSimulator.getClock()));
			}
		}
		
	}
}
