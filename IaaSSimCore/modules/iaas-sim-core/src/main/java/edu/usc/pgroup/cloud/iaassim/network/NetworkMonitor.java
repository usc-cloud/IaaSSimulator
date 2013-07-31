package edu.usc.pgroup.cloud.iaassim.network;

import java.util.List;

import edu.usc.pgroup.cloud.iaassim.IaaSBroker;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.vm.VM;

class NetworkMonitor {
	
	private static float previousTime = -1;
	
	public static float getLatency(int srcVMId,int destVMId, float time)
	{
		//TODO: Get this from monitoring.. 
		if(srcVMId == destVMId) return 0;
		else
		{
			return 0.1f;//.in seconds 0.1s = 100ms
		}
	}
	
	public static float getAvailableBandwidth(int srcVMId, int destVMId, float time)
	{
		//TODO: Get this from monitoring. OR based on probabilities
		
		if(srcVMId == destVMId)
		{
			return 1.0f/8.0f*1000*1000 ;//in KB/s (if on same vm bandwidth is half that of the ram. typical speed 2Gbps = 2/8 GBps = 2/8 * 1000 * 1000 KBPS
		}
		else
		{
			return 1 * 1000 ;//1 MBPS = 1000 KBPS average speed
		}
	}

	public static List<VM> getConnectedVMs() {
		return ((IaaSBroker)(IaaSSimulator.getBroker())).getVMs();
	}
}
