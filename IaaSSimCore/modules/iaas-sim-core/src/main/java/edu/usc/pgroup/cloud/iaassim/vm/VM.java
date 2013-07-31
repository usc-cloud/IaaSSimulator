package edu.usc.pgroup.cloud.iaassim.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.network.NetworkTopology;
import edu.usc.pgroup.cloud.iaassim.pescheduler.PEScheduler;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.ContinuousDistribution;
import edu.usc.pgroup.cloud.iaassim.utils.LogEntity;
import edu.usc.pgroup.cloud.iaassim.utils.PerfLog;


/**
 * 
 * @author kumbhare
 * IaaSSim v0.1
 * 
 * Represents a Virtual Machine Instance
 * 
 */

public class VM {

	public static final int TURNEDOFF = 0;
	public static final int PROVISIONED = 1;
	public static final int STARTING = 2;
	public static final int RUNNING = 3;
	public static final int SHUTTING_DOWN = 4;
	public static final int PERFECTLY_STANDARD_VM = 999991;
	

	/**
	 * VM State (TurnedOff, Provisioned, Starting, Running)
	 */
	private int state;
	
	/**
	 * VM instance id
	 */
	private int id;

	/**
	 * VM Class
	 */
	private VMClass vmClass;

	/**
	 * Following variables are defined to track the "infrastructure variability"
	 */
	
	/**
	 * Current Million instrstuction per second per core. (core speed)
	 */
	private float currentMips;
	
	/**
	 * core coefficient. Compared to the "standard" vm. (this.mips/standard.mips)
	 */
	private float currentCoreCoeff;
	
	/**
	 * Current available cores
	 */
	private int utilizedCores;
	
	
	/**
	 * current available Disk size in GB. We are assuming disk speed is same across the cluster. Ignore that for now.
	 */
	private float utilizedDiskSpace;
	
	/**
	 * current Utilized bandwidth
	 */
	private float utilizedNwBandwidth;

	private int iaasBrokerId;
	private PEScheduler peScheduler;
	private VMMonitor monitor;

	float startTime;
	float shutdownTime;
	private Map<Integer,Double> allocatedPellets;
	
	public VM(int vmId, int iaasBrokerId, VMClass vmClass, PEScheduler peScheduler) {
		this.id = vmId;
		this.iaasBrokerId = iaasBrokerId;
		this.vmClass = vmClass;
		this.state = TURNEDOFF;
		this.peScheduler = peScheduler;
		this.monitor = new VMMonitor();
		this.allocatedPellets = new HashMap<>();
		startTime = -1;
		shutdownTime = -1;
	}


	public VM(int vmId, int iaasBrokerId, VMClass vmClass, PEScheduler peScheduler,
			ContinuousDistribution dist) {
		this(vmId,iaasBrokerId,vmClass,peScheduler);
		this.monitor.setVariationDistribution(dist);
	}


	public int getId() {
		return id;
	}

	public VMClass getVMClass() {
		return vmClass;
	}


	public void setState(int state) {
		this.state = state;
	}


	public PEScheduler getPEScheduler() {
		return peScheduler;
	}


	public boolean isBeingProvisioned() {
		if(state == PROVISIONED || state == STARTING)
			return true;
		return false;
	}


	public boolean isCoreAvailable() {
		return (vmClass.getCoreCount() - utilizedCores) > 0 ? true : false;
	}

	public int getAvailableCores()
	{
		return (vmClass.getCoreCount() - utilizedCores);
	}

	/*public void allocateCore() {
		utilizedCores++;
	}*/
	
	public void allocateCore(Integer pelletId, Integer instanceId) {		
		allocatedPellets.put(pelletId, (allocatedPellets.containsKey(pelletId)?allocatedPellets.get(pelletId):0) + vmClass.getCoreCoeff() );
		utilizedCores++;
	}
	
	public void freeCore(Integer pelletId, Integer instanceId) {
		allocatedPellets.put(pelletId, allocatedPellets.get(pelletId)-vmClass.getCoreCoeff());
		utilizedCores--;
	}

	public Map<Integer, Double> getAllocatedPellets() {
		return allocatedPellets;
	}
	
	public void updateVMPerf(float time)
	{
		this.currentCoreCoeff = monitor.getExpectedCoreCoeff(this, time);
	}
	
	public void perfLog() {
		
		//print vm performance
		PerfLog.printCSV(LogEntity.VM,getId(),getStartTime(),getShutdownTime(),getVMClass().getCoreCount()-utilizedCores,getVMClass().getCoreCount(),this.currentCoreCoeff);
		
		//print nw performance peer-peer
		List<VM> vms = NetworkTopology.getConnectedVMs(this);
		for(VM vm: vms)
		{
			PerfLog.printCSV(LogEntity.NW,getId(),vm.getId(),NetworkTopology.getLatency(getId(), vm.getId()),NetworkTopology.getAvailableBandwidth(getId(), vm.getId()));	
		}
		
		getPEScheduler().perfLog();		
	}


	public float getCurrentCoreCoeff() {
		return this.currentCoreCoeff;
	}


	public void setStartTime(float time) {
		this.startTime = time;
	}
	
	public float getStartTime() {
		return startTime;
	}
	
	public float getShutdownTime() {
		return shutdownTime;
	}

	public void setShutdownTime(float shutdownTime) {
		this.shutdownTime = shutdownTime;
	}

	public boolean isRunning() {
		return state == VM.RUNNING;
	}


	public int getState() {
		return state;
	}


	
}
