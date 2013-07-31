package edu.usc.pgroup.cloud.iaassim;

import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.vm.VM;

public abstract class ProcessingElement implements Cloneable{

	/**
	 * Core seconds required to process a sing message. 
	 * (same as time required for one execution of PE on a standard core) 
	 */
	protected float peStandardCoreSecondPerMessage;
	
	protected float remainingCoreSeconds;
	
	/**
	 * Number of cores required per intance of the PE
	 */
	protected int numberOfCores; 
	
	/**
	 * PE id (instance id)
	 */
	protected final int instanceId;
	
	/**
	 * PE Node id
	 */
	protected final int nodeId;
	
	/**
	 * Execution states
	 */
	protected int state;
	
	/**
	 * All States
	 */
	public static final int WAITING = 1;
	public static final int READY = 2;
	public static final int EXECUTING = 3;
	public static final int SUCCESS = 4;
	public static final int FAILED = 5;
	
	/**
	 * Id of the VM where this PE is being executed
	 */
	protected int vmId;

	protected int ownerId;
	
	protected boolean isContinuous;

	protected ExecutionTrigger trigger;

	private int messagesProcessedCurrentCycle;

	private VM vm;
	
	public ProcessingElement(
			int id,
			float standardCoreSecondPerMessage,
			int numCores,
			boolean isContinuous,
			ExecutionTrigger trigger		
			) {		
		this(id,id, standardCoreSecondPerMessage,numCores,isContinuous,trigger);
	}
	
	public ProcessingElement(
			int id,
			int nodeId,
			float standardCoreSecondPerMessage,
			int numCores,
			boolean isContinuous,
			ExecutionTrigger trigger
			) {		
		
		this.instanceId = id;
		this.nodeId = nodeId;
		this.peStandardCoreSecondPerMessage = standardCoreSecondPerMessage;
		this.remainingCoreSeconds = this.peStandardCoreSecondPerMessage;
		this.numberOfCores = numCores;
		this.vmId = -1;
		this.state = WAITING;
		this.isContinuous = isContinuous;
		this.trigger = trigger;
		messagesProcessedCurrentCycle = 0;
	}
	
	public int getVmId() {
		return vmId;
	}
	
	public void setVm(VM vm) {
		this.vmId = vm.getId();
		this.vm = vm;
	}
	
	public VM getVm() {
		return vm;
	}
	
	public int getInstanceId() {
		return instanceId;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public int getState() {
		return state;
	}

	public void setOwnerId(int ownerId) {
		this.ownerId = ownerId;
	}
	
	public int getOwnerId() {
		return ownerId;
	}

	public boolean isFinished() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public float getPeStandardCoreSecondPerMessage() {
		return peStandardCoreSecondPerMessage;
	}
	
	public float getRemainingCoreSeconds() {
		return remainingCoreSeconds;
	}
	
	public void setRemainingCoreSeconds(float remainingCoreSeconds) {
		this.remainingCoreSeconds = remainingCoreSeconds;
	}

	public boolean isContinuous() {
		return isContinuous;
	}

	public ExecutionTrigger getTrigger() {
		return trigger;
	}
	
	public void reset() {		
		if(resetState())
			state = WAITING;
		remainingCoreSeconds = peStandardCoreSecondPerMessage;
	}
	
	public abstract boolean resetState();

	public int getMessagesProcessedCurrentCycle() {
		return messagesProcessedCurrentCycle;
	}

	public void setMessagesProcessedCurrentCycle(int m) {
		messagesProcessedCurrentCycle = m;
	}

	public abstract boolean isDataAvailableForTransfer();

	public abstract void scheduleDataTransfer(SimulationEntity owner);

	public abstract void enqueueMessage();
	public abstract void enqueueMessage(int n);
	
	public abstract void dequeueMessage();
}
