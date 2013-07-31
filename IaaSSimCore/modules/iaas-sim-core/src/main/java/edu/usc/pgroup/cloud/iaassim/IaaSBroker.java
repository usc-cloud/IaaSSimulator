package edu.usc.pgroup.cloud.iaassim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataSource;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventTag;
import edu.usc.pgroup.cloud.iaassim.pescheduler.PEScheduler;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.ContinuousDistribution;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.SmoothWalk;
import edu.usc.pgroup.cloud.iaassim.utils.Constants;
import edu.usc.pgroup.cloud.iaassim.utils.Log;
import edu.usc.pgroup.cloud.iaassim.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;

public class IaaSBroker extends SimulationEntity {

	private int vmCount;

	//Only contains pending vm requests. Once the vm is created, it is moved to the createdVMs list
	private List<VM> pendingVMs;

	private List<VM> submittedVMs;
	
	//Only contains currently running VMs. Once the VMs is shutdown, it is moved to destroyedVMs list
	private List<VM> runningVMs;
	
	//to shutdown vms
	private List<VM> shuttingDownVMs;
	
	//Contains only shutdown VMs..
	private List<VM> destroyedVMs;

	private List<ProcessingElement> pendingPEs;

	private List<ProcessingElement> submittedPEs;
	
	private List<ProcessingElement> runningPEs;
	
	private List<VM> shutDownSubmittedVMs;
	
	private Map<Integer,Integer> vmToCloudMap;

	

	private int PESubmitAckCount;

	private List<DataSource> dataSources;

	private boolean initialSubmission;

	private List<ProcessingElement> toRemovePEs;

	

	
	
	public IaaSBroker(String name) {
		super(name);
		vmCount = 0;
		PESubmitAckCount = 0;
		
		vmToCloudMap = new HashMap<>();
		
		pendingVMs = new ArrayList<>();
		submittedVMs = new ArrayList<>();
		runningVMs = new ArrayList<>();
		shuttingDownVMs = new ArrayList<>();
		shutDownSubmittedVMs = new ArrayList<>();
		destroyedVMs = new ArrayList<>();
		
		pendingPEs = new ArrayList<>();
		submittedPEs = new ArrayList<>();		
		runningPEs = new ArrayList<>();
		toRemovePEs = new ArrayList<>();
		
		dataSources = new ArrayList<>();
		
		
		initialSubmission = true;
		//nodeToPEMap = new HashMap<>();
	}

	@Override
	public void startEntity() {
		Log.printLine(getName() + " is starting...");
		schedule(getId(), 0, SimulationEventTag.CLOUD_CHARACTERISTICS_REQ);		
		// schedule(getId(), 0, SimulationEventTag.VM_CREATE_REQUEST_WITH_ACK);
	}

	@Override
	public void processEvent(SimulationEvent ev) {
		Log.printLine(getName() + " Event process " + ev.getEventTag().toString());
		switch (ev.getEventTag()) {
		case CLOUD_CHARACTERISTICS_REQ:			
			processCloudCharacteristicsReq(ev);
			break;
		case CLOUD_CHARACTERISTICS_RES:
			processCloudCharacteristicsRes(ev);
			break;			
			
		// VM Creation response
		case VM_CREATE_RESPONSE_ACK:
			processVmCreateResponse(ev);
			break;
			
		case VM_SHUTDOWN_RESPONSE_ACK:
			processVmShutdownResponse(ev);
			break;
			
		case RUNTIME_PE_CHANGE_REQ:
			processRuntimePEChange(ev);
			break;
			
		case RUNTIME_VM_CHANGE_REQ:
			processRuntimeVmChange(ev);
			break;	
			
		case PE_SUBMIT_ACK:
			processPESubmitResponse(ev);
			break;
		
		case PE_REMOVE_ACK:
			processPERemoveResponse(ev);
			break;
			
		case SCALE_OUT:
			processScaleOutRequest(ev);
			break;
		case SCALE_IN:
			processScaleInRequest(ev);
			break;
		case PE_RETURN:
			break;
		default:

		}
	}


	private void processVmShutdownResponse(SimulationEvent ev) {
		int[] data = (int[])(ev.getEventData());
		
		int cloudId = data[0];
		int vmId = data[1];
		
		
		VM vm = Utils.getVMById(shutDownSubmittedVMs, vmId);
		
		runningVMs.remove(vm);
		shutDownSubmittedVMs.remove(vm);
		destroyedVMs.add(vm);
		vm.setState(VM.TURNEDOFF);
		
		Log.printLine(IaaSSimulator.getClock() + ": " + getName() + ": VM #" + vmId
					+ " has been turned off in cloud #" + cloudId);
		
		//incrementVmsAcks();

		// all the requested VMs have been created
		if (shutDownSubmittedVMs.size() == 0) {
			Log.printLine(IaaSSimulator.getClock() + ": submitting cloudlets" );

			//reset requested vms
			shutDownSubmittedVMs.clear();
		}
	}

	private void processRuntimeVmChange(SimulationEvent ev) {
		//TODO.. check for launch/destroy request..
		Integer cloudId = (Integer)ev.getEventData();
		launchVmsInCloud(cloudId);
		shutDownVms(cloudId);
	}

	private void shutDownVms(Integer cloudId) {
		for (VM vm : shuttingDownVMs) {			
			send(cloudId,0,SimulationEventTag.VM_SHUTDOWN_REQUEST_WITH_ACK, vm);
			shutDownSubmittedVMs.add(vm);
		}
		shuttingDownVMs.removeAll(shutDownSubmittedVMs);
	}

	private void processRuntimePEChange(SimulationEvent ev) {
		//TODO: set ev appropriately.
		submitProcessingElements();
	}

	private void processScaleInRequest(SimulationEvent ev) {
		// TODO Auto-generated method stub
		
	}

	private void processScaleOutRequest(SimulationEvent ev) {
		int cloudId = (Integer)ev.getEventData();
		launchVmsInCloud(cloudId);
	}

	private void processPERemoveResponse(SimulationEvent ev) {

		int[] response = (int[])ev.getEventData();
		
		int cloudId = response[0];
		int peId = response[1];
		int success = response[2];
		
		if(success == 0)
		{
			System.out.println("Current PEMappings are not allowed by the PEScheduler");
			System.exit(0);
		}
		
		Iterator<ProcessingElement> toRemoveIterator = toRemovePEs.iterator();
		while(toRemoveIterator.hasNext())
		{
			synchronized (toRemovePEs) {
				ProcessingElement pe = toRemoveIterator.next();
				if(peId == pe.getInstanceId())
				{
					runningPEs.remove(pe);
					toRemoveIterator.remove();
				}
			}
		}
	}

	
	private void processPESubmitResponse(SimulationEvent ev) {
		PESubmitAckCount++;
		
		int[] response = (int[])ev.getEventData();
		
		int cloudId = response[0];
		int peId = response[1];
		int success = response[2];
		
		if(success == 0)
		{
			System.out.println("Current PEMappings are not allowed by the PEScheduler");
			System.exit(0);
		}
		
		Iterator<ProcessingElement> submittedIterator = submittedPEs.iterator();
		while(submittedIterator.hasNext())
		{
			synchronized (submittedPEs) {
				ProcessingElement pe = submittedIterator.next();
				if(peId == pe.getInstanceId())
				{
					runningPEs.add(pe);
					submittedIterator.remove();
				}
			}
		}
		
		if(submittedPEs.size() == 0)
		{
			for(DataSource s : dataSources)
			{
				if(!s.started())
				{
					s.start(IaaSSimulator.getClock());
					send(cloudId,Constants.TICKLENGTH,SimulationEventTag.DATA_SOURCE_EVENT,s);
				}
			}
			if(initialSubmission)
			{
				send(cloudId, Constants.TICKLENGTH, SimulationEventTag.VM_CLOUD_EVENT, null);
				initialSubmission = false;
			}
			
			submittedPEs.clear();
			PESubmitAckCount = 0;
		}
	}

	private void processCloudCharacteristicsRes(SimulationEvent ev) {
		IaaSCloudCharacteristics characteristics = (IaaSCloudCharacteristics) ev.getEventData();
		//getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

		//Ignoring this condition, since we are assuming only one cloud provider
		//if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
			//setDatacenterRequestedIdsList(new ArrayList<Integer>());
			launchVmsInCloud(ev.getSrcEntity()); //src entity is the IaaSCloud
		//}
	}

	private void processCloudCharacteristicsReq(SimulationEvent ev) {
		for (Integer cloudId : getCloudIdsList()) {
			send(cloudId, 0, SimulationEventTag.CLOUD_CHARACTERISTICS_REQ, getId());
		}
	}

	private List<Integer> getCloudIdsList() {
		return IaaSSimulator.getCloudList();
	}

	private void processVmCreateResponse(SimulationEvent ev) {
		int[] data = (int[])(ev.getEventData());
		
		int cloudId = data[0];
		int vmId = data[1];
		
		vmToCloudMap.put(vmId, cloudId);
		
		VM vm = Utils.getVMById(submittedVMs, vmId);
		vm.setStartTime(IaaSSimulator.getClock());
		runningVMs.add(vm);
		submittedVMs.remove(vm);
		vm.setState(VM.RUNNING);
		
		Log.printLine(IaaSSimulator.getClock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in cloud #" + cloudId);
		
		//incrementVmsAcks();

		// all the requested VMs have been created
		if (submittedVMs.size() == 0) {
			Log.printLine(IaaSSimulator.getClock() + ": submitting cloudlets" );
			submitProcessingElements();
			
			//reset requested vms
			submittedVMs.clear();
		}
	}

	private void submitProcessingElements() {

		for (ProcessingElement pe : pendingPEs) {
			VM vm = null;
			// if user didn't bind this cloudlet and it has not been executed yet
			if (pe.getVmId() == -1) {
				Log.printLine("VM has not be assigned to PE:"+pe.getInstanceId());
				System.exit(1);
			} else { // submit to the specific vm
				vm = Utils.getVMById(runningVMs, pe.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(getName() + ": Postponing execution of PE "
							+ pe.getInstanceId() + ": bount VM not available");
					continue;
				}
			}

			Log.printLine(getName() + ": Sending cloudlet "
					+ pe.getInstanceId() + " to VM #" + vm.getId());
			pe.setVm(vm);
			send(vmToCloudMap.get(vm.getId()), 0, SimulationEventTag.CLOUDLET_SUBMIT, pe);
			
			submittedPEs.add(pe);
		}

		for (ProcessingElement pe : toRemovePEs) {

			Log.printLine(getName() + ": Sending cloudlet remove request for "
					+ pe.getInstanceId() + " to VM #" + pe.getVmId());
			
			send(vmToCloudMap.get(pe.getVmId()), 0, SimulationEventTag.CLOUDLET_REMOVE, pe);
		}		
		
		// remove submitted PEs from waiting list		
		pendingPEs.removeAll(submittedPEs);
		
	}

	private List<ProcessingElement> getRunningPEList() {
		return runningPEs;
	}


	private void launchVmsInCloud(int cloudId) {
		for (VM vm : pendingVMs) {			
			send(cloudId,0,SimulationEventTag.VM_CREATE_REQUEST_WITH_ACK, vm);
			submittedVMs.add(vm);
		}
		pendingVMs.removeAll(submittedVMs);
	}

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub

	}

	public VM submitVMRequest(VMClass vmClass, PEScheduler peScheduler, ContinuousDistribution dist) {
		VM vm = new VM(vmCount++,getId(),vmClass, peScheduler, dist);
		peScheduler.setVM(vm);
		vm.setState(VM.PROVISIONED);
		pendingVMs.add(vm);
		return vm;
	}

	
	public void submitVMShutdownRequest(VM vm) {
		vm.setState(VM.SHUTTING_DOWN);
		shuttingDownVMs.add(vm);
	}
	
//	public VM submitVMRequest(int vmId, VMClass vmClass, PEScheduler peScheduler, boolean noVariations) {
//		VM vm = new VM(vmId,getId(),vmClass, peScheduler, noVariations);
//		peScheduler.setVM(vm);
//		vm.setState(VM.PROVISIONED);
//		requestedVMs.add(vm);
//		return vm;
//	}
	
	
	
	public void submitPE(ProcessingElement pe, VM vm) {
		pe.setVm(vm);
		pe.setOwnerId(getId());
		pendingPEs.add(pe);
	}


	public void removePE(DataflowProcessingElement pe) {
		toRemovePEs.add(pe);
	}
	
	public List<VM> getVMs() {
		return runningVMs;
	}

	public void submitDataSource(DataSource source) {
		source.getAttactedPE().setDataSource(source);
		this.dataSources.add(source);
	}

	public void removeDataSource(DataSource dataSource) {
		this.dataSources.remove(dataSource);
	}
	
	public Map<Integer, Double> getInputDataRates() {
		Map<Integer, Double> inputDataRatesPerNode = new HashMap<>();
		for(DataSource source: dataSources)
		{
			double d;
			if(!inputDataRatesPerNode.containsKey(source.getAttactedPE().getNodeId()))
			{
				d = 0;
			}
			else
			{
				d = inputDataRatesPerNode.get(source.getAttactedPE().getNodeId());
			}
			
			d += source.getAttactedPE().getDataRate(IaaSSimulator.getClock());
			inputDataRatesPerNode.put(source.getAttactedPE().getNodeId(), d);
		}
		return inputDataRatesPerNode;
	}

	
}
