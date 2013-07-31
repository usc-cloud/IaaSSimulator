package edu.usc.pgroup.cloud.iaassim;

import java.util.ArrayList;
import java.util.List;

import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataSource;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventTag;
import edu.usc.pgroup.cloud.iaassim.network.NetworkTopology;
import edu.usc.pgroup.cloud.iaassim.pescheduler.PEScheduler;
import edu.usc.pgroup.cloud.iaassim.utils.Constants;
import edu.usc.pgroup.cloud.iaassim.utils.Log;
import edu.usc.pgroup.cloud.iaassim.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.vm.VM;

public class IaaSCloud extends SimulationEntity {

	/**
	 * cloud characteristics
	 */
	private IaaSCloudCharacteristics cloudCharacteristics;
	
	/**
	 * List of virtual machines in the cloud. Current version only supports one User.
	 */
	private List<VM> vmList;

	private float lastProcessTime;
	
	public IaaSCloud(String name, IaaSCloudCharacteristics characteristics) {
		super(name);
		setCloudCharacteristics(characteristics);
	
		IaaSSimulator.addCloud(this.getId());
		vmList = new ArrayList<>();		
	}

	
	public void setCloudCharacteristics(
			IaaSCloudCharacteristics cloudCharacteristics) {
		this.cloudCharacteristics = cloudCharacteristics;
	}

	@Override
	public void startEntity() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processEvent(SimulationEvent ev) {
		// TODO Auto-generated method stub
		
		Log.printLine(getName() + " Event process " + ev.getEventTag().toString());
		switch (ev.getEventTag()) {

		case CLOUD_CHARACTERISTICS_REQ:
			processCloudCharacteristicsReq(ev);
			break;
		
		// VM Creation answer
		case VM_CREATE_REQUEST_WITH_ACK:
			processVmCreateRequest(ev,true);
			break;
			
		case VM_SHUTDOWN_REQUEST_WITH_ACK:
			processVMShutdownRequest(ev,true);
			break;
		case CLOUDLET_SUBMIT:
			processPESubmit(ev,true);
			break;
		
		case CLOUDLET_REMOVE:
			processPERemove(ev,true);
			break;
			
			
		case INCOMING_DATA:
			processIncomingData(ev);
			break;
			
		case DATA_SOURCE_EVENT:
			processDataSourceEvent(ev);
			break;
			
		case VM_CLOUD_EVENT:
			
			updateCloudPerformance();			
			updatePEProcessing();
			perfLog();
			checkForAvailableDataTransfer();
			checkPECompletion();
			schedule(getId(), Constants.TICKLENGTH, SimulationEventTag.VM_CLOUD_EVENT);
			break;
		default:

		}
		
	}

	private void processVMShutdownRequest(SimulationEvent ev, boolean ack) {
		VM vm = (VM) ev.getEventData();

		//todo: launch vm here.. 
		
		int result = vm.getId();

		if (ack) {
			int[] data = new int[3];
			data[0] = getId();
			data[1] = result;
			data[2] = 1;
			
			//Since we oly have one user, send it back to the IaaSBroker
			//send(vm.getUserId(), 0.1, CloudSimTags.VM_CREATE_ACK, data);
			send(ev.getSrcEntity(), vm.getVMClass().getExpectedShutdownTime(), SimulationEventTag.VM_SHUTDOWN_RESPONSE_ACK, data);
		}

		
		vm.setState(VM.TURNEDOFF);
		vm.setShutdownTime(IaaSSimulator.getClock());		
	}


	private void perfLog() {
		//LOG EACH VM
		List<? extends VM> list = vmList;
		for (int i = 0; i < list.size(); i++) {
			VM vm = list.get(i);
			if(vm.isRunning())
				vm.perfLog();
		}

	}
	private void updateCloudPerformance() {
		
		for (VM vm : vmList) {
			vm.updateVMPerf(IaaSSimulator.getClock());
		}
		
		NetworkTopology.updateTopologyStats(vmList);
	}


	private void processDataSourceEvent(SimulationEvent ev) {
		DataSource s = (DataSource) ev.getEventData();
		int numMessages = -1;
		try {
			numMessages = s.getMessages(IaaSSimulator.getClock());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		
		ProcessingElement pe = s.getAttactedPE();
		
		if(numMessages > 0)
		{
			Object[] data = new Object[2];
			data[0] = pe;
			data[1] = numMessages;
			
			schedule(getId(),0,SimulationEventTag.INCOMING_DATA,data);
		}
		
		schedule(getId(),Constants.TICKLENGTH,SimulationEventTag.DATA_SOURCE_EVENT,s);
	}
	
	private void processIncomingData(SimulationEvent ev) {
		Object data[] = (Object[]) ev.getEventData();
		
		ProcessingElement destPe = (ProcessingElement)(data[0]);
		Integer numMessages = (Integer)(data[1]);
		destPe.enqueueMessage(numMessages);
	}


	
	private void processPERemove(SimulationEvent ev, boolean ack) {

		try {
			// gets the Cloudlet object
			ProcessingElement pe = (ProcessingElement) ev.getEventData();

			int vmId = pe.getVmId();

			VM vm = Utils.getVMById(vmList, vmId);
			
			PEScheduler scheduler = vm.getPEScheduler();
			
			//TODO..
			boolean submitted = scheduler.PERemove(pe);

			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = pe.getInstanceId();
				data[2] = submitted ? 1 : 0;

				// unique tag = operation tag
				SimulationEventTag tag = SimulationEventTag.PE_REMOVE_ACK;
				send(pe.getOwnerId(),0 , tag, data);
			}

			
			//Start the processing and monitor every 0.1 seconds
			//send(getId(), 0.1, SimulationEventTag.VM_CLOUD_EVENT, null);			
			
		} catch (ClassCastException c) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
			c.printStackTrace();
			System.exit(1);
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
			System.exit(1);
		}

		checkForAvailableDataTransfer();
		
		checkPECompletion();
	}

	
	/**
	 * Processes a Cloudlet submission.
	 * 
	 * @param ev a SimEvent object
	 * @param ack an acknowledgement
	 * @pre ev != null
	 * @post $none
	 */
	protected void processPESubmit(SimulationEvent ev, boolean ack) {
		updatePEProcessing();

		try {
			// gets the Cloudlet object
			ProcessingElement cl = (ProcessingElement) ev.getEventData();

			int vmId = cl.getVmId();

			VM vm = Utils.getVMById(vmList, vmId);
			
			PEScheduler scheduler = vm.getPEScheduler();
			
			//TODO..
			boolean submitted = scheduler.PESubmit(cl, IaaSSimulator.getClock());

			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = cl.getInstanceId();
				data[2] = submitted ? 1 : 0;

				// unique tag = operation tag
				SimulationEventTag tag = SimulationEventTag.PE_SUBMIT_ACK;
				send(cl.getOwnerId(),0 , tag, data);
			}

			
			//Start the processing and monitor every 0.1 seconds
			//send(getId(), 0.1, SimulationEventTag.VM_CLOUD_EVENT, null);			
			
		} catch (ClassCastException c) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
			c.printStackTrace();
			System.exit(1);
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
			System.exit(1);
		}

		checkForAvailableDataTransfer();
		
		checkPECompletion();
	}
	
	private void checkForAvailableDataTransfer() {
		List<? extends VM> list = vmList;
		for (int i = 0; i < list.size(); i++) {
			VM vm = list.get(i);

			while (vm.getPEScheduler().isDataAvailableForTransfer()) {				
				vm.getPEScheduler().scheduleDataTransfer(this);
			}		
		}
	}


	/**
	 * Verifies if some cloudlet inside this PowerDatacenter already finished. If yes, send it to
	 * the User/Broker
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void checkPECompletion() {
		List<? extends VM> list = vmList;
		for (int i = 0; i < list.size(); i++) {
			VM vm = list.get(i);

			while (vm.getPEScheduler().isFinishedCloudlets()) {
				ProcessingElement cl = vm.getPEScheduler().getNextFinishedPE();
				if (cl != null) {
					send(cl.getOwnerId(), 0, SimulationEventTag.PE_RETURN, cl);
				}
			}		
		}
	}
	
	
	/**
	 * Updates processing of each cloudlet running in this Cloud. It is necessary because
	 * Hosts and VirtualMachines are simple objects, not entities. So, they don't receive events and
	 * updating cloudlets inside them must be called from the outside.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void updatePEProcessing() {
		// if some time passed since last processing
		// R: for term is to allow loop at simulation start. Otherwise, one initial
		// simulation step is skipped and schedulers are not properly initialized
		if (IaaSSimulator.getClock() < 0.111 || IaaSSimulator.getClock() > getLastProcessTime()) {
			List<? extends VM> list = vmList;
			
			
			for (int i = 0; i < list.size(); i++) {
				VM vm = list.get(i);
				// inform VMs to update processing
				boolean processed = vm.getPEScheduler().processPEs(IaaSSimulator.getClock());

			}
			setLastProcessTime(IaaSSimulator.getClock());
		}
		
	}
	
	
	
	private void setLastProcessTime(float time) {
		this.lastProcessTime = time;
	}


	private float getLastProcessTime() {
		return lastProcessTime;
	}


	private void processCloudCharacteristicsReq(SimulationEvent ev) {
		int srcId = ((Integer) ev.getEventData()).intValue();
		send(srcId, 0, SimulationEventTag.CLOUD_CHARACTERISTICS_RES, getCloudCharacteristics());
	}



	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub
		
	}

	
	/**
	 * Process the event for an User/Broker who wants to create a VM in this PowerDatacenter. This
	 * PowerDatacenter will then send the status back to the User/Broker.
	 * 
	 * @param ev a Sim_event object
	 * @param ack the ack
	 * @pre ev != null
	 * @post $none
	 */
	protected void processVmCreateRequest(SimulationEvent ev, boolean ack) {
		VM vm = (VM) ev.getEventData();

		//todo: launch vm here.. 
		
		int result = vm.getId();

		if (ack) {
			int[] data = new int[3];
			data[0] = getId();
			data[1] = result;
			data[2] = 1;
			
			//Since we oly have one user, send it back to the IaaSBroker
			//send(vm.getUserId(), 0.1, CloudSimTags.VM_CREATE_ACK, data);
			send(ev.getSrcEntity(), vm.getVMClass().getExpectedStartupTime(), SimulationEventTag.VM_CREATE_RESPONSE_ACK, data);
		}

		//if (result > 0) {
//			float amount = 0.0;
//			if (getDebts().containsKey(vm.getUserId())) {
//				amount = getDebts().get(vm.getUserId());
//			}
//			amount += getCharacteristics().getCostPerMem() * vm.getRam();
//			amount += getCharacteristics().getCostPerStorage() * vm.getSize();
//
//			getDebts().put(vm.getUserId(), amount);
//
			getVmList().add(vm);
//
			if (vm.isBeingProvisioned()) {
				vm.setState(VM.RUNNING);
			}
	}
	
	private List<VM> getVmList() {
		return vmList;
	}


	public IaaSCloudCharacteristics getCloudCharacteristics() {
		return cloudCharacteristics;
	}
}
