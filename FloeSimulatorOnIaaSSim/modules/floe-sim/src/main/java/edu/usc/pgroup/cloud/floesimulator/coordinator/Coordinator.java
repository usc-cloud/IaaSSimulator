package edu.usc.pgroup.cloud.floesimulator.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;


import edu.usc.pgroup.cloud.floesimulator.ApplicationDeployer;
import edu.usc.pgroup.cloud.floesimulator.ApplicationDeployment;
import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Alternate;
import edu.usc.pgroup.cloud.floesimulator.appmodel.ConcretePellet;
import edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms.ApplicationDeploymentDelta;
import edu.usc.pgroup.cloud.floesimulator.utils.AppConfig;
import edu.usc.pgroup.cloud.floesimulator.utils.FloeConstants;
import edu.usc.pgroup.cloud.floesimulator.utils.RuntimeStatusUtils;
import edu.usc.pgroup.cloud.floesimulator.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.ExecutionTrigger;
import edu.usc.pgroup.cloud.iaassim.IaaSBroker;
import edu.usc.pgroup.cloud.iaassim.IaaSCloud;
import edu.usc.pgroup.cloud.iaassim.IaaSCloudCharacteristics;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataSource;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.Dataflow;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.PeriodicDataSource;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.SmoothRandomDataSource;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventTag;
import edu.usc.pgroup.cloud.iaassim.pescheduler.DataflowExecutionScheduler;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.ConstantDist;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.SmoothWalk;
import edu.usc.pgroup.cloud.iaassim.utils.Constants;
import edu.usc.pgroup.cloud.iaassim.utils.LogEntity;
import edu.usc.pgroup.cloud.iaassim.utils.PerfLog;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;


public class Coordinator extends SimulationEntity{

	private IaaSBroker broker;
	private IaaSCloud  cloud;
	public Coordinator(IaaSBroker broker, IaaSCloud iaaSCloud) {
		super("Coordinator");		
		deployments = new HashMap<>();
		this.broker = broker;
		this.cloud = iaaSCloud;
	}

	Map<String,ApplicationDeployment> deployments;
	private int peid = 0;

	public String deployApplication(AbstractDynamicDataflow app)
	{
		ApplicationDeployment d = ApplicationDeployer.deploy(app);
		deployments.put(d.getDeploymentId(), d);
		
		/*
		 * Given the deployment creted by the Application Deployer. 
		 * Launch VM, create "DataflowPeocessingElements", create Dataflow and launch them on the VMs 
		 * 
		 */
		
		Map<Integer, VM> vms = new HashMap<>();
		
		for(Entry<Integer, VMClass> vmEntry: d.getVmInstanceClassMapping().entrySet())
		{
			//TODO make this configurable..
			VM vm = submitVMRequest(vmEntry.getValue());
			vms.put(vmEntry.getKey(), vm);
		}
		
		
		Map<Integer, Map<Integer, Integer>> pelletMappings = d.getPelletVMMapping();
		
		Map<Integer, List<DataflowProcessingElement>> peInstances = new HashMap<>();
		
		Dataflow flow = new Dataflow();
		
		d.setDataflow(flow);
		
		for(ConcretePellet cp : d.getConcretePellets())
		{
			Alternate activeAlternate = cp.getActiveAlternate();
			
			//Create a DataflowProcessingElement
			DataflowProcessingElement pe = new DataflowProcessingElement(cp.getId(), activeAlternate.getPeStandardCoreSecondPerMessage(), 1
					, true, ExecutionTrigger.Message, activeAlternate.getSelectivity());
			
			
			//Add instances to the dataflow
			List<DataflowProcessingElement> instances = flow.addPEInstances(pe, cp.getPelletUnitCount());
			peInstances.put(pe.getInstanceId(),instances);
			
			//broker.submitPE(pe, cp.getPelletUnitCount());			
		}
			
		//Create Edge in the dataflow
		for(ConcretePellet cp : d.getConcretePellets())
		{
			List<Integer> successors = d.getSuccessors(cp.getId());
			
			for(Integer successor: successors)
			{
				//Add flow edges
				flow.addEdge(cp.getId(), successor);
			}			
		}
		
		
		//deploy pe instances on the VMs..
		for(Entry<Integer, List<DataflowProcessingElement>> peInstanceEntry: peInstances.entrySet())
		{
			int peId = peInstanceEntry.getKey();
			
			Iterator<DataflowProcessingElement> instances = peInstanceEntry.getValue().iterator();
			System.out.println("submitting:"+peId);
			Map<Integer, Integer> vmMappings = pelletMappings.get(peId);
			for(Entry<Integer, Integer> vmMapping: vmMappings.entrySet())
			{
				int vmId = vmMapping.getKey();
				VM vm = vms.get(vmId);
				
				int count = vmMapping.getValue();
				for(int i = 0; i < count; i++)
				{
					broker.submitPE(instances.next(), vm);
				}
			}
			
			//System.out.println("Pending instances?:"+instances.hasNext());
		}
		
		//set data rates here.. 
		for(Entry<Integer, Double> inputSource: d.getInputSources().entrySet())
		{
			int peid = inputSource.getKey();
			List<DataflowProcessingElement> currPeInstances = peInstances.get(peid);
			int numInstances = currPeInstances.size();
			for(DataflowProcessingElement ele: currPeInstances)
			{
				Properties params = new Properties();
				params.setProperty("datarate", Double.toString(inputSource.getValue()/numInstances));
				params.setProperty("period", "60");
				params.setProperty("activeDuration", "60");
				DataSource s = new SmoothRandomDataSource(params, ele);
				broker.submitDataSource(s);
			}
		}
		d.setVms(vms);
		d.setPeNodeToInstancesMap(peInstances);
		return d.getDeploymentId();
	}
	
	private VM submitVMRequest(VMClass vmClass) {
		VM vm = broker.submitVMRequest(vmClass, new DataflowExecutionScheduler(), /*new ConstantDist(1)*/ new SmoothWalk(1, 0.85));		
		schedule(broker.getId(), 0, SimulationEventTag.RUNTIME_VM_CHANGE_REQ, cloud.getId());
		return vm;
	}

	static Coordinator instance = null;
	public static synchronized Coordinator getInstance()
	{
		if(instance == null)
		{
			instance = new Coordinator(new IaaSBroker("floe_broker"),new IaaSCloud("myCloud",IaaSCloudCharacteristics.getDefaultCharacteristics()));
		}
		return instance;
	}

	@Override
	public void startEntity() {
		schedule(getId(), FloeConstants.OptimizationTimeSlotInterval, SimulationEventTag.COORDINATOR_ACTION_EVENT, deployments.keySet().iterator().next());
		schedule(getId(), FloeConstants.MonitoringTimeSlotInterval, SimulationEventTag.COORDINATOR_MONITOR_EVENT, deployments.keySet().iterator().next());
	}

	@Override
	public void processEvent(SimulationEvent ev) {
		switch(ev.getEventTag())
		{
		case COORDINATOR_ACTION_EVENT:
			logCurrentValueAndCost(ev);
			processCoordinatorActionEvent(ev);
			schedule(getId(), FloeConstants.OptimizationTimeSlotInterval, SimulationEventTag.COORDINATOR_ACTION_EVENT,ev.getEventData());
			break;
		case COORDINATOR_MONITOR_EVENT:
			processMonitorEvent(ev);
			schedule(getId(), FloeConstants.MonitoringTimeSlotInterval, SimulationEventTag.COORDINATOR_MONITOR_EVENT,ev.getEventData());
			break;
		}
	}

	private void processMonitorEvent(SimulationEvent ev) {
		
		String deploymentId = (String)ev.getEventData();
		ApplicationDeployment d  = deployments.get(deploymentId);
		
		Map<Integer, Double> inputDataRate = broker.getInputDataRates();
		//Map<Integer, Double> inputQueueLength = broker.getInputQueueLengthAteachPellet();
		
		//double maxOutDataRate = Utils.getMaxDataRate(d.getAppGraph(), d.getConcretePellets(), inputDataRate);
		
		Map<Integer, Double> maxOutDataRates = Utils.getMaxDataRateAtAllPellet(d.getAppGraph(), d.getConcretePellets(), inputDataRate);
		

		
		Map<Integer, Double> currentOutDataRates = RuntimeStatusUtils.getOutputDataRatesAtAllPellets(d.getPeNodeToInstancesMap());
		Map<Integer,Double> currentDeltaQueueLengths = RuntimeStatusUtils.getCurrentDeltaQueueLengths(d.getPeNodeToInstancesMap());
		Map<Integer,Double> currentInputRates = RuntimeStatusUtils.getCurrentInputRates(d.getPeNodeToInstancesMap());
		Map<Integer,Double> currentNumMsgProcessedPerSec = RuntimeStatusUtils.getNumMsgProcessedPerSec(d.getPeNodeToInstancesMap());
		
		
		double maxOutDataRate = 0;
		double currentOutDataRate = 0;
		for(Integer outPellet: d.getAppGraph().getOutputPellets())
		{
			maxOutDataRate += maxOutDataRates.get(outPellet);
			currentOutDataRate += currentOutDataRates.get(outPellet);
		}
		//System.out.println("currentO:"+currentOutDataRate);
		double omega = maxOutDataRate > 0 ? currentOutDataRate/maxOutDataRate : currentOutDataRate;
		
		
		d.addCurrentMaxOutDataRates(maxOutDataRates);
		d.addCurrentOutDataRates(currentOutDataRates);
		d.addCurrentInputRates(currentInputRates);
		d.addCurrentQueueLength(currentDeltaQueueLengths);
		d.addCurrentMessgaesProcessedPerSecond(currentNumMsgProcessedPerSec);
		d.addCurrentOmega(omega);
	}

	int count = 0;
	private void processCoordinatorActionEvent(SimulationEvent ev) {
		
		
		String deploymentId = (String)ev.getEventData();
		ApplicationDeployment d  = deployments.get(deploymentId);
		
		double minOmega = Double.parseDouble(AppConfig.getInstance().get("minAppThroughput"));
		
		ApplicationDeploymentDelta deltaDeployment = 
				d.getDeploymentAlgorithm().getDeltaDeployment(d, VMClasses.getAll(), minOmega);
		
		Map<Integer,Integer> tempAssignedVMCores = new HashMap<>();
		
		for(DataflowProcessingElement ni: deltaDeployment.getToAddList())
		{
			//System.out.println("adding:"+ ni.getNodeId() +", "+ni.getInstanceId()+ " at time:" + IaaSSimulator.getClock());
			broker.submitPE(ni, getFreeVM(d, tempAssignedVMCores));
			if(ni.getDataSource() != null)
				broker.submitDataSource(ni.getDataSource());
		}
		
		for(DataflowProcessingElement ri: deltaDeployment.getToRemoveList())
		{
			//System.out.println("removing:"+ ri.getNodeId() +", "+ri.getInstanceId()+ " at time:" + IaaSSimulator.getClock());
			broker.removePE(ri);
			if(ri.getDataSource() != null)
				broker.removeDataSource(ri.getDataSource());
		}
		
		for(ConcretePellet cp: deltaDeployment.getToChangeAlternateList())
		{
			//System.out.println("changing:"+ cp.getId() +" to alternate: "+ cp.getActiveAlternate().getNodeId()+ " at time:" + IaaSSimulator.getClock());
			for(DataflowProcessingElement ri: d.getPeNodeToInstancesMap().get(cp.getId()))
			{
				/*System.out.println("removing:"+ri.getInstanceId()+ " at time:" + IaaSSimulator.getClock());
				broker.removePE(ri);
				if(ri.getDataSource() != null)
					broker.removeDataSource(ri.getDataSource());*/				
				ri.updatePE(cp.getActiveAlternate().getPeStandardCoreSecondPerMessage(), cp.getActiveAlternate().getSelectivity());
			}
		}
		
		List<VM> vmsToShutdown =  Utils.checkVMShutdowns(d.getVms());
		
		for(VM toShutdown: vmsToShutdown)
		{
			broker.submitVMShutdownRequest(toShutdown);			
		}
		if(vmsToShutdown.size() > 0)
		{
			schedule(broker.getId(), 0, SimulationEventTag.RUNTIME_VM_CHANGE_REQ, cloud.getId());
		}
		
		d.resetCurrentStats();
		schedule(broker.getId(), 0, SimulationEventTag.RUNTIME_PE_CHANGE_REQ);
	}

	private VM getFreeVM(ApplicationDeployment d, Map<Integer,Integer> tempAssignedVMCores) {
		Map<Integer, VM> vms = d.getVms();
		List<VM> available = new ArrayList<>();
		for(VM vm: vms.values())
		{
			if(vm.getState() == VM.RUNNING && (vm.getAvailableCores() - (tempAssignedVMCores.containsKey(vm.getId()) ? tempAssignedVMCores.get(vm.getId()):0) > 0))
			{
				available.add(vm);
			}
		}
		
		VM best = null;
		if(available.size() > 0)
		{
			double minCostPerCore = Double.MAX_VALUE;
			for(VM vm : available)
			{
				double corecoeff = vm.getCurrentCoreCoeff();
				int numCores = vm.getVMClass().getCoreCount();
				double cost = vm.getVMClass().getCostPerHour();
				
				double costpercore = corecoeff * numCores/cost;
				if(costpercore < minCostPerCore)
				{
					minCostPerCore = costpercore;
					best = vm;
				}
				else if(costpercore == minCostPerCore)
				{
					if(numCores > best.getVMClass().getCoreCount()){
						minCostPerCore = costpercore;
						best = vm;
					}
				}
			}
		}
		
		if(best == null)
		{
			best = submitVMRequest(VMClasses.getVMsSortedBySize().get(0));
			d.getVms().put(best.getId(), best);
		}
		
		tempAssignedVMCores.put(best.getId(), (tempAssignedVMCores.containsKey(best.getId()) ? tempAssignedVMCores.get(best.getId()):0) + 1);
		return best;
	}

	private void logCurrentValueAndCost(SimulationEvent ev) {
		String deploymentId = (String)ev.getEventData();
		ApplicationDeployment d  = deployments.get(deploymentId);
		double value = Utils.getAppValue(d.getAppGraph(), d.getConcretePellets());
		double totalCost = Utils.getTotalCost(new ArrayList<>(d.getVms().values()));
		double f = value - d.getSigma() *  totalCost;
		
		//setup input data rate..
		double numEvents = FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval;
		PerfLog.printCSV(LogEntity.COORD,deploymentId,value,totalCost,f,d.getMaxOutDataRateForOutPellets()/numEvents,
				d.getCurrentOutDataRateForOutPellets()/numEvents,d.getOmega()/numEvents);
		System.out.println(String.format("%s, %s, %f, %f, %f, %f, %f, %f", LogEntity.COORD,deploymentId,value,totalCost,f,d.getMaxOutDataRateForOutPellets()/numEvents,
				d.getCurrentOutDataRateForOutPellets()/numEvents,d.getOmega()/numEvents));
	}

	

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub
		
	}

	public int getBrokerId() {
		return broker.getId();
	}
	
}
