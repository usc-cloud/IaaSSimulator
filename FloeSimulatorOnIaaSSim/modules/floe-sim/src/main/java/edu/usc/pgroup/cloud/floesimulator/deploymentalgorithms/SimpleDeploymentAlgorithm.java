package edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import javax.management.modelmbean.RequiredModelMBean;

import edu.usc.pgroup.cloud.floesimulator.ApplicationDeployment;
import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Alternate;
import edu.usc.pgroup.cloud.floesimulator.appmodel.ConcretePellet;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Pellet;
import edu.usc.pgroup.cloud.floesimulator.appmodel.PelletEdge;
import edu.usc.pgroup.cloud.floesimulator.utils.AppConfig;
import edu.usc.pgroup.cloud.floesimulator.utils.FloeConstants;
import edu.usc.pgroup.cloud.floesimulator.utils.RuntimeStatusUtils;
import edu.usc.pgroup.cloud.floesimulator.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.Dataflow;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;

public class SimpleDeploymentAlgorithm extends DeploymentAlgorithm{

	long count = 1;
	
	int m = 5;
	int n = 7;
	
	@Override
	public ApplicationDeployment getInitialDeployment(
			AbstractDynamicDataflow graph, List<VMClass> resourceClasses,
			Map<Integer, Double> inputSources, double sigma,
			double minOmega, double optimizationPeriod) {

		ApplicationDeployment deployment = new ApplicationDeployment(UUID.randomUUID().toString(), graph);
		
		
		//Select alternates locally		
		Map<Integer,Float> cost = new HashMap<>();
		Map<Integer,Alternate> selectedAlternates = new HashMap<>();
		
		for(Pellet p: graph.getPellets())
		{
			double maxValueToCost = 0;
			Alternate selectedAlternate = null;
			for(Alternate a: p.getAlternates())
			{
				double valueToCost = a.getValue()/a.getPeStandardCoreSecondPerMessage();
				if(valueToCost > maxValueToCost)
				{
					maxValueToCost = valueToCost;
					selectedAlternate = a;
				}
			}
			if(selectedAlternate != null)
			{
				selectedAlternates.put(p.getId(), selectedAlternate);
				cost.put(p.getId(),selectedAlternate.getPeStandardCoreSecondPerMessage());
			}
		}
		
		
		List<ConcretePellet> concretePellets = new ArrayList<>();
		for(Pellet p: graph.getPellets())
		{
			//System.out.println(p.getId() + " Cost:" + cost.get(p.getId()) + " alternate:" + selectedAlternates.get(p.getId()).getInstanceId());
			ConcretePellet cp = new ConcretePellet(p);
			cp.setActiveAlternate(selectedAlternates.get(p.getId()));
			concretePellets.add(cp);
		}
		
		
		//Use simple round robin technique to deploy pellet instances on "best VM" without re packing		
		double maxOutputDataRate = Utils.getMaxOutputDataRate(graph, concretePellets, inputSources);
		//System.out.println(maxOutputDataRate);
		
		double appValue = Utils.getAppValue(graph, concretePellets);
		//System.out.println(appValue);
		

		List<Integer> BFSOrder = Utils.getBFSOrder(graph);
		
		VM lastVirtualMachine = null;
		Map<Integer, VM> virtualMachineInstances = new HashMap<>();
		double omega = 0;
		
		VMClass best = VMClasses.getVMsSortedBySize().get(0);
		
		Map<Integer, Map<Integer, Integer>> pelletVMMapping = new HashMap<>();
		
		double totalCost = 0;
		double f = 0;
		
		List<Integer> pelletOrder = BFSOrder;
		
		Map<Integer,Integer> pelletUnitCounts = new HashMap<>();
		int vmid = 0;
		do
		{
			
			if(lastVirtualMachine==null || !lastVirtualMachine.isCoreAvailable())
			{
				
				lastVirtualMachine = new VM(vmid++,1,best,null);
 				virtualMachineInstances.put(lastVirtualMachine.getId(), lastVirtualMachine);
			}
			
			//Get the VM mappings.. 
			//System.out.println(omega);
			Integer pelletId = pelletOrder.remove(0);
			
			if(!pelletUnitCounts.containsKey(pelletId))
			{
				pelletUnitCounts.put(pelletId,1);
			}
			else
			{
				pelletUnitCounts.put(pelletId,pelletUnitCounts.get(pelletId)+1);
			}
			Utils.putPelletOnVM(pelletVMMapping,pelletId, lastVirtualMachine);
			
			
			Utils.ProcessTick(graph, pelletVMMapping, concretePellets, virtualMachineInstances, inputSources, maxOutputDataRate);
			
			//Utils.printPelletMapping(pelletVMMapping);
			
			omega = Utils.getExpectedRelativeApplicationThroughput(graph,concretePellets,maxOutputDataRate);
		
			if(omega <= 0) continue;
			
			totalCost = Utils.getTotalExpectedCost(virtualMachineInstances)
					* optimizationPeriod;
			
			f = appValue - sigma * totalCost;
			
			pelletOrder = Utils.findBottlenecks(concretePellets);
			
			
			//System.out.println("\n=>O:" + omega + ", f" + f + ", c" + totalCost);
			for (ConcretePellet cp : concretePellets) {
				//System.out.print(cp.getId() + "["
				//		+ cp.getActiveAlternate().getInstanceId()
				//		+ "]" + " -> "
				//		+ pelletUnitCounts.get(cp.getId())
				//		+ ",");
									
				cp.setPelletUnitCount(pelletUnitCounts.get(cp.getId()));
			}
			
			//System.out.println(omega);
		}while(omega < minOmega);
		
		Utils.printPelletMapping(pelletVMMapping);
		
		Map<Integer,VMClass> vmInstanceClassMapping = new HashMap<>();
		
		for (VM vm : virtualMachineInstances.values()) {
			//System.out.print(vm.getId()
			//		+ "=>"
			//		+ vm.getVMClass()
			//				.getVmClassName() + ", ");
			vmInstanceClassMapping.put(vm.getId(), vm.getVMClass());
		}
		
		deployment.setSigma(sigma);
		deployment.setConcretePellets(concretePellets);
		deployment.setPelletVMMapping(pelletVMMapping);
		deployment.setInputSources(inputSources);
		deployment.setVmInstanceClassMapping(vmInstanceClassMapping);

		System.out.println("\n*****");
		return deployment;
	}

	@Override
	public ApplicationDeploymentDelta getDeltaDeployment(			
			ApplicationDeployment d,
			List<VMClass> resourceClasses, double minOmega) {
		
		
		double maxOutDataRate = d.getMaxOutDataRateForOutPellets()/(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval);				
		double currentOutDataRate = d.getCurrentOutDataRateForOutPellets()/(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval); 

		double currentOmega = maxOutDataRate > 0 ? currentOutDataRate/maxOutDataRate : currentOutDataRate;
		double epsilon = 0.15;
		
		
		Map<Integer, Double> maxOutRates = d.getMaxOutDataRates();
		
		Map<Integer, Double> currOutRates = d.getCurrentOutDataRates();
		Map<Integer, Double> currInRates = d.getCurrentInputRates();
		Map<Integer, Double> currDeltaQueueLengths = d.getCurrentQueueLengths();
		Map<Integer, Double> currProcessRates = d.getCurrentNumMsgProcessedPerSec();
		
		
		Normalize(maxOutRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currOutRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currInRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currDeltaQueueLengths,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currProcessRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		
		
		
		List<DataflowProcessingElement> toAddList = new ArrayList<>();
		List<DataflowProcessingElement> toRemoveList = new ArrayList<>();
		List<ConcretePellet> toChangeAlternate = new ArrayList<>();
		if(count%n == 0)
		{
			//Select appropriate alternates based whether we are under or overprovisioned.. do not change resource allocations.
			//i.e. give the current resources. Choose the best alternative under the given data rates.
			Map<Integer,Double> coreUnits = RuntimeStatusUtils.getCurrentAvailableCoreUnitsPerNode(d.getPeNodeToInstancesMap());
			Map<Integer,Double> dataRates = d.getCurrentInputRates();
			
			if(currentOmega < minOmega - epsilon)
			{
			
				
				for(ConcretePellet cp: d.getConcretePellets())
				{
					double dr = dataRates.get(cp.getId()); 
					double cu = coreUnits.get(cp.getId());
					double maxValueToCost = 0;
					Alternate selectedAlternate = null;
					double currentRequired = cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr;
					
					List<Alternate> sortedFeasibleAlternates = new ArrayList<>();
					for(Alternate a : cp.getAlternates())
					{
						if(a == cp.getActiveAlternate()) continue;
						
						double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
						
						if(currentRequired < requriedCoreSeconds) continue;
						
						double valueToCost = a.getValue()/a.getPeStandardCoreSecondPerMessage();
						
						int i = 0;
						for(; i < sortedFeasibleAlternates.size(); i++)
						{
							Alternate b = sortedFeasibleAlternates.get(i);
							if(valueToCost > b.getValue()/b.getPeStandardCoreSecondPerMessage())
							{
								break;
							}
						}
						
						sortedFeasibleAlternates.add(i, a);
					}
					
					if(sortedFeasibleAlternates.size() > 0)
					{						
						for(Alternate a: sortedFeasibleAlternates)
						{
							double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
							if(requriedCoreSeconds < cu)
							{
								selectedAlternate = a;
								break;
							}
						}
						
						if(selectedAlternate == null)
						{
							selectedAlternate = sortedFeasibleAlternates.get(0);
						}
					}
					
					
					if(selectedAlternate != null)
					{
						cp.setActiveAlternate(selectedAlternate);
						toChangeAlternate.add(cp);
					}
				}
			}
			else if(currentOmega > minOmega + epsilon)
			{
				for(ConcretePellet cp: d.getConcretePellets())
				{
					double dr = dataRates.get(cp.getId()); 
					double cu = coreUnits.get(cp.getId());
					
					Alternate selectedAlternate = null;
					double currentRequired = cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr;
					
					List<Alternate> sortedFeasibleAlternates = new ArrayList<>();
					for(Alternate a : cp.getAlternates())
					{
						if(a == cp.getActiveAlternate()) continue;
						
						double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
						
						if(currentRequired > requriedCoreSeconds) continue;
						
						double valueToCost = a.getValue()/a.getPeStandardCoreSecondPerMessage();
						
						int i = 0;
						for(; i < sortedFeasibleAlternates.size(); i++)
						{
							Alternate b = sortedFeasibleAlternates.get(i);
							if(valueToCost > b.getValue()/b.getPeStandardCoreSecondPerMessage())
							{
								break;
							}
						}
						
						sortedFeasibleAlternates.add(i, a);
					}
					
					//System.out.println("CP: " + cp.getId());
					if(sortedFeasibleAlternates.size() > 0)
					{						
						for(Alternate a: sortedFeasibleAlternates)
						{
							//System.out.println("feaible: " + a.getNodeId());
							double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
							if(requriedCoreSeconds < cu)
							{
								selectedAlternate = a;
								break;
							}
						}
						
						if(selectedAlternate == null)
						{
							selectedAlternate = sortedFeasibleAlternates.get(0);
						}
					}
					
					
					if(selectedAlternate != null)
					{
						cp.setActiveAlternate(selectedAlternate);
						toChangeAlternate.add(cp);
					}
				}
			}
			
		}
		else if(count%m == 0)
		{
			//Given the datarate and the alternates, choose to provision or shutdown (put on standby) the resources 
			//based on we are over or under provisioned.
			
			//For simple strategy.. just use local? information (but still use "all" instances)
			Map<Integer, List<DataflowProcessingElement>> nodeInstances = d.getPeNodeToInstancesMap();
			

			Dataflow flow = d.getDataflow();
			if(currentOmega < minOmega - epsilon)
			{
				
				//find bottlencks in the current deployed system(ordered by relative throughput) small to large..
				List<Integer> currentBottlenecks = RuntimeStatusUtils.findBottlenecks(currDeltaQueueLengths);
				
				
				double expectedOmega = 0;
				do
				{
					//increase pellet instance (in the abstract dataflow)
					if(currentBottlenecks.size() == 0)break;
					Integer bottleNeck = currentBottlenecks.remove(0);
					List<DataflowProcessingElement> bottleNeckPeList = d.getPeNodeToInstancesMap().get(bottleNeck);
					
					Double currBottleNeckProcessRate = currProcessRates.get(bottleNeck);
					
					List<DataflowProcessingElement> newInstances = flow.incrementPEInstance(bottleNeck,1);
					d.insertPEInstances(bottleNeck,newInstances);
					toAddList.addAll(newInstances);
					
					int newNumInstances = bottleNeckPeList.size();
					Double updatedProcessRates = currBottleNeckProcessRate * (newNumInstances)/(newNumInstances-1);
					
					currProcessRates.put(bottleNeck, updatedProcessRates);
					
					double preExpectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, d.getAppGraph().getOutputPellets(), maxOutDataRate);
					//System.out.println(expectedOmega);
					
					RuntimeStatusUtils.processTick(d.getAppGraph(),d.getConcretePellets(),currInRates,currOutRates,currProcessRates,currDeltaQueueLengths);

					expectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, d.getAppGraph().getOutputPellets(),maxOutDataRate);
					//System.out.println(expectedOmega);
					
					currentBottlenecks = RuntimeStatusUtils.findBottlenecks(currDeltaQueueLengths);
					
					if(currentBottlenecks.size() == 0) break;
					
				}
				while(expectedOmega < minOmega);
				
				//Given the pellets for which to increase the pellet instances, deploy them on the VMs
				
			}
			else if(currentOmega > minOmega + epsilon)
			{
				double expectedOmega = 0;
				
				
				//find over provisioned (ordered by relative throughput) large to small..
				List<Integer> currentOverProvisioned = RuntimeStatusUtils.findOverProvisioned(currDeltaQueueLengths);
				
				do
				{
					//increase pellet instance (in the abstract dataflow)
					Integer overProvisioned = -1;
					List<DataflowProcessingElement> overProvisionedPeList = null;
					while(currentOverProvisioned.size() > 0)
					{
						overProvisioned = currentOverProvisioned.remove(0);
						List<DataflowProcessingElement> tempOverProvisionedPeList = d.getPeNodeToInstancesMap().get(overProvisioned);
						if(tempOverProvisionedPeList.size() > 1)
						{
							overProvisionedPeList = tempOverProvisionedPeList;
						}						
					}
					
					if(overProvisionedPeList == null || overProvisionedPeList.size() <= 1) break;
					
					
					Double currOverProvisionedProcessRate = currProcessRates.get(overProvisioned);
					
					//Ideally, choose the pe based on the VM.. for simple strategy.. just pick any
					List<DataflowProcessingElement> toRemove = new ArrayList<>();
					toRemove.add(overProvisionedPeList.get(0));
					
					List<DataflowProcessingElement> removedInstances = flow.decrementPEInstance(toRemove);
					
					d.removePEInstances(removedInstances);
					toRemoveList.addAll(removedInstances);
					
					int newNumInstances = overProvisionedPeList.size();
					Double updatedProcessRates = currOverProvisionedProcessRate * (newNumInstances)/(newNumInstances+1);
					
					currProcessRates.put(overProvisioned, updatedProcessRates);
					
					expectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, d.getAppGraph().getOutputPellets(), maxOutDataRate);
					//System.out.println(expectedOmega);
					
					RuntimeStatusUtils.processTick(d.getAppGraph(),d.getConcretePellets(),currInRates,currOutRates,currProcessRates,currDeltaQueueLengths);

					expectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, d.getAppGraph().getOutputPellets(),maxOutDataRate);
					//System.out.println(expectedOmega);
					
					currentOverProvisioned = RuntimeStatusUtils.findOverProvisioned(currDeltaQueueLengths);

				}
				while(expectedOmega > minOmega + epsilon);
		
				
				//Given the pellets for which to remove the pellet instances, remove the instance.. 
				//if the VM gets free put it on standby
			}
		}
		
		ApplicationDeploymentDelta delta = new ApplicationDeploymentDelta();
		delta.setToAdd(toAddList);
		delta.setToRemoveList(toRemoveList);
		delta.setToChangeAlternatesList(toChangeAlternate);
		count++;
		//if(count > Math.max(n, m)) count = 1;
		return delta;
	}

	private void Normalize(Map<Integer, Double> map, float f) {
		for(Integer key: map.keySet())
		{
			map.put(key, map.get(key)/f);
		}
	}

	@Override
	public VMConsolidationInfo consolidateVMs(List<VM> vms) {
		return new VMConsolidationInfo();
	}

}
