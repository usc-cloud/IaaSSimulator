package edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms;

import java.util.ArrayList;
import java.util.Arrays;
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
import edu.usc.pgroup.cloud.floesimulator.utils.FloeConstants;
import edu.usc.pgroup.cloud.floesimulator.utils.RuntimeStatusUtils;
import edu.usc.pgroup.cloud.floesimulator.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.Dataflow;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;

public class HeuristicDeployment extends DeploymentAlgorithm{

	private int vmid = 0;
	long count = 1;
	
	int scalePhasePeriod = 5;
	int changeAlternatePhasePeriod = 7;

	double epsilon = 0.1;
	
	@Override
	public ApplicationDeployment getInitialDeployment(AbstractDynamicDataflow graph,
			List<VMClass> resourceClasses,
			Map<Integer, Double> inputSources, double sigma,
			double minAppThroughput, double optimizationPeriod) {
		List<PelletEdge> reverseEdges = graph.reverseEdges();
		List<Integer> reverseInputPellets = new ArrayList<>();
		
		Queue<Integer> queue = new LinkedBlockingQueue<Integer>();
		Map<Integer,Boolean> marks = new HashMap<>();

		Map<Integer,Float> cost = new HashMap<>();
		Map<Integer,Alternate> selectedAlternates = new HashMap<>();
		
		for(Pellet p: graph.getPellets())
		{
			reverseInputPellets.add(p.getId());
		}
		
		for(PelletEdge edge: reverseEdges)
		{
			reverseInputPellets.remove((Integer)edge.getDestNodeId());
		}
		
		for(Integer p: reverseInputPellets)
		{
			marks.put(p, true);
			queue.add(p);
		}
		
 
		while(queue.isEmpty() == false)
		{
			Integer pstr = queue.remove();
			
			Pellet p = Utils.findNode(graph.getPellets(), pstr);
			
			//reverseBFSOrder.add();
			if(!cost.containsKey(pstr))
			{
				List<Integer> successors = Utils.getSuccessors(pstr, graph.getEdges());
				if(successors == null || successors.size() == 0)
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
						selectedAlternates.put(pstr, selectedAlternate);
						cost.put(pstr,selectedAlternate.getPeStandardCoreSecondPerMessage());
					}
				}
				else
				{					
					double maxValueToCost = 0;
					Alternate selectedAlternate = null;
					float selectedAlternateCost = 0;
					float currentCost;
					for(Alternate a: p.getAlternates())
					{
						double successor_cost = 0;
						for(Integer successor: successors)
						{
							successor_cost += cost.get(successor);
						}
						currentCost = (float) (a.getPeStandardCoreSecondPerMessage() + a.getSelectivity()*successor_cost);
						double valueToCost = a.getValue()/currentCost;
						if(valueToCost > maxValueToCost)
						{
							maxValueToCost = valueToCost;
							selectedAlternate = a;
							selectedAlternateCost = currentCost;
						}
					}
					if(selectedAlternate != null)
					{
						selectedAlternates.put(pstr, selectedAlternate);
						cost.put(pstr, selectedAlternateCost);
					}
				}
			}
			
			//Continue (reverse) BFS
			List<Integer> reverseSuccessors = Utils.getSuccessors(pstr,reverseEdges);
			for(Integer successor: reverseSuccessors)
			{
				if(!marks.containsKey(successor))
				{
					marks.put(successor, true);
					queue.add(successor);
				}
			}
		}
		
		List<ConcretePellet> concretePellets = new ArrayList<>();
		for(Pellet p: graph.getPellets())
		{
			//System.out.println(p.getId() + " Cost:" + cost.get(p.getId()) + " alternate:" + selectedAlternates.get(p.getId()).getNodeId());
			ConcretePellet cp = new ConcretePellet(p);
			cp.setActiveAlternate(selectedAlternates.get(p.getId()));
			concretePellets.add(cp);
		}
		
		
		//phase 2 (select resources and do mapping)
		List<Integer> BFSOrder = Utils.getBFSOrder(graph);
		
		double maxOutputDataRate = Utils.getMaxOutputDataRate(graph, concretePellets, inputSources);
		//System.out.println(maxOutputDataRate);
		
		double appValue = Utils.getAppValue(graph, concretePellets);
		//System.out.println(appValue);
		
		
		VM lastVirtualMachine = null;
		Map<Integer, VM> virtualMachineInstances = new HashMap<>();
		double omega = 0;
				
		List<VMClass> sortedList = VMClasses.getVMsSortedBySize(); //change this to biggest?
		VMClass bestClass = sortedList.get(0);
		
		Map<Integer, Map<Integer, Integer>> pelletVMMapping = new HashMap<>();
		
		double totalCost = 0;
		double f = 0;
		
		List<Integer> pelletOrder = BFSOrder;
		
		Map<Integer,Integer> pelletUnitCounts = new HashMap<>();
		
		
		Map<VMClass,List<VM>> initializedVMs = new HashMap<>();
		do
		{
			
			if(lastVirtualMachine==null || !lastVirtualMachine.isCoreAvailable())
			{
				lastVirtualMachine = new VM(vmid ++,1,bestClass,null);
				
				List<VM> vmList = initializedVMs.containsKey(bestClass) ? initializedVMs.get(bestClass) : new ArrayList<VM>();
				
				vmList.add(lastVirtualMachine);
				
				initializedVMs.put(bestClass, vmList);
				
				virtualMachineInstances.put(lastVirtualMachine.getId(), lastVirtualMachine);
			}
			
			//Get the VM mappings.. 
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
			//System.out.println(omega);
		}while(omega < minAppThroughput);
		
		
		//Map<Integer,Double> overProvisioned = Utils.findOverProvisioned(concretePellets);
		
		/*System.out.println("\n====");
		for (Integer ov : overProvisioned.keySet()) {
			System.out.println("over:" + ov + ": " + overProvisioned.get(ov));
			
			ConcretePellet cp = ((ConcretePellet)Utils.findNode(concretePellets, ov));
					
			double actualRequired = bestClass.getCoreCoeff() -  -1 * overProvisioned.get(ov) * cp.getActiveAlternate().getPeStandardCoreSecondPerMessage();
			System.out.println("ov cs:"+actualRequired);
			
			
		}
		System.out.println("last vm available cores:"+lastVirtualMachine.getAvailableCores());
		System.out.println("###");*/
		
		if(lastVirtualMachine.getAvailableCores() > 0)
		{
			ArrayList<VMClass> sortedVMList = (ArrayList<VMClass>) (((ArrayList<VMClass>) sortedList).clone());
			sortedVMList.remove(0);
			virtualMachineInstances.remove(lastVirtualMachine.getId());
			Map<Integer, Double> allocated = lastVirtualMachine.getAllocatedPellets();
			
			Map<Integer, VM> vmInstances = Utils.repack(allocated,lastVirtualMachine, pelletVMMapping, sortedVMList, pelletUnitCounts);
			virtualMachineInstances.putAll(vmInstances);
		}
		
		for (ConcretePellet cp : concretePellets) {
			//System.out.print(cp.getId() + "["
			//		+ cp.getActiveAlternate().getNodeId()
			//		+ "]" + " -> "
			//		+ pelletUnitCounts.get(cp.getId())
			//		+ ",");
			cp.setPelletUnitCount(pelletUnitCounts.get(cp.getId()));
		}

		//System.out.println("f:" + f + " cost:"
		//		+ totalCost + " omega:" + omega);
		Utils.printPelletMapping(pelletVMMapping);
		
		Map<Integer,VMClass> vmInstanceClassMapping = new HashMap<>();
		
		for (VM vm : virtualMachineInstances.values()) {
			//System.out.print(vm.getId()
			//		+ "=>"
			//		+ vm.getVMClass()
			//				.getVmClassName() + ", ");
			vmInstanceClassMapping.put(vm.getId(), vm.getVMClass());
		}
		
		System.out.println("\n*****");
		
		ApplicationDeployment deployment = new ApplicationDeployment(UUID.randomUUID().toString(), graph);
		deployment.setSigma(sigma);
		deployment.setConcretePellets(concretePellets);
		deployment.setPelletVMMapping(pelletVMMapping);
		deployment.setInputSources(inputSources);
		deployment.setVmInstanceClassMapping(vmInstanceClassMapping);
		
		return deployment;
	}

	
	@Override
	public ApplicationDeploymentDelta getDeltaDeployment(
			ApplicationDeployment currentDeployment,
			List<VMClass> resourceClasses, double minOmega) {
		
		
		double maxOutDataRate = currentDeployment.getMaxOutDataRateForOutPellets()/(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval);				
		double currentOutDataRate = currentDeployment.getCurrentOutDataRateForOutPellets()/(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval); 

		double currentOmega = maxOutDataRate > 0 ? currentOutDataRate/maxOutDataRate : currentOutDataRate;
		
		
		
		Map<Integer, Double> maxOutRates = currentDeployment.getMaxOutDataRates();
		
		Map<Integer, Double> currOutRates = currentDeployment.getCurrentOutDataRates();
		Map<Integer, Double> currInRates = currentDeployment.getCurrentInputRates();
		Map<Integer, Double> currDeltaQueueLengths = currentDeployment.getCurrentQueueLengths();
		Map<Integer, Double> currProcessRates = currentDeployment.getCurrentNumMsgProcessedPerSec();
		
		
		Normalize(maxOutRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currOutRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currInRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currDeltaQueueLengths,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		Normalize(currProcessRates,(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
		
		
		
		List<DataflowProcessingElement> toAddList = new ArrayList<>();
		List<DataflowProcessingElement> toRemoveList = new ArrayList<>();
		List<ConcretePellet> toChangeAlternate = new ArrayList<>();
		if(count%changeAlternatePhasePeriod == 0)
		{
			//Select appropriate alternates based whether we are under or overprovisioned.. do not change resource allocations.
			//i.e. give the current resources. Choose the best alternative under the given data rates.
			Map<Integer,Double> coreUnits = RuntimeStatusUtils.getCurrentAvailableCoreUnitsPerNode(currentDeployment.getPeNodeToInstancesMap());
			Map<Integer,Double> dataRates = currentDeployment.getCurrentInputRates();
			
			if(currentOmega < minOmega - epsilon)
			{
				List<Integer> reverseInputPellets = new ArrayList<>();
				List<PelletEdge> reverseEdges = currentDeployment.getAppGraph().reverseEdges();
				
				
				Queue<Integer> queue = new LinkedBlockingQueue<Integer>();
				Map<Integer,Boolean> marks = new HashMap<>();

				Map<Integer,Float> tcost = new HashMap<>();
				Map<Integer,Float> tavailable = new HashMap<>();
				Map<Integer,Alternate> selectedAlternates = new HashMap<>();
				
				for(ConcretePellet p: currentDeployment.getConcretePellets())
				{
					reverseInputPellets.add(p.getId());
				}
				
				for(PelletEdge edge: reverseEdges)
				{
					reverseInputPellets.remove((Integer)edge.getDestNodeId());
				}
				
				for(Integer p: reverseInputPellets)
				{
					marks.put(p, true);
					queue.add(p);
				}
				
				while(queue.isEmpty() == false)
				{
					Integer pstr = queue.remove();
					
					ConcretePellet cp = (ConcretePellet) Utils.findNode(currentDeployment.getConcretePellets(), pstr);
					double dr = dataRates.get(cp.getId()); 
					double cu = coreUnits.get(cp.getId());
					
					//reverseBFSOrder.add();
					if(!tcost.containsKey(pstr))
					{
						List<Integer> successors = Utils.getSuccessors(pstr, currentDeployment.getAppGraph().getEdges());
						if(successors == null || successors.size() == 0)
						{
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
								
//								if(selectedAlternate == null)
//								{
//									selectedAlternate = sortedFeasibleAlternates.get(0);
//								}
							}
							
							
							if(selectedAlternate != null)
							{
								cp.setActiveAlternate(selectedAlternate);
								
								selectedAlternates.put(cp.getId(), selectedAlternate);
								
								//add to tcost
								tcost.put(cp.getId(), (float) (selectedAlternate.getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) cu);
								toChangeAlternate.add(cp);
							}
							else
							{
								tcost.put(cp.getId(), (float) (cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) cu);
							}
						}
						else
						{					
							Alternate selectedAlternate = null;
							
							double successor_cost = 0;
							for(Integer successor: successors)
							{
								successor_cost += tcost.get(successor);
							}
							
							double successor_available = 0;
							for(Integer successor: successors)
							{
								successor_available += tavailable.get(successor);
							}
							
							
							double currentRequired = cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr + successor_cost;
							
							Map<Alternate,Double> valueToCostMap = new HashMap<>();
							List<Alternate> sortedFeasibleAlternates = new ArrayList<>();
							for(Alternate a : cp.getAlternates())
							{
								if(a == cp.getActiveAlternate()) continue;
								
								
								
								//currentCost = (float) (a.getPeStandardCoreSecondPerMessage() + a.getSelectivity()*successor_cost);
								
								double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr + a.getSelectivity()*successor_cost/cp.getActiveAlternate().getSelectivity();
								
								if(currentRequired < requriedCoreSeconds) continue;
								
								double valueToCost = a.getValue()/requriedCoreSeconds;
								valueToCostMap.put(a, valueToCost);
								
								int i = 0;
								for(; i < sortedFeasibleAlternates.size(); i++)
								{
									Alternate b = sortedFeasibleAlternates.get(i);
									if(valueToCost > valueToCostMap.get(b))
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
									//double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
									double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr + a.getSelectivity()*successor_cost/cp.getActiveAlternate().getSelectivity();
									if(requriedCoreSeconds < (cu + successor_available))
									{
										selectedAlternate = a;
										break;
									}
								}
								
//								if(selectedAlternate == null)
//								{
//									selectedAlternate = sortedFeasibleAlternates.get(0);
//								}
							}
							
							
							if(selectedAlternate != null)
							{
								cp.setActiveAlternate(selectedAlternate);
								
								selectedAlternates.put(cp.getId(), selectedAlternate);
								//add to tcost
								tcost.put(cp.getId(), (float) (selectedAlternate.getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) (cu + successor_available));
								toChangeAlternate.add(cp);
							}
							else
							{
								tcost.put(cp.getId(), (float) (cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) (cu + successor_available));
							}
						}
					}
					//Continue (reverse) BFS
					List<Integer> reverseSuccessors = Utils.getSuccessors(pstr,reverseEdges);
					for(Integer successor: reverseSuccessors)
					{
						if(!marks.containsKey(successor))
						{
							marks.put(successor, true);
							queue.add(successor);
						}
					}
				}
			}
			else if(currentOmega > minOmega + epsilon)
			{
				List<Integer> reverseInputPellets = new ArrayList<>();
				List<PelletEdge> reverseEdges = currentDeployment.getAppGraph().reverseEdges();
				
				
				Queue<Integer> queue = new LinkedBlockingQueue<Integer>();
				Map<Integer,Boolean> marks = new HashMap<>();

				Map<Integer,Float> tcost = new HashMap<>();
				Map<Integer,Float> tavailable = new HashMap<>();
				Map<Integer,Alternate> selectedAlternates = new HashMap<>();
				
				for(ConcretePellet p: currentDeployment.getConcretePellets())
				{
					reverseInputPellets.add(p.getId());
				}
				
				for(PelletEdge edge: reverseEdges)
				{
					reverseInputPellets.remove((Integer)edge.getDestNodeId());
				}
				
				for(Integer p: reverseInputPellets)
				{
					marks.put(p, true);
					queue.add(p);
				}
				
				while(queue.isEmpty() == false)
				{
					Integer pstr = queue.remove();
					
					ConcretePellet cp = (ConcretePellet) Utils.findNode(currentDeployment.getConcretePellets(), pstr);
					double dr = dataRates.get(cp.getId()); 
					double cu = coreUnits.get(cp.getId());
					
					//reverseBFSOrder.add();
					if(!tcost.containsKey(pstr))
					{
						List<Integer> successors = Utils.getSuccessors(pstr, currentDeployment.getAppGraph().getEdges());
						if(successors == null || successors.size() == 0)
						{
							Alternate selectedAlternate = null;
							double currentRequired = cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr;
							
							List<Alternate> sortedFeasibleAlternates = new ArrayList<>();
							for(Alternate a : cp.getAlternates())
							{
								if(a == cp.getActiveAlternate()) continue;
								
								double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
								
								if(currentRequired > requriedCoreSeconds) continue;
								
								double valueToCost = a.getValue()/(a.getPeStandardCoreSecondPerMessage()* dr);
								
								int i = 0;
								for(; i < sortedFeasibleAlternates.size(); i++)
								{
									Alternate b = sortedFeasibleAlternates.get(i);
									if(valueToCost > b.getValue()/(b.getPeStandardCoreSecondPerMessage()*dr))
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
								
								/*if(selectedAlternate == null)
								{
									selectedAlternate = sortedFeasibleAlternates.get(0);
								}*/
							}
							
							
							if(selectedAlternate != null)
							{
								cp.setActiveAlternate(selectedAlternate);
								
								selectedAlternates.put(cp.getId(), selectedAlternate);
								
								//add to tcost
								tcost.put(cp.getId(), (float) (selectedAlternate.getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) cu);
								toChangeAlternate.add(cp);
							}
							else
							{
								tcost.put(cp.getId(), (float) (cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) cu);
							}
						}
						else
						{					
							Alternate selectedAlternate = null;
							
							double successor_cost = 0;
							for(Integer successor: successors)
							{
								successor_cost += tcost.get(successor);
							}
							
							double successor_available = 0;
							for(Integer successor: successors)
							{
								successor_available += tavailable.get(successor);
							}
							
							
							double currentRequired = cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr+ successor_cost;
							
							Map<Alternate,Double> valueToCostMap = new HashMap<>();
							List<Alternate> sortedFeasibleAlternates = new ArrayList<>();
							for(Alternate a : cp.getAlternates())
							{
								if(a == cp.getActiveAlternate()) continue;
								
								
								
								//currentCost = (float) (a.getPeStandardCoreSecondPerMessage() + a.getSelectivity()*successor_cost);
								
								double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr + a.getSelectivity()*successor_cost/cp.getActiveAlternate().getSelectivity();
								
								if(currentRequired > requriedCoreSeconds) continue;
								
								double valueToCost = a.getValue()/requriedCoreSeconds;
								valueToCostMap.put(a, valueToCost);
								
								int i = 0;
								for(; i < sortedFeasibleAlternates.size(); i++)
								{
									Alternate b = sortedFeasibleAlternates.get(i);
									if(valueToCost > valueToCostMap.get(b))
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
									//double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr;
									double requriedCoreSeconds = a.getPeStandardCoreSecondPerMessage() * dr + a.getSelectivity()*successor_cost;
									if(requriedCoreSeconds < (cu + successor_available))
									{
										selectedAlternate = a;
										break;
									}
								}
								
								/*if(selectedAlternate == null)
								{
									selectedAlternate = sortedFeasibleAlternates.get(0);
								}*/
							}
							
							
							if(selectedAlternate != null)
							{
								cp.setActiveAlternate(selectedAlternate);
								
								selectedAlternates.put(cp.getId(), selectedAlternate);
								//add to tcost
								tcost.put(cp.getId(), (float) (selectedAlternate.getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) (cu + successor_available));
								toChangeAlternate.add(cp);
							}
							else
							{
								tcost.put(cp.getId(), (float) (cp.getActiveAlternate().getPeStandardCoreSecondPerMessage() * dr));
								//add to available coreseconds
								tavailable.put(cp.getId(), (float) (cu + successor_available));
							}
						}
					}
					//Continue (reverse) BFS
					List<Integer> reverseSuccessors = Utils.getSuccessors(pstr,reverseEdges);
					for(Integer successor: reverseSuccessors)
					{
						if(!marks.containsKey(successor))
						{
							marks.put(successor, true);
							queue.add(successor);
						}
					}
				}
			}
			
		}
		else if(count%scalePhasePeriod == 0)
		{
			//Given the datarate and the alternates, choose to provision or shutdown (put on standby) the resources 
			//based on we are over or under provisioned.
			
			//For simple strategy.. just use local? information (but still use "all" instances)
			Map<Integer, List<DataflowProcessingElement>> nodeInstances = currentDeployment.getPeNodeToInstancesMap();
			

			Dataflow flow = currentDeployment.getDataflow();
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
					List<DataflowProcessingElement> bottleNeckPeList = currentDeployment.getPeNodeToInstancesMap().get(bottleNeck);
					
					Double currBottleNeckProcessRate = currProcessRates.get(bottleNeck);
					
					List<DataflowProcessingElement> newInstances = flow.incrementPEInstance(bottleNeck,1);
					currentDeployment.insertPEInstances(bottleNeck,newInstances);
					toAddList.addAll(newInstances);
					
					int newNumInstances = bottleNeckPeList.size();
					Double updatedProcessRates = currBottleNeckProcessRate * (newNumInstances)/(newNumInstances-1);
					
					currProcessRates.put(bottleNeck, updatedProcessRates);
					
					double preExpectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, currentDeployment.getAppGraph().getOutputPellets(), maxOutDataRate);
					//System.out.println(expectedOmega);
					
					RuntimeStatusUtils.processTick(currentDeployment.getAppGraph(),currentDeployment.getConcretePellets(),currInRates,currOutRates,currProcessRates,currDeltaQueueLengths);

					expectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, currentDeployment.getAppGraph().getOutputPellets(),maxOutDataRate);
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
						List<DataflowProcessingElement> tempOverProvisionedPeList = currentDeployment.getPeNodeToInstancesMap().get(overProvisioned);
						if(tempOverProvisionedPeList.size() > 1)
						{
							overProvisionedPeList = tempOverProvisionedPeList;
						}						
					}
					
					if(overProvisionedPeList == null || overProvisionedPeList.size() <= 1) break;
					
					
					Double currOverProvisionedProcessRate = currProcessRates.get(overProvisioned);
					
					//Ideally, choose the pe based on the VM.. for simple strategy.. just pick any
					List<DataflowProcessingElement> toRemove = new ArrayList<>();

					//At this point there will be atleast two instances.. so no need to check for "last" instance.
					DataflowProcessingElement toRemoveInstance = Utils.getToRemoveInstance(overProvisionedPeList);
					toRemove.add(toRemoveInstance);
					
					
					List<DataflowProcessingElement> removedInstances = flow.decrementPEInstance(toRemove);
					
					currentDeployment.removePEInstances(removedInstances);
					toRemoveList.addAll(removedInstances);
					
					int newNumInstances = overProvisionedPeList.size();
					Double updatedProcessRates = currOverProvisionedProcessRate * (newNumInstances)/(newNumInstances+1);
					
					currProcessRates.put(overProvisioned, updatedProcessRates);
					
					expectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, currentDeployment.getAppGraph().getOutputPellets(), maxOutDataRate);
					//System.out.println(expectedOmega);
					
					RuntimeStatusUtils.processTick(currentDeployment.getAppGraph(),currentDeployment.getConcretePellets(),currInRates,currOutRates,currProcessRates,currDeltaQueueLengths);

					expectedOmega = RuntimeStatusUtils.getExpectedOmega(currOutRates, currentDeployment.getAppGraph().getOutputPellets(),maxOutDataRate);
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
		count ++;
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
