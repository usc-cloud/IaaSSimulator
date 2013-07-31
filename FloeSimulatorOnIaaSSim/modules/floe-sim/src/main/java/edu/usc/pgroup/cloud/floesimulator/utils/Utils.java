package edu.usc.pgroup.cloud.floesimulator.utils;

import java.io.ObjectOutputStream.PutField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Alternate;
import edu.usc.pgroup.cloud.floesimulator.appmodel.ConcretePellet;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Pellet;
import edu.usc.pgroup.cloud.floesimulator.appmodel.PelletEdge;
import edu.usc.pgroup.cloud.floesimulator.appmodel.PelletInstance;
import edu.usc.pgroup.cloud.floesimulator.coordinator.Coordinator;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;

public class Utils {

	// for now assume one tick = 1 sec..
	private static long numTicksPerSecond = 1;

	public static long getNumberofTicks(long timeValue, TimeUnit unit) {
		long seconds = unit.toSeconds(timeValue);
		return seconds * numTicksPerSecond;
	}

	public static <T> List<List<T>> PermutationFinder(List<T> s) {
		List<List<T>> perm = new ArrayList<>();
		if (s == null) {
			// error case
			return null;
		} else if (s.size() == 0) {
			perm.add(new ArrayList<T>());
			// initial
			return perm;
		}
		T initial = s.get(0);

		// first character
		List<T> rem = s.subList(1, s.size());

		// Full string without first character
		List<List<T>> words = PermutationFinder(rem);
		for (List<T> str : words) {
			for (int i = 0; i <= str.size(); i++) {
				perm.add(charinsert(str, initial, i));
			}
		}
		return perm;
	}

	public static <T> List<T> charinsert(List<T> str, T c, int j) {
		// String begin = str.substring(0, j);
		List<T> begin = new ArrayList<>(str.subList(0, j));
		// String end = str.substring(j);
		List<T> end = str.subList(j, str.size());
		begin.add(c);
		begin.addAll(end);
		return begin;
	}


	public static Pellet findNode(Collection<? extends Pellet> pellets2,
			int id) {
		for (Pellet p : pellets2) {
			if (p.getId() == id)
				return p;
		}
		return null;
	}

	public static double getSigma(AbstractDynamicDataflow app, double dollarAtMaxValue,
			double dollarAtMinValue) {
		double totalMinValue = 0;
		for (Pellet p : app.getPellets()) {
			double minValue = Double.POSITIVE_INFINITY;
			for (Alternate a : p.getAlternates()) {
				if (a.getValue() < minValue) {
					minValue = a.getValue();
				}
			}
			totalMinValue += minValue;
		}
		totalMinValue = totalMinValue / (double) app.getPellets().size();

		double sigma = (1 - totalMinValue) / (dollarAtMaxValue - dollarAtMinValue);
		//double sigma = 1/dollarAtMaxValue;
		return sigma;
	}

	public static List<Integer> getSuccessors(Integer id,
			List<PelletEdge> edges) {
		List<Integer> successors = new ArrayList<>();
		for (PelletEdge e : edges) {
			if (e.getSrcNodeId() == id)
				successors.add(e.getDestNodeId());
		}
		return successors;
	}	

	public static double getMaxOutputDataRate(AbstractDynamicDataflow graph,
			List<ConcretePellet> concretePellets,
			Map<Integer, Double> inputDataRate) {

		Map<Integer, Double> outDataRate = getMaxDataRateAtPellet(graph,graph.getOutputPellets(),concretePellets,inputDataRate);
		double total = 0;
		for(Integer nodeId: outDataRate.keySet())
		{
			total += outDataRate.get(nodeId);
		}
		return total;
	}
	
	public static Map<Integer,Double> getMaxDataRateAtAllPellet(
			AbstractDynamicDataflow appGraph,
			List<ConcretePellet> concretePellets,
			Map<Integer, Double> inputDataRate) {
	
		List<Integer> pellets = new ArrayList<>();
		for(ConcretePellet cp : concretePellets)
		{
			pellets.add(cp.getId());
		}
		
		return getMaxDataRateAtPellet(appGraph, pellets, concretePellets, inputDataRate);
		
	}

	public static Map<Integer,Double> getMaxDataRateAtPellet(AbstractDynamicDataflow graph, List<Integer> nodeIds,
			List<ConcretePellet> concretePellets,
			Map<Integer, Double> inputDataRate) {

		Queue<Pellet> queue = new LinkedBlockingQueue<Pellet>();

		Map<Integer, Boolean> marks = new HashMap<>();

		Map<Integer, Double> inputDataRateMap = new HashMap<>();

		for (ConcretePellet cp : concretePellets) {
			if (graph.getInputPellets().contains(cp.getId())) {
				inputDataRateMap.put(cp.getId(), inputDataRate.get(cp.getId()));
				marks.put(cp.getId(), true);
				queue.add(cp);
			}
		}

		while (queue.isEmpty() == false) {
			ConcretePellet p = (ConcretePellet) queue.remove();

			double selectivity = p.getActiveAlternate().getSelectivity();

			List<Integer> successors = getSuccessors(p.getId(),graph.getEdges());
			for (Integer successor : successors) {
				//System.out.println(successor);
				//System.out.println(p.getId());
				inputDataRateMap.put(successor, inputDataRateMap.get(p.getId())
						* selectivity + (inputDataRateMap.containsKey(successor) ? inputDataRateMap.get(successor) : 0));

				if (!marks.containsKey(successor)) {
					marks.put(successor, true);
					queue.add(findNode(concretePellets, successor));
				}
			}
		}

		Map<Integer,Double> outDataRate = new HashMap<>();
		for (Integer pellet : nodeIds) {
			ConcretePellet p = (ConcretePellet) findNode(concretePellets, pellet);
			double in = inputDataRateMap.get(pellet);
			double out2 = in * p.getActiveAlternate().getSelectivity();
			
			outDataRate.put(pellet, out2);
		}


		return outDataRate;
	}
	
	public static double getAppValue(AbstractDynamicDataflow graph,
			List<ConcretePellet> concretePellets) {
		double value = 0;
		for (ConcretePellet p : concretePellets) {
			value += p.getActiveAlternate().getValue();
		}
		return value / concretePellets.size();
	}
	
	public static List<Integer> getBFSOrder(AbstractDynamicDataflow graph) {

		Queue<Integer> queue = new LinkedBlockingQueue<Integer>();

		Map<Integer, Boolean> marks = new HashMap<>();

		List<Integer> bfsOrder = new ArrayList<>();

		for (Integer cp : graph.getInputPellets()) {
			marks.put(cp, true);
			queue.add(cp);
		}

		while (queue.isEmpty() == false) {
			Integer p = queue.remove();

			bfsOrder.add(p);

			List<Integer> successors = getSuccessors(p,graph.getEdges());
			for (Integer successor : successors) {
				if (!marks.containsKey(successor)) {
					marks.put(successor, true);
					queue.add(successor);
				}
			}
		}

		return bfsOrder;
	}
	
	public static double getExpectedRelativeApplicationThroughput(
			AbstractDynamicDataflow graph, List<ConcretePellet> concretePellets,
			double maxOutputDataRate) {

		double totalOutput = 0;
		for (Integer out : graph.getOutputPellets()) {
			ConcretePellet o = (ConcretePellet) findNode(concretePellets, out);
			totalOutput += o.getTotalOutputDataRate();
		}

		totalOutput /= graph.getOutputPellets().size();
		return totalOutput / maxOutputDataRate;
	}
	
	public static double getTotalExpectedCost(
			Map<Integer, VM> virtualMachineInstances) {

		double totalExpectedCost = 0;
		for (VM vm : virtualMachineInstances.values()) {
			totalExpectedCost += vm.getVMClass().getCostPerHour();
		}

		return totalExpectedCost;
	}
	
	public static List<Integer> findBottlenecks(List<ConcretePellet> concretePellets) {

		List<Integer> bottleNecks = new ArrayList<>();
		

		Map<Integer,Double> delqs = new HashMap<>(); 
		for (ConcretePellet p : concretePellets) {
			double delq = p.getOverallDeltaQueueLength();			
			if(delq<=0) continue;
			
			delqs.put(p.getId(), delq);
			
			int i = 0;
			for(; i < bottleNecks.size(); i++)
			{
				if(delq > delqs.get(bottleNecks.get(0))) break;
			}
			
			bottleNecks.add(i, p.getId());
		}
//		for(Integer bn: bottleNecks)
//		{
//			System.out.print("BT=>" + bn + ": dq="+delqs.get(bn) + "; ");
//			break;
//		}
//		System.out.println("");
		return bottleNecks;
	}
	
	public static Map<Integer, Double> findOverProvisioned(List<ConcretePellet> concretePellets) {

		Map<Integer,Double> delqs = new HashMap<>(); 
		for (ConcretePellet p : concretePellets) {
			double delq = p.getOverallDeltaQueueLength();			
			if(delq>=0) continue;
			
			delqs.put(p.getId(), delq);
		}
		

		return delqs;
	}
	
	public static void printPelletMapping(
			Map<Integer, Map<Integer, Integer>> pelletVMMapping) {

		for (Integer x : pelletVMMapping.keySet()) {
			System.out.print(x + "=>");
			for (Integer y : pelletVMMapping.get(x).keySet()) {
				System.out.print(y + ":" + pelletVMMapping.get(x).get(y)
						+ ",");
			}
		}
		System.out.println("");
	}

	public static double getTotalCost(List<VM> vMs) {
		float totalCost = 0;
		for(VM vm: vMs)
		{			
			totalCost += Math.ceil(((vm.getShutdownTime() == -1 ? IaaSSimulator.getClock() : vm.getShutdownTime()) - vm.getStartTime())/(60*60)) * vm.getVMClass().getCostPerHour();
		}
		return totalCost;
	}

	public static int ProcessTick(AbstractDynamicDataflow graph,
			Map<Integer, Map<Integer, Integer>> pelletVMMapping,
			List<ConcretePellet> concretePellets,
			Map<Integer, VM> virtualMachineInstances,
			Map<Integer, Double> inputSources, double maxOutputDataRate) {

		
		Queue<Pellet> queue = new LinkedBlockingQueue<Pellet>();

		Map<Integer, Boolean> marks = new HashMap<>();

		for (ConcretePellet cp : concretePellets) {
			Map<Integer, Integer> vmMapping = pelletVMMapping.get(cp.getId());
			if(vmMapping == null) return -1;
			cp.clearePelletInstance();
			
			for (Integer vmid : vmMapping.keySet()) {
				VM vm = virtualMachineInstances.get(vmid);

				for (int i = 0; i < vmMapping.get(vmid); i++) {
					PelletInstance pi = new PelletInstance(
							cp.getActiveAlternate(), vm);
					cp.addPelletInstance(pi);
				}
			}
			
			if (cp.getPelletInstances().size() == 0)
				return -1;

			if (graph.getInputPellets().contains(cp.getId())) {
				cp.distributeDataRate(inputSources.get(cp.getId()));
				cp.processTickAllInstances();

				List<Integer> successors = getSuccessors(cp.getId(),graph.getEdges());
				for (Integer successor : successors) {
					ConcretePellet s = (ConcretePellet) Utils.findNode(concretePellets, successor);
					cp.transmitDataOnEdge(s);
				}

				marks.put(cp.getId(), true);
				queue.add(cp);
			}

		}

		while (queue.isEmpty() == false) {
			ConcretePellet p = (ConcretePellet) queue.remove();

			p.processTickAllInstances();

			List<Integer> successors = getSuccessors(p.getId(),graph.getEdges());
			for (Integer successor : successors) {
				ConcretePellet s = (ConcretePellet) Utils.findNode(
						concretePellets, successor);
				p.transmitDataOnEdge(s);

				if (!marks.containsKey(s.getId())) {
					marks.put(s.getId(), true);
					queue.add(s);
				}
			}
		}
		
		return 0;
	}

	
	
	
	static int instanceId = 0;
	public static void putPelletOnVM(Map<Integer, Map<Integer, Integer>> pelletVMMapping, Integer pelletId,
			VM lastVirtualMachine) {
		
		Map<Integer, Integer> VMMap = pelletVMMapping.get(pelletId);
		
		if(VMMap == null)
		{
			VMMap = new HashMap<>();
			pelletVMMapping.put(pelletId, VMMap);
		}
		
		Integer count = VMMap.get(lastVirtualMachine.getId());
		if(count == null)
			VMMap.put(lastVirtualMachine.getId(), 1);
		else
			VMMap.put(lastVirtualMachine.getId(), count+1);
		
		lastVirtualMachine.allocateCore(pelletId, instanceId ++);
		
		//getCpuProperties().setAvailableCoreCount(lastVirtualMachine.getCpuProperties().getAvailableCoreCount()-1);
	}
	
	static int vmId = 11000;
	
	public static Map<Integer, VM> repack(Map<Integer, Double> allocatedPellets, VM lastVirtualMachine,  
			Map<Integer, Map<Integer, Integer>> pelletVMMapping, List<VMClass> sortedVMClasses, Map<Integer, Integer> pelletUnitCounts) {
		
		Map<Integer, VM> vmInstances = new HashMap<>();
		
		VMClass topVMClass = sortedVMClasses.remove(0);
		
		
		VM currentLastVM = null;
				
		//TODO: change to bfs order.. 
		while(allocatedPellets.size() > 0)
		{
			List<Integer> toRemove = new ArrayList<>();
			for(int pellet : allocatedPellets.keySet())
			{
				Map<Integer, Integer> VMMapping = pelletVMMapping.get(pellet);
				if(VMMapping.containsKey((Integer)lastVirtualMachine.getId()))
				{
					Integer count = VMMapping.remove((Integer)lastVirtualMachine.getId());
					pelletUnitCounts.put(pellet,pelletUnitCounts.get(pellet)-count);
				}
				
				if(currentLastVM == null || !currentLastVM.isCoreAvailable())
				{
					currentLastVM = new VM(vmId++, 1, topVMClass, null);
					vmInstances.put(currentLastVM.getId(),currentLastVM);				
				}
	
				Utils.putPelletOnVM(pelletVMMapping, pellet, currentLastVM);
				pelletUnitCounts.put(pellet,pelletUnitCounts.get(pellet)+1);
				
				allocatedPellets.put(pellet,allocatedPellets.get(pellet) - currentLastVM.getVMClass().getCoreCoeff());
				
				if(allocatedPellets.get(pellet) <= 0)
				{
					toRemove.add(pellet);
				}
			}
			for(int pellet: toRemove)
			{
				allocatedPellets.remove(pellet);
			}
		}
		
		if(currentLastVM.isCoreAvailable() && sortedVMClasses.size() > 0)	
		{
			vmInstances.remove(currentLastVM.getId());
			vmInstances.putAll(repack(currentLastVM.getAllocatedPellets(), currentLastVM, pelletVMMapping, sortedVMClasses, pelletUnitCounts));
		}

		return vmInstances;
	}

	public static DataflowProcessingElement getToRemoveInstance(
			List<DataflowProcessingElement> overProvisionedPeList) {

		int N = overProvisionedPeList.size();
		float currentTime = IaaSSimulator.getClock();
		
		Map<Integer,Integer> VMMapping = new HashMap<>(); 
		for(DataflowProcessingElement pe: overProvisionedPeList)
		{
			VMMapping.put(pe.getVm().getId(),(VMMapping.containsKey(pe.getVm().getId())?VMMapping.get(pe.getVm().getId()):0)+1);			
		}
		float minValue = Float.MAX_VALUE;
		DataflowProcessingElement bestPE = null;
		for(DataflowProcessingElement pe: overProvisionedPeList)
		{
			VM vm = pe.getVm();
			
			float timeLeftInCurrentCycle = (float) Math.ceil((int)(IaaSSimulator.getClock() - vm.getStartTime())%(60*60));			
			int numPEInstancesOnVM = VMMapping.get(vm.getId());			
			int numFreeCores = vm.getAvailableCores();
			float costPerStandardCore = vm.getVMClass().getCostPerHour()/(vm.getVMClass().getCoreCount() * vm.getVMClass().getCoreCoeff());
			
			float normalizedTimeLeft = timeLeftInCurrentCycle/(60*60);
			float normalizedPEContribution = (float)numPEInstancesOnVM/(float)N;
			float normalizedFreeCores = 1.0f - (float)numFreeCores/vm.getVMClass().getCoreCount();
			float normalizedCostPerStandardCore = costPerStandardCore/VMClasses.getStandard().getCostPerHour();
			
			float value = (normalizedTimeLeft * normalizedPEContribution * normalizedFreeCores)/normalizedCostPerStandardCore;
			//System.out.println(String.format("id:%d time:%f pes:%f freeC:%f cost:%f vale:%f",vm.getId(),normalizedTimeLeft,normalizedPEContribution, normalizedFreeCores, costPerStandardCore, value));
			
			if(value < minValue)
			{
				minValue = value;
				bestPE = pe;
			}
		}
		
		return bestPE;
	}

	public static List<VM> checkVMShutdowns(Map<Integer, VM> vms) {
		List<VM> vmsToShutdown = new ArrayList<>();
		for(VM vm: vms.values())
		{
			if(vm.getAvailableCores() != vm.getVMClass().getCoreCount()) continue;
			
			if(isCloseToCostCycle(vm))
			{
				vmsToShutdown.add(vm);
			}
		}
		return vmsToShutdown;
	}

	private static boolean isCloseToCostCycle(VM vm) {
		float timeLeftInCurrentCycle = (float) Math.ceil((int)(IaaSSimulator.getClock() - vm.getStartTime())%(60*60));
		if(timeLeftInCurrentCycle <= FloeConstants.OptimizationTimeSlotInterval) return true;
		return false;
	}


}
