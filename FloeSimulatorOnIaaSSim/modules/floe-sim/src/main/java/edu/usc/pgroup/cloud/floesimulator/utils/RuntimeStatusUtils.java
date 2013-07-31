package edu.usc.pgroup.cloud.floesimulator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.appmodel.ConcretePellet;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Pellet;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;

public class RuntimeStatusUtils {
	
	public static List<Integer> findBottlenecks(
			Map<Integer,Double> inQLengths) {
		
		List<Integer> bottleNecks = new ArrayList<>();
		

		Map<Integer,Double> delqs = new HashMap<>();
		
		
		for (Integer nodeId : inQLengths.keySet()) {

			double currentQueueLength = inQLengths.get(nodeId);
			//System.out.println("delq:" + nodeId + ": " + currentQueueLength);
			if(currentQueueLength<=0) continue;
			
			delqs.put(nodeId, currentQueueLength);
			
			int i = 0;
			for(; i < bottleNecks.size(); i++)
			{
				if(currentQueueLength > delqs.get(bottleNecks.get(0))) break;
			}
			
			bottleNecks.add(i, nodeId);
		}

		return bottleNecks;		
	}

	public static Map<Integer,Double> getOutputDataRatesAtAllPellets(
			Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap) {

		Map<Integer,Double> outDataRates = new HashMap<>();
		for(Integer pellet: peNodeToInstancesMap.keySet())
		{
			double dataRate = 0;
			for(DataflowProcessingElement peInstance: peNodeToInstancesMap.get(pellet))
			{
				dataRate += peInstance.getOutDataRate(IaaSSimulator.getClock());
			}
			outDataRates.put(pellet,dataRate);
		}
		return outDataRates;
	}
	
	public static Map<Integer, Double> getCurrentDeltaQueueLengths(
			Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap) {
	

		Map<Integer,Double> currentDeltaQLengths = new HashMap<>();
		
		for(Integer nodeId : peNodeToInstancesMap.keySet()) {

			List<DataflowProcessingElement> instances = peNodeToInstancesMap.get(nodeId);
			
			double currentDeltaQueueLength = 0;
			
			for(DataflowProcessingElement peInstance: instances)
			{
				currentDeltaQueueLength += peInstance.getDeltaQLength(IaaSSimulator.getClock());
			}
			
			currentDeltaQLengths.put(nodeId, currentDeltaQueueLength);
		}

		return currentDeltaQLengths;	
	}

	public static Map<Integer, Double> getCurrentAvailableCoreUnitsPerNode(
			Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap) {
		Map<Integer,Double> coreUnits = new HashMap<>();
		
		for(Integer nodeId : peNodeToInstancesMap.keySet()) {

			List<DataflowProcessingElement> instances = peNodeToInstancesMap.get(nodeId);
			
			double currCoreUnits = 0;
			
			for(DataflowProcessingElement peInstance: instances)
			{
				currCoreUnits += peInstance.getVm().getCurrentCoreCoeff();//getVMClass().getCoreCoeff();//todo: change this to .getCurrentCoreCoeff();
			}
			
			coreUnits.put(nodeId, currCoreUnits);
		}

		return coreUnits;	
	}
	
	public static Map<Integer, Double> getCurrentInputRates(
			Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap) {
		
		Map<Integer,Double> curentInputRates = new HashMap<>();
		
		
		for (Integer nodeId : peNodeToInstancesMap.keySet()) {

			List<DataflowProcessingElement> instances = peNodeToInstancesMap.get(nodeId);
			
			double currentQueueLength = 0;
			
			for(DataflowProcessingElement peInstance: instances)
			{
				currentQueueLength += peInstance.getDataRate(IaaSSimulator.getClock());
			}

			if(currentQueueLength<=0) continue;
			
			curentInputRates.put(nodeId, currentQueueLength);
		}

		return curentInputRates;	
	}
	

	public static Map<Integer, Double> getNumMsgProcessedPerSec(
			Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap) {
		Map<Integer,Double> numMsgsProcessedPerSec = new HashMap<>();
		
		
		for (Integer nodeId : peNodeToInstancesMap.keySet()) {

			List<DataflowProcessingElement> instances = peNodeToInstancesMap.get(nodeId);
			
			double currentQueueLength = 0;
			
			for(DataflowProcessingElement peInstance: instances)
			{
				currentQueueLength += peInstance.numMsgsProcessedPerSec(IaaSSimulator.getClock());
			}

			if(currentQueueLength<=0) continue;
			
			numMsgsProcessedPerSec.put(nodeId, currentQueueLength);
		}

		return numMsgsProcessedPerSec;
	}

	public static double getExpectedOmega(Map<Integer, Double> currOutRates,
			List<Integer> outputPellets, double maxOutDataRate) {
		
		double outDataRate = 0;
		for(Integer nodeId: outputPellets)
		{
			outDataRate += currOutRates.get(nodeId);
		}
		
		
		return outDataRate/maxOutDataRate;
	}

	public static void processTick(AbstractDynamicDataflow graph, List<ConcretePellet> concretePellets,
			Map<Integer, Double> currInRates,
			Map<Integer, Double> currOutRates,
			Map<Integer, Double> currProcessRates,
			Map<Integer, Double> currDeltaQueueLengths) {
		
		Queue<Integer> queue = new LinkedBlockingQueue<Integer>();
		Map<Integer, Boolean> marks = new HashMap<>();
		
		
		//TODO: UPDATE DELTA QUEUE HERE.. 
		
		for(Integer pellet: graph.getInputPellets())
		{
			
			//get num messages processed
			double numMessagesProcessed = Math.min(currInRates.get(pellet), currProcessRates.get(pellet));

			currDeltaQueueLengths.put(pellet, currInRates.get(pellet) - numMessagesProcessed);
			
			double numMessagesProduced = numMessagesProcessed * ((ConcretePellet)Utils.findNode(concretePellets, pellet)).getActiveAlternate().getSelectivity();
			currOutRates.put(pellet, numMessagesProduced);
			
			List<Integer> successors = Utils.getSuccessors(pellet, graph.getEdges());			
			for (Integer successor : successors) {				
				//transmit to the successor
				//NOTE: HOW TO ACCOUNT FOR TRANSFER LATENCY?
				currInRates.put(successor, Math.max(currInRates.get(successor),numMessagesProduced));
			}
			
			
			marks.put(pellet, true);
			queue.add(pellet);
		}
		
		
		while (queue.isEmpty() == false) {
			Integer pellet = (Integer) queue.remove();

			//get num messages processed
			double numMessagesProcessed = Math.min(currInRates.get(pellet), currProcessRates.get(pellet));

			currDeltaQueueLengths.put(pellet, currInRates.get(pellet) - numMessagesProcessed);
			
			double numMessagesProduced = numMessagesProcessed * ((ConcretePellet)Utils.findNode(concretePellets, pellet)).getActiveAlternate().getSelectivity();
			currOutRates.put(pellet, numMessagesProduced);
			

			List<Integer> successors = Utils.getSuccessors(pellet, graph.getEdges());			
			for (Integer successor : successors) {				
				//transmit to the successor
				//NOTE: HOW TO ACCOUNT FOR TRANSFER LATENCY?
				currInRates.put(successor, Math.max(currInRates.get(successor),numMessagesProcessed));

				if (!marks.containsKey(successor)) {
					marks.put(successor, true);
					queue.add(successor);
				}
			}
		}
	}

	public static List<Integer> findOverProvisioned(
			Map<Integer, Double> currQueueLengths) {
		List<Integer> bottleNecks = new ArrayList<>();
		

		Map<Integer,Double> delqs = new HashMap<>();
		
		
		for (Integer nodeId : currQueueLengths.keySet()) {

			double currentQueueLength = currQueueLengths.get(nodeId);
			
			delqs.put(nodeId, currentQueueLength);
			
			int i = 0;
			for(; i < bottleNecks.size(); i++)
			{
				if(currentQueueLength < delqs.get(bottleNecks.get(0))) break;
			}
			
			bottleNecks.add(i, nodeId);
		}

		return bottleNecks;		
	}

	
	
	
}
