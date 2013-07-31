package edu.usc.pgroup.cloud.floesimulator.appmodel;

import java.util.HashMap;
import java.util.Map;

import edu.usc.pgroup.cloud.floesimulator.utils.FloeConstants;
import edu.usc.pgroup.cloud.iaassim.vm.VM;


public class PelletInstance {

	Map<Double,Double> currentInputDataRate = new HashMap<>();
	Double currentOutputDataRate = 0.0;
	Alternate activeAlternate;
	VM vm;
	private double deltaQueueLength;
	
	public PelletInstance(Alternate activeAlternate, VM vm) {		
		this.activeAlternate = activeAlternate;
		this.vm = vm;
	}

	
	public Double getCurrentOutputDataRate() {
		return currentOutputDataRate;
	}
	
	public void processTick() {
		double tickLength = FloeConstants.OfflineCoordinatorTickLength;
		double coreRequirementsPerMessages = activeAlternate.getPeStandardCoreSecondPerMessage();
		double coreCoeff = vm.getVMClass().getCoreCoeff();
		double selectivity = activeAlternate.getSelectivity();
		double availableCoreSecondsPerTick = coreCoeff * tickLength;
		
		//Number of messages processed per tick = coreCoeff * tickLengh/require core seconds per message;
		
		for(double start : currentInputDataRate.keySet())
		{
			double numMessagesProcessed = availableCoreSecondsPerTick * (tickLength - start)/coreRequirementsPerMessages;
			
			
			if(numMessagesProcessed > currentInputDataRate.get(start) * tickLength)
			{
				currentOutputDataRate += currentInputDataRate.get(start) * tickLength * selectivity;
				
			}
			else
			{
				currentOutputDataRate = numMessagesProcessed * selectivity;
				deltaQueueLength += currentInputDataRate.get(start) - numMessagesProcessed/(tickLength-start);
			}			
		}
	}

	public void addToInputDataRate(double output) {
		addToInputDataRate(0, output);
	}
	
	public void addToInputDataRate(double start, double output) {
		currentInputDataRate.put(start, output + (currentInputDataRate.containsKey(start)?currentInputDataRate.get(start):0)); 
	}

	public double getDeltaQueueLength() {
		return deltaQueueLength;
	}
	
	public VM getVm() {
		return vm;
	}
}
