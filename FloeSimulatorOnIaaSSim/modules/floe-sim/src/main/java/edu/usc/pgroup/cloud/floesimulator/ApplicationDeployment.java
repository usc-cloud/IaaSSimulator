package edu.usc.pgroup.cloud.floesimulator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Alternate;
import edu.usc.pgroup.cloud.floesimulator.appmodel.ConcretePellet;
import edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms.DeploymentAlgorithm;
import edu.usc.pgroup.cloud.floesimulator.utils.FloeConstants;
import edu.usc.pgroup.cloud.floesimulator.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.Dataflow;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;

public class ApplicationDeployment {

	String deploymentId;
	List<ConcretePellet> concretePellets;
	Map<Integer, Map<Integer, Integer>> pelletVMMapping;
	Map<Integer,VMClass> vmInstanceClassMapping;
	private AbstractDynamicDataflow appGraph;
	private Map<Integer, Double> inputSources;
	private double sigma;
	private Dataflow dataflow;
	private Map<Integer, VM> vms;
	private Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap;
	
	private Map<Integer,Double> maxOutDataRates;
	private Map<Integer,Double> currentOutDataRates;
	private Map<Integer,Double> currentInputRates;
	private Map<Integer,Double> currentQueueLengths;
	private Map<Integer,Double> currentNumMsgProcessedPerSec;
	
	private double omega = 0;
	private DeploymentAlgorithm deploymentAlgorithm;
	
	
	
	
	
	public ApplicationDeployment(String deploymentId, AbstractDynamicDataflow appGraph) {
		this.deploymentId = deploymentId;
		this.appGraph = appGraph;
		this.maxOutDataRates = new HashMap<>();
		
		this.currentOutDataRates = new HashMap<>();
		this.currentInputRates = new HashMap<>();
		this.currentQueueLengths = new HashMap<>();
		this.currentNumMsgProcessedPerSec = new HashMap<>();
		
		
	}

	public String getDeploymentId() {
		return deploymentId;
	}
	
	public void setDeploymentId(String deploymentId) {
		this.deploymentId = deploymentId;
	}

	public void setConcretePellets(List<ConcretePellet> concretePellets) {
		this.concretePellets = concretePellets;
	}

	public void setPelletVMMapping(Map<Integer, Map<Integer, Integer>> pelletVMMapping) {
		this.pelletVMMapping = pelletVMMapping;
	}
	
	public Map<Integer, Map<Integer, Integer>> getPelletVMMapping() {
		return pelletVMMapping;
	}
	
	public Map<Integer, VMClass> getVmInstanceClassMapping() {
		return vmInstanceClassMapping;
	}
	public void setVmInstanceClassMapping(
			Map<Integer, VMClass> vmInstanceClassMapping) {
		this.vmInstanceClassMapping = vmInstanceClassMapping;
	}

	public List<ConcretePellet> getConcretePellets() {
		return concretePellets;
	}

	public List<Integer> getSuccessors(int id) {
		return Utils.getSuccessors(id, appGraph.getEdges());
	}

	public void setInputSources(Map<Integer, Double> inputSources) {
		this.inputSources = inputSources;
	}

	public Map<Integer, Double> getInputSources() {
		return this.inputSources;
	}
	
	public AbstractDynamicDataflow getAppGraph() {
		return appGraph;
	}

	public void setSigma(double sigma) {
		this.sigma = sigma;
	}
	
	public double getSigma() {
		return sigma;
	}

	public void setDataflow(Dataflow flow) {
		this.dataflow = flow;		
	}
	
	public Dataflow getDataflow() {
		return dataflow;
	}

	public void setVms(Map<Integer, VM> vms) {
		this.vms = vms;
	}
	public Map<Integer, VM> getVms() {
		return vms;
	}

	public Map<Integer, List<DataflowProcessingElement>> getPeNodeToInstancesMap() {
		return peNodeToInstancesMap;
	}
	
	public void setPeNodeToInstancesMap(
			Map<Integer, List<DataflowProcessingElement>> peNodeToInstancesMap) {
		this.peNodeToInstancesMap = peNodeToInstancesMap;
	}


	public void insertPEInstances(int id,
			List<DataflowProcessingElement> newInstances) {
		this.peNodeToInstancesMap.get(id).addAll(newInstances);
	}	
	
	public void addCurrentMaxOutDataRates(Map<Integer, Double> aMaxOutDataRates) {
		for(Integer nodeId: aMaxOutDataRates.keySet())
		{
			this.maxOutDataRates.put(nodeId, aMaxOutDataRates.get(nodeId) + ((maxOutDataRates.containsKey(nodeId))?maxOutDataRates.get(nodeId):0));
		}
	}
	
	public Map<Integer, Double> getMaxOutDataRates() {
		return maxOutDataRates;
	}

	public void addCurrentOutDataRates(Map<Integer, Double> aCurrentOutDataRates) {
		for(Integer nodeId: aCurrentOutDataRates.keySet())
		{
			double value = aCurrentOutDataRates.get(nodeId) + ((currentOutDataRates.containsKey(nodeId))?currentOutDataRates.get(nodeId):0);
			this.currentOutDataRates.put(nodeId, value);
		}
	}
	public void addCurrentQueueLength(Map<Integer, Double> aCurrentQueueLengths) {
		for(Integer nodeId: aCurrentQueueLengths.keySet())
		{			
			double value = aCurrentQueueLengths.get(nodeId) + ((currentQueueLengths.containsKey(nodeId))?currentQueueLengths.get(nodeId):0);
			this.currentQueueLengths.put(nodeId, value);
		}
	}
	public void addCurrentInputRates(Map<Integer, Double> aCurrentInputRates) {
		for(Integer nodeId: aCurrentInputRates.keySet())
		{
			double value = aCurrentInputRates.get(nodeId) + ((currentInputRates.containsKey(nodeId))?currentInputRates.get(nodeId):0);
			this.currentInputRates.put(nodeId, value);
		}
	}
	
	public void addCurrentMessgaesProcessedPerSecond(
			Map<Integer, Double> aCurrentNumMsgProcessedPerSec) {
		for(Integer nodeId: aCurrentNumMsgProcessedPerSec.keySet())
		{
			double value = aCurrentNumMsgProcessedPerSec.get(nodeId) + ((currentNumMsgProcessedPerSec.containsKey(nodeId))?currentNumMsgProcessedPerSec.get(nodeId):0);
			this.currentNumMsgProcessedPerSec.put(nodeId, value);
		}
	}
	
	public Map<Integer, Double> getCurrentNumMsgProcessedPerSec() {
		return currentNumMsgProcessedPerSec;
	}
	
	public Map<Integer, Double> getCurrentOutDataRates() {
		return currentOutDataRates;
	}
	
	public Map<Integer, Double> getCurrentInputRates() {
		return currentInputRates;
	}

	public Map<Integer, Double> getCurrentQueueLengths() {
		return currentQueueLengths;
	}
	
	public void addCurrentOmega(double omega) {
		this.omega += omega;
	}
	
	public double getOmega() {
		return omega;
	}

	public void resetCurrentStats() {
		for(Integer nodeId: currentOutDataRates.keySet())
		{
			
			this.maxOutDataRates.put(nodeId, 0.0);
			this.currentOutDataRates.put(nodeId, 0.0);
			this.currentInputRates.put(nodeId, 0.0);
			this.currentQueueLengths.put(nodeId, 0.0);;
			this.currentNumMsgProcessedPerSec.put(nodeId,0.0);
		}
		this.omega = 0;
	}

	public void removePEInstances(
			List<DataflowProcessingElement> removedInstances) {
		for(DataflowProcessingElement dpe: removedInstances)
		{
			peNodeToInstancesMap.get(dpe.getNodeId()).remove(dpe);
		}
	}

	public void setDeploymentAlgorithm(DeploymentAlgorithm algorithm) {
		deploymentAlgorithm = algorithm;
	}
	
	public DeploymentAlgorithm getDeploymentAlgorithm() {
		return deploymentAlgorithm;
	}

	public double getMaxOutDataRateForOutPellets() {
		double outDataRate = 0;
		for(Integer nodeId: appGraph.getOutputPellets())
		{
			outDataRate += maxOutDataRates.get(nodeId);
		}
		return outDataRate;
	}

	public double getCurrentOutDataRateForOutPellets() {
		double outDataRate = 0;
		for(Integer nodeId: appGraph.getOutputPellets())
		{
			outDataRate += currentOutDataRates.get(nodeId);
		}
		return outDataRate;
	}
	
//	public Map<Integer, Double> getCurrentInputRatesAtInputPellets() {
//		Map<Integer, Double> outDataRate = new HashMap<>();
//		for(Integer nodeId: appGraph.getInputPellets())
//		{
//			outDataRate.put(nodeId, currentInputRates.get(nodeId)/(FloeConstants.OptimizationTimeSlotInterval/FloeConstants.MonitoringTimeSlotInterval));
//		}
//		return outDataRate;
//	}
//
//	
//	
//
//	
}
