package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.usc.pgroup.cloud.iaassim.ExecutionTrigger;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.utils.LogEntity;
import edu.usc.pgroup.cloud.iaassim.utils.PerfLog;
import edu.usc.pgroup.cloud.iaassim.utils.Utils;

public class DataflowProcessingElement extends ProcessingElement{

	List<Edge> outEdges;
	float sM; //number of messages to consume per execution
	float sN; //number of messages to emitt
	private int edgeCount;
	private float selectivity;
	
	private long inQLength = 0;
	
	List<Float> previousMsgTimings;
	List<Float> previousOutMsgTimings;
	private DataSource dataSource;
	List<Float> previousDequeMsgTimings;
	
	
	public DataflowProcessingElement(int id, 
			float standardCoreSecondPerMessage, int numCores,
			boolean isContinuous, ExecutionTrigger trigger, float selectivity) {
		this(id,id,standardCoreSecondPerMessage,numCores,isContinuous,trigger,selectivity);
		
	}
	
	public DataflowProcessingElement(int id, int nodeid,
			float standardCoreSecondPerMessage, int numCores,
			boolean isContinuous, ExecutionTrigger trigger, float selectivity) {
		super(id, nodeid, standardCoreSecondPerMessage, numCores, isContinuous, trigger);

		outEdges = new ArrayList<>();
		edgeCount = 0;
		
		this.selectivity = selectivity;
		if(selectivity < 0)
		{
			throw new IllegalArgumentException("Selectivty cannot be negative:" + selectivity);
		}
		else if(selectivity >= 1)
		{
			sM = 1;
			sN = selectivity;
		}
		else
		{
			sM = 1/selectivity;
			sN = 1;
		}
		
		previousMsgTimings = new ArrayList<>();			
		previousOutMsgTimings = new ArrayList<>();
		previousDequeMsgTimings = new ArrayList<>();
	}

	public void updatePE(float peStandardCoreSecondPerMessage,
			float selectivity) {
		this.selectivity = selectivity;
		this.peStandardCoreSecondPerMessage = peStandardCoreSecondPerMessage;
		
		if(selectivity < 0)
		{
			throw new IllegalArgumentException("Selectivty cannot be negative:" + selectivity);
		}
		else if(selectivity >= 1)
		{
			sM = 1;
			sN = selectivity;
		}
		else
		{
			sM = 1/selectivity;
			sN = 1;
		}
	}
	
	public Edge createEmptyEdge(int destPeId)
	{
		Edge e = new Edge(edgeCount++, destPeId,this);
		outEdges.add(e);
		return e;
	}
	
	@Override
	protected Object clone(){
		return new DataflowProcessingElement(instanceId, peStandardCoreSecondPerMessage, numberOfCores, isContinuous, trigger, selectivity);
	}

	public DataflowProcessingElement clone(int instanceId, int nodeId) {
		return new DataflowProcessingElement(instanceId, nodeId, peStandardCoreSecondPerMessage, numberOfCores, isContinuous, trigger, selectivity);
	}
	
	public void incrementInQLength()
	{
		incrementInQLength(1);
	}
	
	public void incrementInQLength(int count)
	{
		inQLength += count;
	}
	
	public float getM() {
		return sM;
	}
	
	public float getN() {
		return sN;
	}
	
	public List<Edge> getOutEdges() {
		return outEdges;
	}

	
	@Override
	public boolean isDataAvailableForTransfer() {
		for(Edge e: outEdges)
		{
			if(e.isDataAvailableForTransfer()) return true;
		}
		return false;
	}

	@Override
	public void scheduleDataTransfer(SimulationEntity owner) {
		for(Edge e: outEdges)
		{
			if(e.isDataAvailableForTransfer())
			{
				e.scheduleDataTransfer(owner);
			}
		}		
	}
	
	@Override
	public void enqueueMessage() {
		enqueueMessage(1);
	}
	
	@Override
	public void enqueueMessage(int n) {
		incrementInQLength(n);
		
		float currentTime = IaaSSimulator.getClock();
		
		for(int i = 0; i < n; i++)
		{
			previousMsgTimings.add(new Float(currentTime));
		}
		
		if(inQLength > 0) state = READY;
	}
	
	@Override
	public void dequeueMessage() {
		decrementInQLength();
		
		float currentTime = IaaSSimulator.getClock();
		
		//for(int i = 0; i < n; i++)
		{
			previousDequeMsgTimings.add(new Float(currentTime));
		}
		
		if(inQLength <= 0) state = WAITING;
	}

	private void decrementInQLength() {
		inQLength--;
	}

	public void perfLog() {
		PerfLog.printCSV(LogEntity.PE,nodeId,instanceId,selectivity,peStandardCoreSecondPerMessage,getDataRate(IaaSSimulator.getClock()),inQLength,getOutDataRate(IaaSSimulator.getClock()),getOutQLength(), getDeltaQLength(IaaSSimulator.getClock()));
	}

	public long getInQLength() {
		return inQLength;
	}
	
	public float getDataRate(float time) {
		if(previousMsgTimings.size() == 0) return 0;
		
		Iterator<Float> pit = previousMsgTimings.iterator();
		while(pit.hasNext())
		{
			Float f = (float)Utils.roundDoubleValue(pit.next());
			if(f < (float)Utils.roundDoubleValue(time - 1)) 
				pit.remove();
		}
		
		return previousMsgTimings.size();		
	}

	
	public double numMsgsProcessedPerSec(float time) {
		if(previousDequeMsgTimings.size() == 0) return 0;
		
		Iterator<Float> pit = previousDequeMsgTimings.iterator();
		while(pit.hasNext())
		{
			Float f = (float)Utils.roundDoubleValue(pit.next());
			if(f < (float)Utils.roundDoubleValue(time - 1)) 
				pit.remove();
		}
		
		return previousDequeMsgTimings.size();	
	}
	
	public double getDeltaQLength(float time) {
		double delq = getDataRate(time) - numMsgsProcessedPerSec(time);
		return delq;
	}
	
	public double getOutDataRate(float time) {
		if(previousOutMsgTimings.size() == 0) return 0;
		
		Iterator<Float> pit = previousOutMsgTimings.iterator();
		while(pit.hasNext())
		{
			Float f = (float)Utils.roundDoubleValue(pit.next());
			if(f < (float)Utils.roundDoubleValue(time - 1)) 
				pit.remove();
		}
		
		return previousOutMsgTimings.size();
	}
	
	private long getOutQLength() {
		long maxQ = Long.MIN_VALUE;
		for(Edge e: outEdges)
		{
			if(maxQ < e.getOutQLength()) maxQ = e.getOutQLength();
		}
		return maxQ;
	}

	public void putMessagesInOutQueue(float n) {
		
		float currentTime = IaaSSimulator.getClock();		
		for(int i = 0; i < n; i++)
		{
			previousOutMsgTimings.add(new Float(currentTime));
		}
		
		for(Edge e: outEdges)
		{
			e.putMessagesInOutQueue(n);
		}
	}

	@Override
	public boolean resetState() {
		if(inQLength <= 0) return true;
		return false;
	}
	
	public float getSelectivity() {
		return selectivity;
	}

	public Edge getEdgeByDestPeId(int peId) {		
		for(Edge outEdge: outEdges)
		{
			if(outEdge.getDownPeId() == peId) return outEdge;
		}
		return null;
	}

	public DataSource getDataSource() {
		return dataSource;
	}
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

}
