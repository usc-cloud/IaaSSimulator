package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.IaaSBroker;
import edu.usc.pgroup.cloud.iaassim.ProcessingElement;

public class Dataflow {
	
	private Map<Integer,List<DataflowProcessingElement>> dpesMap;
	
	private int baseInstanceId = 5000;

	private List<DataSource> dataSources;
	
	private List<PEdge> edges; 
	
	public Dataflow()
	{
		dpesMap = new HashMap<>();
		dataSources = new ArrayList<>();
		edges = new ArrayList<>();
	}
	
	public List<DataflowProcessingElement> addPEInstances(DataflowProcessingElement pe, int count)
	{
		List<DataflowProcessingElement> instances = new ArrayList<>();
		int peId = pe.getInstanceId();
		for(int i = 0; i < count; i++)
		{
			DataflowProcessingElement instance = (DataflowProcessingElement) pe.clone(baseInstanceId++, pe.getInstanceId());
			
			if(!dpesMap.containsKey(peId))
			{
				dpesMap.put(peId, new ArrayList<DataflowProcessingElement>());
			}
			
			dpesMap.get(peId).add(instance);
			instances.add(instance);
		}
		return instances;
	}
	

	public void addEdge(int srcPeId, int destPeId)
	{
		//Add an out edge (and all the fibers) to all the instances of the src Pe
		PEdge pedge = new PEdge(srcPeId,destPeId);		
		edges.add(pedge);
		
		for(DataflowProcessingElement pe: dpesMap.get(srcPeId))
		{
			Edge e = pe.createEmptyEdge(destPeId);
			for(DataflowProcessingElement destPe: dpesMap.get(destPeId))
			{
				e.addFiber(destPe);
			}
		}
	}

	public List<DataflowProcessingElement> getAllPEInstances() {
		List<DataflowProcessingElement> allInstances = new ArrayList<>();
		for(List<DataflowProcessingElement> peList: dpesMap.values())
		{
			allInstances.addAll(peList);
		}
		
		return allInstances;
	}

	public void setInputDataSource(DataflowProcessingElement pe1,
			DataSource source) {
		dataSources.add(source);
	}

	
	public List<DataflowProcessingElement> decrementPEInstance(
			List<DataflowProcessingElement> toRemove) {
		
		List<DataflowProcessingElement> removed = new ArrayList<>();
		
		for(DataflowProcessingElement dpe: toRemove)
		{
			//Remove instance from dpesMap
			if(dpesMap.get(dpe.getNodeId()).contains(dpe))
			{
				dpesMap.get(dpe.getNodeId()).remove(dpe);
				removed.add(dpe);
			}
			
			//Remove edges from predecessors edges.. 			
			//get predecessors.. 
			int peId = dpe.getNodeId();
			List<Integer> predecessors = getPredecessors(peId);
			
			if(predecessors.size() > 0)
			{
				for(Integer predecessor: predecessors)
				{
					List<DataflowProcessingElement> predecessorPeInstances = dpesMap.get(predecessor);
					for(DataflowProcessingElement predecessorInstance: predecessorPeInstances)
					{
						 Edge outEdge = predecessorInstance.getEdgeByDestPeId(peId);						 
						 outEdge.removeFiber(dpe);
					}
				}
			}
			else
			{
				//add new data source and update the existing ones.. 
				
				List<DataflowProcessingElement> allInstances = dpesMap.get(peId);
				
				for(DataflowProcessingElement pe: allInstances) //iterate over all instances
				{
					pe.getDataSource().distributeData(dpesMap.get(peId).size() + 1, dpesMap.get(peId).size());
				}			
			}						
		}
		return removed;
	}
	
	public List<DataflowProcessingElement> incrementPEInstance(int peId, int count) {
		List<DataflowProcessingElement> newInstances = new ArrayList<>();
		
		DataflowProcessingElement masterPe = dpesMap.get(peId).get(0);//Master pellet
		for(int i = 0; i < count; i++)
		{
			DataflowProcessingElement instance = (DataflowProcessingElement) masterPe.clone(baseInstanceId++, masterPe.getNodeId());
			
			dpesMap.get(peId).add(instance);
			newInstances.add(instance);
		}
		
		
		//Connect forward edges.. 
		for(DataflowProcessingElement newPe: newInstances)
		{
			for(Edge e: masterPe.getOutEdges())
			{
				Edge newEdge = newPe.createEmptyEdge(e.getDownPeId());
				for(DataflowProcessingElement down_pe: e.getDownstreamPEs())
				{
					newEdge.addFiber(down_pe);
				}
			}
		}
		
		//get predecessors.. 
		List<Integer> predecessors = getPredecessors(peId);
		
		if(predecessors.size() > 0)
		{
			for(Integer predecessor: predecessors)
			{
				List<DataflowProcessingElement> predecessorPeInstances = dpesMap.get(predecessor);
				for(DataflowProcessingElement predecessorInstance: predecessorPeInstances)
				{
					 Edge outEdge = predecessorInstance.getEdgeByDestPeId(peId);
					 for(DataflowProcessingElement newPe: newInstances)
					 {
						 outEdge.addFiber(newPe);
					 }
				}
			}
		}
		else
		{
			//add new data source and update the existing ones.. 
			
			
			for(DataflowProcessingElement newPe: newInstances)
			{
				DataSource s = masterPe.getDataSource().clone(newPe);
				newPe.setDataSource(s);
			}
			
			for(DataflowProcessingElement pe: dpesMap.get(peId)) //iterate over all instances
			{
				pe.getDataSource().distributeData(dpesMap.get(peId).size()-1,dpesMap.get(peId).size());
			}			
		}
		
		return newInstances;		
	}
	
	private List<Integer> getPredecessors(Integer destId) {
		List<Integer> predecessors = new ArrayList<>();
		for(PEdge pedge: edges)
		{
			if(pedge.dest == destId)
			{
				predecessors.add(pedge.src);
			}
		}
		return predecessors;
	}

	private class PEdge
	{
		public PEdge(int srcPeId, int destPeId) {
			this.src = srcPeId;
			this.dest = destPeId;					
		}
		Integer src;
		Integer dest;
	}

	public Map<Integer, List<DataflowProcessingElement>> getNodeToPEMapping() {
		return dpesMap; 
	}
}

