package edu.usc.pgroup.cloud.floesimulator.appmodel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.floesimulator.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.ExecutionTrigger;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.Dataflow;

public class AbstractDynamicDataflow{
	Dataflow concreteDataflow;
	
	private List<Pellet> pellets;
	private List<Integer> inputPellets;
	private List<Integer> outputPellets;
	
	
	private List<PelletEdge> edges;

	Map<Integer,Double> inputDataRate;
	
	public AbstractDynamicDataflow() {
		pellets = new ArrayList<>();
		inputPellets = new ArrayList<>();
		outputPellets = new ArrayList<>();
		edges = new ArrayList<>();
		inputDataRate = new HashMap<>();
	}
	
	public void initialize(String fileName) throws IOException {
		FileInputStream fis = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		
		String node = null;
		//nodes
		while(!(node = br.readLine()).startsWith("---"))
		{
			System.out.println(node);
			if(node.startsWith("#")) continue;
			String alternates[] = node.split(" ");
			Pellet np = new Pellet(Integer.parseInt(alternates[0]));
			
			for(int i = 1; i < alternates.length; i++)
			{
				String alternateInfo = alternates[i];
				String info[] = alternateInfo.split(",");
				float corePerMessage = Float.parseFloat(info[1]);
				float value = Float.parseFloat(info[2]);
				float selectivity = Float.parseFloat(info[3]);	
				Alternate a = new Alternate(Integer.parseInt(info[0]),corePerMessage,1,true,ExecutionTrigger.Message,selectivity,value);
				np.addAlternate(a);
			}
			pellets.add(np);
		}
		
		for(Pellet p: pellets)
		{
			inputPellets.add(p.getId());
			outputPellets.add(p.getId());
		}
		
		//edges
		String edge;		
		while(!(edge = br.readLine()).startsWith("---"))
		{
			String alternates[] = edge.split(" ");
			
			Pellet source = Utils.findNode(pellets,Integer.parseInt(alternates[0]));
			Pellet destination = Utils.findNode(pellets,Integer.parseInt(alternates[1]));
			if(source == null || destination == null) {
				System.err.println("Error in graph file");
				System.exit(1);
			}
			
			PelletEdge e = new PelletEdge(source, destination);
			edges.add(e);
			
			inputPellets.remove((Integer)destination.getId());
			outputPellets.remove((Integer)source.getId());
		}
		
		//data rate
		String dr;
		while(!(dr = br.readLine()).startsWith("---"))
		{
			String[] drr = dr.split(" ");
			inputDataRate.put(Integer.parseInt(drr[0]),Double.parseDouble(drr[1]));
		}
	}

	

	public List<Pellet> getPellets() {
		return pellets;
	}
	
	public List<PelletEdge> getEdges() {
		return edges;
	}

	public List<Integer> getInputPellets() {
		return inputPellets;
	}

/*	public List<Integer> getSuccessors(String id) {
		return Utils.getSuccessors(id, edges);
	}*/

	public List<Integer> getOutputPellets() { 
		return outputPellets;
	}

	public Map<Integer, Double> getInputDataRateMap() {
		return inputDataRate;
	}

	public List<PelletEdge> reverseEdges() {
		List<PelletEdge> reverseEdges = new ArrayList<>();
		for(PelletEdge e: edges)
		{
			PelletEdge re = new PelletEdge(e.getDestNode(),e.getSourceNode());
			reverseEdges.add(re);
		}
		return reverseEdges;
	}
}
