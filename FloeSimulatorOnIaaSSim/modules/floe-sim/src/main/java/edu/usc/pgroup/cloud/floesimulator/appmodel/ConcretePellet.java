package edu.usc.pgroup.cloud.floesimulator.appmodel;

import java.util.ArrayList;
import java.util.List;

public class ConcretePellet extends Pellet {

	private Alternate activeAlternate;
	
	private int pelletUnitCount;
	
	List<PelletInstance> pelletInstances;
	
	public ConcretePellet(Pellet pellet) {
		super(pellet.id);
		super.alternates = pellet.alternates;
		if(pellet.getClass() == this.getClass())
		{
			activeAlternate = ((ConcretePellet)pellet).activeAlternate;
			pelletUnitCount = ((ConcretePellet)pellet).pelletUnitCount;
		}
		pelletInstances = new ArrayList<>();
	}
	
	public void addPelletInstance(PelletInstance instance)
	{
		pelletInstances.add(instance);
	}
	
	public Alternate getActiveAlternate() {
		return activeAlternate;
	}
	
	public void setActiveAlternate(Alternate activeAlternate) {
		this.activeAlternate = activeAlternate;
	}

	public int getPelletUnitCount() {
		return pelletUnitCount;
	}
	
	public void setPelletUnitCount(int pelletUnitCount) {
		this.pelletUnitCount = pelletUnitCount;
	}

	public void distributeDataRate(Double totalInputRate) {
		int totalInstances = pelletInstances.size();
		for(PelletInstance pi: pelletInstances)
		{
			pi.addToInputDataRate(totalInputRate/totalInstances);
		}
	}

	public void processTickAllInstances() {
		for(PelletInstance pi: pelletInstances)
		{
			pi.processTick();
		}		
	}

	public double getOverallDeltaQueueLength()
	{
		double total = 0;
		for(PelletInstance pi: pelletInstances)
		{
			total += pi.getDeltaQueueLength();
		}	
		return total;
	}
	
	public void transmitDataOnEdge(ConcretePellet s) {
		for(PelletInstance pi: pelletInstances)
		{
			double output = pi.getCurrentOutputDataRate()/s.getPelletInstances().size();
			
			for(PelletInstance po: s.getPelletInstances())
			{		
				
				if(pi.getVm() == po.getVm())
				{
					po.addToInputDataRate(output);
				}				
				else
				{
					po.addToInputDataRate(0.3,output);
				}
			}
		}
	}

	public List<PelletInstance> getPelletInstances() {
		return pelletInstances;
	}

	public double getTotalOutputDataRate() {
		double outputDataRate = 0;
		for(PelletInstance pi: pelletInstances)
		{
			outputDataRate += pi.getCurrentOutputDataRate();
		}
		return outputDataRate;			
	}

	
	public void clearePelletInstance() {
		pelletInstances.clear();
	}
}
