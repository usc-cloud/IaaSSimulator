package edu.usc.pgroup.cloud.floesimulator.appmodel;

import java.util.ArrayList;
import java.util.List;


public class Pellet {
	List<Alternate> alternates;
	int id;
	
	public Pellet(int id) {
		this.id = id;
		alternates = new ArrayList<>();
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	
	public List<Alternate> getAlternates() {
		return alternates;
	}
	
	public void addAlternate(Alternate a) {
		alternates.add(a);		
	}	
}
