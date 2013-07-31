/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package edu.usc.pgroup.cloud.iaassim.core.predicates;

import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventTag;


/**
 * A predicate to select events with specific tags.
 * 
 * @author Marcos Dias de Assuncao
 * @since CloudSim Toolkit 1.0
 * @see PredicateNotType
 * @see Predicate
 */
public class PredicateType extends Predicate {

	/** The tags. */
	private final SimulationEventTag[] tags;

	/**
	 * Constructor used to select events with the tag value <code>t1</code>.
	 * 
	 * @param t1 an event tag value
	 */
	public PredicateType(SimulationEventTag t1) {
		tags = new SimulationEventTag[] { t1 };
	}

	/**
	 * Constructor used to select events with a tag value equal to any of the specified tags.
	 * 
	 * @param tags the list of tags
	 */
	public PredicateType(SimulationEventTag[] tags) {
		this.tags = tags.clone();
	}

	/**
	 * The match function called by <code>Sim_system</code>, not used directly by the user.
	 * 
	 * @param ev the ev
	 * @return true, if match
	 */
	@Override
	public boolean match(SimulationEvent ev) {
		SimulationEventTag tag = ev.getEventTag();
		for (SimulationEventTag tag2 : tags) {
			if (tag == tag2) {
				return true;
			}
		}
		return false;
	}

}
