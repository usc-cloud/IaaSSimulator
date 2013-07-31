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
 * A predicate to select events that don't match specific tags.
 * 
 * @author Marcos Dias de Assuncao
 * @since CloudSim Toolkit 1.0
 * @see PredicateType
 * @see Predicate
 */
public class PredicateNotType extends Predicate {

	/** The tags. */
	private final SimulationEventTag[] tags;

	/**
	 * Constructor used to select events whose tags do not match a given tag.
	 * 
	 * @param tag An event tag value
	 */
	public PredicateNotType(SimulationEventTag tag) {
		tags = new SimulationEventTag[] { tag };
	}

	/**
	 * Constructor used to select events whose tag values do not match any of the given tags.
	 * 
	 * @param tags the list of tags
	 */
	public PredicateNotType(SimulationEventTag[] tags) {
		this.tags = tags.clone();
	}

	/**
	 * The match function called by {@link Simulation}, not used directly by the user.
	 * 
	 * @param ev the ev
	 * @return true, if match
	 */
	@Override
	public boolean match(SimulationEvent ev) {
		SimulationEventTag tag = ev.getEventTag();
		for (SimulationEventTag tag2 : tags) {
			if (tag == tag2) {
				return false;
			}
		}
		return true;
	}

}