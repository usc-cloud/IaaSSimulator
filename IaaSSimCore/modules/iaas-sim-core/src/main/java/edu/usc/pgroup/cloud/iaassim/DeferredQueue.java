/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package edu.usc.pgroup.cloud.iaassim;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;

/**
 * This class implements the deferred event queue used by {@link Simulation}. The event queue uses a
 * linked list to store the events.
 * 
 * @author Marcos Dias de Assuncao
 * @since CloudSim Toolkit 1.0
 * @see Simulation
 * @see SimulationEvent
 */
public class DeferredQueue {

	/** The list. */
	private final List<SimulationEvent> list = new LinkedList<SimulationEvent>();

	/** The max time. */
	private float maxTime = -1;

	/**
	 * Adds a new event to the queue. Adding a new event to the queue preserves the temporal order
	 * of the events.
	 * 
	 * @param newEvent The event to be added to the queue.
	 */
	public void addEvent(SimulationEvent newEvent) {
		// The event has to be inserted as the last of all events
		// with the same event_time(). Yes, this matters.
		float eventTime = newEvent.getEventTime();
		if (eventTime >= maxTime) {
			list.add(newEvent);
			maxTime = eventTime;
			return;
		}

		ListIterator<SimulationEvent> iterator = list.listIterator();
		SimulationEvent event;
		while (iterator.hasNext()) {
			event = iterator.next();
			if (event.getEventTime() > eventTime) {
				iterator.previous();
				iterator.add(newEvent);
				return;
			}
		}

		list.add(newEvent);
	}

	/**
	 * Returns an iterator to the events in the queue.
	 * 
	 * @return the iterator
	 */
	public Iterator<SimulationEvent> iterator() {
		return list.iterator();
	}

	/**
	 * Returns the size of this event queue.
	 * 
	 * @return the number of events in the queue.
	 */
	public int size() {
		return list.size();
	}

	/**
	 * Clears the queue.
	 */
	public void clear() {
		list.clear();
	}

}