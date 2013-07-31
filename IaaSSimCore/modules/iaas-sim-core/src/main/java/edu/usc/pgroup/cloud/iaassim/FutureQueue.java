package edu.usc.pgroup.cloud.iaassim;

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;

/**
 * This class implements the future event queue used by {@link Simulation}. The event queue uses a
 * {@link TreeSet} in order to store the events.
 * 
 * @author Marcos Dias de Assuncao
 * @since CloudSim Toolkit 1.0
 * @see Simulation
 * @see java.util.TreeSet
 */
public class FutureQueue {

	/** The sorted set. */
	private final SortedSet<SimulationEvent> sortedSet = new TreeSet<SimulationEvent>();

	/** The serial. */
	private long serial = 0;

	/**
	 * Add a new event to the queue. Adding a new event to the queue preserves the temporal order of
	 * the events in the queue.
	 * 
	 * @param newEvent The event to be put in the queue.
	 */
	public void addEvent(SimulationEvent newEvent) {
		newEvent.setSerial(serial++);
		sortedSet.add(newEvent);
	}

	/**
	 * Add a new event to the head of the queue.
	 * 
	 * @param newEvent The event to be put in the queue.
	 */
	public void addEventFirst(SimulationEvent newEvent) {
		newEvent.setSerial(0);
		sortedSet.add(newEvent);
	}

	/**
	 * Returns an iterator to the queue.
	 * 
	 * @return the iterator
	 */
	public Iterator<SimulationEvent> iterator() {
		return sortedSet.iterator();
	}

	/**
	 * Returns the size of this event queue.
	 * 
	 * @return the size
	 */
	public int size() {
		return sortedSet.size();
	}

	/**
	 * Removes the event from the queue.
	 * 
	 * @param event the event
	 * @return true, if successful
	 */
	public boolean remove(SimulationEvent event) {
		return sortedSet.remove(event);
	}

	/**
	 * Removes all the events from the queue.
	 * 
	 * @param events the events
	 * @return true, if successful
	 */
	public boolean removeAll(Collection<SimulationEvent> events) {
		return sortedSet.removeAll(events);
	}

	/**
	 * Clears the queue.
	 */
	public void clear() {
		sortedSet.clear();
	}

}