package edu.usc.pgroup.cloud.iaassim.core;

import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.core.predicates.Predicate;

public abstract class SimulationEntity {

	public static final int RUNNABLE = 2000;

	public static final int FINISHED = 2001;

	

	/**
	 * entity name
	 */
	private String name;
	
	/**
	 * Entity id (this is globally unique)
	 */
	private int id;
	
	private int state;
	
	/**
	 * Creates a new entity.
	 * 
	 * @param name the name to be associated with this entity
	 */
	public SimulationEntity(String name) {
		if (name.indexOf(" ") != -1) {
			throw new IllegalArgumentException("Entity names can't contain spaces.");
		}
		this.name = name;
		id = -1;
		state = RUNNABLE;
		IaaSSimulator.addEntity(this);
	}

	/**
	 * Get the name of this entity.
	 * 
	 * @return The entity's name
	 */
	public String getName() {
		return name;
	}
	
	public int getState() {
		return state;
	}
	
	public void setState(int state) {
		this.state = state;
	}

	/**
	 * Get the unique id number assigned to this entity.
	 * 
	 * @return The id number
	 */
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	/**
	 * Count how many events are waiting in the entity's deferred queue.
	 * 
	 * @return The count of events
	 */
	public int numEventsWaiting() {
		return IaaSSimulator.waiting(id, IaaSSimulator.SIM_ANY);
	}
	
	public int numEventsWaiting(Predicate p) {
		return IaaSSimulator.waiting(id, p);
	}
	
	
	/**
	 * Get the first event matching a predicate from the deferred queue, or if none match, wait for
	 * a matching event to arrive.
	 * 
	 * @param p The predicate to match
	 * @return the simulation event
	 */
	public SimulationEvent getNextEvent(Predicate p) {
		if (!IaaSSimulator.running()) {
			return null;
		}
		if (numEventsWaiting(p) > 0) {
			return selectEvent(p);
		}
		return null;
	}
	
	/**
	 * Get the first event waiting in the entity's deferred queue, or if there are none, wait for an
	 * event to arrive.
	 * 
	 * @return the simulation event
	 */
	public SimulationEvent getNextEvent() {
		return getNextEvent(IaaSSimulator.SIM_ANY);
	}
	
	/**
	 * 
	 * @param p
	 * @return
	 */
	public SimulationEvent selectEvent(Predicate p) {
		if (!IaaSSimulator.running()) {
			return null;
		}

		return IaaSSimulator.selectEvent(id, p);
	}
	
	/* Abstract methods*/
	/**
	 * This method is invoked by the {@link Simulation} class when the simulation is started. This
	 * method should be responsible for starting the entity up.
	 */
	public abstract void startEntity();

	/**
	 * This method is invoked by the {@link Simulation} class whenever there is an event in the
	 * deferred queue, which needs to be processed by the entity.
	 * 
	 * @param ev the event to be processed by the entity
	 */
	public abstract void processEvent(SimulationEvent ev);

	/**
	 * This method is invoked by the {@link Simulation} before the simulation finishes. If you want
	 * to save data in log files this is the method in which the corresponding code would be placed.
	 */
	public abstract void shutdownEntity();

	
	/**
	 * Run method. Process events in the event queue 
	 * that are supposed to be triggerd at the current time.
	 */
	public void run() {
		SimulationEvent ev = getNextEvent();

		while (ev != null) {
			processEvent(ev);
			
			ev = getNextEvent();
		}
	}
	
	
	/**
	 * 
	 * Schedule event (for the given destination entity) function
	 * @param dest
	 * @param delay
	 * @param tag
	 * @param data
	 */
	public void schedule(int dest, float delay, SimulationEventTag tag, Object data) {
		if (!IaaSSimulator.running()) {
			return;
		}
		IaaSSimulator.send(id, dest, delay, tag, data);
	}
	
	public void schedule(int id, float delay, SimulationEventTag tag) {
		schedule(id,delay,tag,null);
	}
	
	/**
	 * Send event functions
	 */
	protected void send(int entityId, float delay, SimulationEventTag cloudSimTag, Object data) {
		if (entityId < 0) {
			return;
		}

		// if delay is -ve, then it doesn't make sense. So resets to 0.0
		if (delay < 0) {
			delay = 0;
		}

		if (Float.isInfinite(delay)) {
			throw new IllegalArgumentException("The specified delay is infinite value");
		}

		if (entityId < 0) {
			//Log.printLine(getName() + ".send(): Error - " + "invalid entity id " + entityId);
			return;
		}

		/*int srcId = getId();
		if (entityId != srcId) {// does not delay self messages
			delay += getNetworkDelay(srcId, entityId);
		}*/

		schedule(entityId, delay, cloudSimTag, data);
	}

	
}
