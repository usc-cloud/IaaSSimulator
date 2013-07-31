package edu.usc.pgroup.cloud.iaassim;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import edu.usc.pgroup.cloud.iaassim.core.SimulationEntity;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEvent;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventTag;
import edu.usc.pgroup.cloud.iaassim.core.SimulationEventType;
import edu.usc.pgroup.cloud.iaassim.core.predicates.Predicate;
import edu.usc.pgroup.cloud.iaassim.core.predicates.PredicateAny;
import edu.usc.pgroup.cloud.iaassim.utils.Log;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;

public class IaaSSimulator {

	public static final Predicate SIM_ANY = new PredicateAny();
	private static boolean traceFlag;
	private static Calendar calendar;
	private static ArrayList<SimulationEntity> entities;
	private static LinkedHashMap<String, SimulationEntity> entitiesByName;
	private static FutureQueue future;
	private static DeferredQueue deferred;
	
	private static float clock;
	private static boolean running;
	private static float terminateAt;
	private static List<Integer> cloudList;

	public static void addEntity(SimulationEntity simulationEntity) {
		SimulationEvent evt;
		if (running) {
			// Post an event to make this entity
			evt = new SimulationEvent(SimulationEventType.Create, clock, 1, 0, SimulationEventTag.Undefined, simulationEntity);
			future.addEvent(evt);
		}
		if (simulationEntity.getId() == -1) { // Only add once!
			int id = entities.size();
			simulationEntity.setId(id);
			entities.add(simulationEntity);
			entitiesByName.put(simulationEntity.getName(), simulationEntity);
		}
	}

	/**
	 * Initialise the simulation for stand alone simulations. This function
	 * should be called at the start of the simulation.
	 */
	protected static void initialize() {
		Log.printLine("Initialising...");
		VMClasses.Initialize();
		
		entities = new ArrayList<SimulationEntity>();
		entitiesByName = new LinkedHashMap<String, SimulationEntity>();
		future = new FutureQueue();
		deferred = new DeferredQueue();
		clock = 0.0f;
		running = false;
	}

	/**
	 * Initialises all the common attributes.
	 * 
	 * @param _calendar
	 *            the _calendar
	 * @param _traceFlag
	 *            the _trace flag
	 * @param numUser
	 *            number of users
	 * @throws Exception
	 *             This happens when creating this entity before initialising
	 *             CloudSim package or this entity name is <tt>null</tt> or
	 *             empty
	 * @pre $none
	 * @post $none
	 */
	private static void initCommonVariable(Calendar _calendar,
			boolean _traceFlag, int numUser) throws Exception {
		initialize();
		// NOTE: the order for the below 3 lines are important
		traceFlag = _traceFlag;

		// Set the current Wall clock time as the starting time of
		// simulation
		if (_calendar == null) {
			calendar = Calendar.getInstance();
		} else {
			calendar = _calendar;
		}

		// creates a CloudSimShutdown object
		// CloudSimShutdown shutdown = new CloudSimShutdown("CloudSimShutdown",
		// numUser);
		// shutdownId = shutdown.getId();
	}

	/**
	 * Initialises CloudSim parameters. This method should be called before
	 * creating any entities.
	 * <p>
	 * Inside this method, it will create the following CloudSim entities:
	 * <ul>
	 * <li>CloudInformationService.
	 * <li>CloudSimShutdown
	 * </ul>
	 * <p>
	 * 
	 * @param numUser
	 *            the number of User Entities created. This parameters indicates
	 *            that {@link gridsim.CloudSimShutdown} first waits for all user
	 *            entities's END_OF_SIMULATION signal before issuing terminate
	 *            signal to other entities
	 * @param cal
	 *            starting time for this simulation. If it is <tt>null</tt>,
	 *            then the time will be taken from
	 *            <tt>Calendar.getInstance()</tt>
	 * @param traceFlag
	 *            <tt>true</tt> if CloudSim trace need to be written
	 * @see gridsim.CloudSimShutdown
	 * @see CloudInformationService.CloudInformationService
	 * @pre numUser >= 0
	 * @post $none
	 */
	public static void init(int numUser, Calendar cal, boolean traceFlag) {
		try {
			initCommonVariable(cal, traceFlag, numUser);

			// create a GIS object
			// cis = new CloudInformationService("CloudInformationService");

			// set all the above entity IDs
			// cisId = cis.getId();
		} catch (IllegalArgumentException s) {
			Log.printLine("CloudSim.init(): The simulation has been terminated due to an unexpected error");
			Log.printLine(s.getMessage());
		} catch (Exception e) {
			Log.printLine("CloudSim.init(): The simulation has been terminated due to an unexpected error");
			Log.printLine(e.getMessage());
		}
	}

	public static boolean running() {
		return running;
	}

	public static void send(int src, int dest, float delay,
			SimulationEventTag tag, Object data) {
		if (delay < 0) {
			throw new IllegalArgumentException("Send delay can't be negative:"+delay);
		}

		SimulationEvent e = new SimulationEvent(SimulationEventType.Send, clock + delay, src, dest, tag, data);
		future.addEvent(e);
	}

	public static SimulationEvent selectEvent(int src, Predicate p) {
		SimulationEvent ev = null;
		Iterator<SimulationEvent> iterator = deferred.iterator();
		while (iterator.hasNext()) {
			ev = iterator.next();
			if (ev.getDestEntity() == src && p.match(ev)) {
				iterator.remove();
				break;
			}
		}
		return ev;
	}

	public static int waiting(int id, Predicate p) {
		int count = 0;
		SimulationEvent event;
		Iterator<SimulationEvent> iterator = deferred.iterator();
		while (iterator.hasNext()) {
			event = iterator.next();
			if ((event.getDestEntity() == id) && (p.match(event))) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Starts the execution of CloudSim simulation. It waits for complete
	 * execution of all entities, i.e. until all entities threads reach
	 * non-RUNNABLE state or there are no more events in the future event queue.
	 * <p>
	 * <b>Note</b>: This method should be called after all the entities have
	 * been setup and added.
	 * 
	 * @return the float
	 * @throws NullPointerException
	 *             This happens when creating this entity before initialising
	 *             CloudSim package or this entity name is <tt>null</tt> or
	 *             empty.
	 * @see gridsim.CloudSim#init(int, Calendar, boolean)
	 * @pre $none
	 * @post $none
	 */
	public static float startSimulation() throws NullPointerException {
		Log.printLine("Starting IaaSSimulator");
		try {
			float clock = run();

			// reset all static variables
			calendar = null;
			traceFlag = false;

			return clock;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new NullPointerException("CloudSim.startCloudSimulation() :"
					+ " Error - you haven't initialized CloudSim.");
		}
	}

	/**
	 * Internal method used to start the simulation. This method should <b>not</b> be used by user
	 * simulations.
	 */
	public static void runStart() {
		running = true;
		// Start all the entities
		for (SimulationEntity ent : entities) {
			ent.startEntity();
		}

		Log.printLine("Entities started.");
	}
	
	
	/**
	 * Internal method used to run one tick of the simulation. This method should <b>not</b> be
	 * called in simulations.
	 * 
	 * @return true, if successful otherwise
	 */
	public static boolean runClockTick() {
		SimulationEntity ent;
		boolean queue_empty;
		int entities_size = entities.size();

		for (int i = 0; i < entities_size; i++) {
			ent = entities.get(i);
			if (ent.getState() == SimulationEntity.RUNNABLE) {
				ent.run();
			}
		}

		// If there are more future events then deal with them
		if (future.size() > 0) {
			List<SimulationEvent> toRemove = new ArrayList<SimulationEvent>();
			Iterator<SimulationEvent> it = future.iterator();
			queue_empty = false;
			SimulationEvent first = it.next();
			processEvent(first);
			future.remove(first);

			it = future.iterator();

			// Check if next events are at same time...
			boolean trymore = it.hasNext();
			while (trymore) {
				SimulationEvent next = it.next();
				if (next.getEventTime() == first.getEventTime()) {
					processEvent(next);
					toRemove.add(next);
					trymore = it.hasNext();
				} else {
					trymore = false;
				}
			}

			future.removeAll(toRemove);

		} else {
			queue_empty = true;
			running = false;
			Log.printLine("Simulation: No more future events");
		}

		return queue_empty;
	}
	
	/**
	 * Processes an event.
	 * 
	 * @param e the e
	 */
	private static void processEvent(SimulationEvent e) {
		int dest, src;
		SimulationEntity dest_ent;
		// Update the system's clock
		if (e.getEventTime() < clock) {
			throw new IllegalArgumentException("Past event detected.");
		}
		clock = e.getEventTime();

		// Ok now process it
		switch (e.getSysEventType()) {
			case Undefined:
				throw new IllegalArgumentException("Event has a null type.");

			case Create:
				SimulationEntity newe = (SimulationEntity) e.getEventData();
				//TODO: Create this function
				//addEntityDynamically(newe);
				break;

			case Send:
				// Check for matching wait
				dest = e.getDestEntity();
				if (dest < 0) {
					throw new IllegalArgumentException("Attempt to send to a null entity detected.");
				} else {
					//SimulationEventTag tag = e.getEventTag();
					//dest_ent = entities.get(dest);
					/*if (dest_ent.getState() == SimulationEntity.WAITING) {
						Integer destObj = Integer.valueOf(dest);
						Predicate p = waitPredicates.get(destObj);
						if ((p == null) || (tag == 9999) || (p.match(e))) {
							dest_ent.setEventBuffer((SimulationEvent) e.clone());
							dest_ent.setState(SimulationEntity.RUNNABLE);
							waitPredicates.remove(destObj);
						} else {
							deferred.addEvent(e);
						}
					} else {
						deferred.addEvent(e);
					}*/
					deferred.addEvent(e);
				}
				break;

			case hold_done:
				src = e.getSrcEntity();
				if (src < 0) {
					throw new IllegalArgumentException("Null entity holding.");
				} else {
					entities.get(src).setState(SimulationEntity.RUNNABLE);
				}
				break;

			default:
				break;
		}
	}
	
	/**
	 * Start the simulation running. This should be called after all the entities have been setup
	 * and added, and their ports linked.
	 * 
	 * @return the float last clock value
	 */
	public static float run() {
		if (!running) {
			runStart();
		}
		while (true) {
			if (runClockTick()) {
				break;
			}

			// this block allows termination of simulation at a specific time
			if (terminateAt > 0.0 && clock >= terminateAt) {
				terminateSimulation();
				clock = terminateAt;
				break;
			}

			//Do not support pause in current version
			/*if (pauseAt != -1
					&& ((future.size() > 0 && clock <= pauseAt && pauseAt <= future.iterator().next()
							.eventTime()) || future.size() == 0 && pauseAt <= clock)) {
				pauseSimulation();
				clock = pauseAt;
			}

			while (paused) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}*/
		}

		float clock = getClock();

		finishSimulation();
		runStop();

		return clock;
	}
	
	public static float getClock() {
		return clock;
	}
	
	
	/**
	 * This method is called if one wants to terminate the simulation.
	 * 
	 * @return true, if successful; false otherwise.
	 */
	public static boolean terminateSimulation() {
		running = false;
		Log.printLine("Simulation: Reached termination time.");
		return true;
	}
	
	/**
	 * Internal method used to stop the simulation. This method should <b>not</b> be used directly.
	 */
	public static void runStop() {
		Log.printLine("Simulation completed.");
	}
	
	/**
	 * Internal method that allows the entities to terminate. This method should <b>not</b> be used
	 * in user simulations.
	 */
	public static void finishSimulation() {
		// Allow all entities to exit their body method
		//if (!abruptTerminate) {
			for (SimulationEntity ent : entities) {
				if (ent.getState() != SimulationEntity .FINISHED) {
					ent.run();
				}
			}
		//}

		for (SimulationEntity ent : entities) {
			ent.shutdownEntity();
		}

		// reset all static variables
		// Private data members
		entities = null;
		entitiesByName = null;
		future = null;
		//deferred = null;
		clock = 0L;
		running = false;

		/*waitPredicates = null;
		paused = false;
		pauseAt = -1;
		abruptTerminate = false;*/
	}

	public static List<Integer> getCloudList() {
		return cloudList;
	}
	
	public static void addCloud(Integer cloudId)
	{
		if(cloudList == null) cloudList= new ArrayList<>();
		
		cloudList.add(cloudId);
	}
	
	public static void setTerminateAt(float terminateAt) {
		IaaSSimulator.terminateAt = terminateAt;
	}

	public static Object getBroker() {
		for(SimulationEntity entity: entities)
		{
			if(entity.getClass() == IaaSBroker.class) return entity;
		}
		return null;
	}
}
