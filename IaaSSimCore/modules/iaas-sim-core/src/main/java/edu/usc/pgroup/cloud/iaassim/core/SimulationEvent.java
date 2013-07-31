package edu.usc.pgroup.cloud.iaassim.core;

public class SimulationEvent implements Cloneable, Comparable<SimulationEvent>{

	/**
	 * Event Type
	 */
	private final SimulationEventType sysEventType;
	
	/**
	 * time at which event should occur
	 */
	private final float eventTime;
	
	
	/**
	 * Src entity id
	 */
	private int srcEntityId;
	
	/**
	 * Destination entity id
	 */
	private int destEntityId;
	
	/**
	 * user defined type of the event
	 */
	private final SimulationEventTag eventTag;
	
	/**
	 * user defined event data
	 */
	private final Object eventData;

	/**
	 * Serial number of an event 
	 * if there are more than one 
	 * event for a given time
	 */
	private long serial;
	
	
	public SimulationEvent()
	{
		sysEventType = SimulationEventType.Undefined;
		eventTime = -1;
		srcEntityId = -1;
		destEntityId = -1;
		eventTag = SimulationEventTag.Undefined;
		eventData = null;
	}
	
	public SimulationEvent(SimulationEventType simEventType,float time,int srcEntity, int destEntity, SimulationEventTag eventTag, Object eventData)
	{
		this.sysEventType = simEventType;
		this.eventTime = time;
		this.srcEntityId = srcEntity;
		this.destEntityId = destEntity;
		this.eventTag = eventTag;
		this.eventData = eventData;
	}
	
	
	/**
	 * Create an exact copy of this event.
	 * 
	 * @return The event's copy
	 */
	@Override
	public Object clone() {
		return new SimulationEvent(sysEventType,
				eventTime,
				srcEntityId,
				destEntityId,
				eventTag,
				eventData);
	}
	
	/**
	 * Compares with event o
	 * based on time (and serial number for same time)
	 */
	@Override
	public int compareTo(SimulationEvent simEvent) {
		if(simEvent == null)
			return 1;
		else if(eventTime < simEvent.eventTime)
			return -1;
		else if(eventTime > simEvent.eventTime)
			return 1;
		else if(eventTime == simEvent.eventTime)
		{
			if(serial < simEvent.serial)
				return -1;
			else if(serial > simEvent.serial)
				return 1;
			else 
				return 0;
						
		}
		else if(this == simEvent)
			return 0;
		else 
			return 1;
	}

	public SimulationEventType getSysEventType() {
		return sysEventType;
	}
	
	public int getSrcEntity() {
		return srcEntityId;
	}
	
	public int getDestEntity() {
		return destEntityId;
	}
	
	public SimulationEventTag getEventTag() {
		return eventTag;
	}
	
	public Object getEventData() {
		return eventData;
	}
	
	public long getSerial() {
		return serial;
	}
	
	public void setSerial(long serial) {
		this.serial = serial;
	}
	
	public float getEventTime() {
		return eventTime;
	}
}
