package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

import java.math.BigDecimal;
import java.util.Properties;

import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.utils.Utils;

public class PeriodicDataSource extends DataSource{

	float activeDuration;
	float period;
	float dataRate;
	
	float previousTime;
	
	float periodStartTime;
	private float overflowMessages;
	
	
	public PeriodicDataSource(Properties params, DataflowProcessingElement pe) {
		this(Float.parseFloat(params.getProperty("datarate")),
				Float.parseFloat(params.getProperty("activeDuration")),Float.parseFloat(params.getProperty("period")),pe);

	}

	public PeriodicDataSource(float dataRate, float activeDuration,
			float period, DataflowProcessingElement sink) {
		super(sink);

		this.activeDuration = activeDuration;
		this.period = period;
		this.dataRate = dataRate;
		
		previousTime = -1;
		periodStartTime = -1;
		overflowMessages = 0;
	}

	@Override
	public boolean start(float time) {
		super.start(time);
		previousTime = time;
		periodStartTime = time;		
		return started;
	}
	
	@Override
	public int getMessages(float time) throws Exception {
		
		if(!started) throw new Exception("Data source has not been started yet");
		
		
		double periodDuration =  (double)time - (double)periodStartTime;
		
		double duration = time - previousTime;		
		previousTime = time;
		
		
		if(periodDuration < activeDuration)
		{
			double numMessages = Utils.roundDoubleValue(dataRate * duration);
			numMessages += overflowMessages;
			overflowMessages = (float)(numMessages - (int) numMessages);
			  
			return (int)numMessages;
		}
		else if(Utils.roundDoubleValue(periodDuration) >= period)		
		{			
			periodStartTime = time;
		}
		
		return 0;
	}

	@Override
	public float distributeData(int oldNumInstance, int newNumInstance) {
		this.dataRate = this.dataRate * (float)oldNumInstance /(float)newNumInstance;
		return this.dataRate;
	}

	@Override
	public DataSource clone(DataflowProcessingElement sink) {
		return new PeriodicDataSource(dataRate,activeDuration,period, sink);
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new PeriodicDataSource(dataRate,activeDuration,period, this.sinkPE);
	}
}
