package edu.usc.pgroup.cloud.iaassim.cloudworkflow;

import java.util.Properties;

import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.ContinuousDistribution;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.SmoothWalk;
import edu.usc.pgroup.cloud.iaassim.utils.Utils;

public class SmoothRandomDataSource extends DataSource{

	float dataRate;
	
	float previousTime;
	float previousUpdateTime;
	
	float currentRate;
	ContinuousDistribution factor = new SmoothWalk(2, 1, 1, 1); 
	float overflowMessages = 0;
	
	public SmoothRandomDataSource(Properties params, DataflowProcessingElement pe) {
		this(Float.parseFloat(params.getProperty("datarate")),pe);

	}

	public SmoothRandomDataSource(float dataRate, DataflowProcessingElement sink) {
		super(sink);

		this.dataRate = dataRate;
		currentRate = dataRate;
		
		previousTime = -1;
		previousUpdateTime = -1;
	}

	@Override
	public boolean start(float time) {
		super.start(time);
		previousTime = time;
		previousUpdateTime = time;
		return started;
	}
	
	@Override
	public int getMessages(float time) throws Exception {
		
		if(!started) throw new Exception("Data source has not been started yet");
		
		
		double duration = time - previousTime;		
		
		double lastUpdatedDuration = time - previousUpdateTime;
		
		if(Utils.roundDoubleValue(lastUpdatedDuration) > 60 * 2)
		{
			currentRate = (float) (factor.sample() * dataRate);
			//System.out.println("crate,"+currentRate);
			lastUpdatedDuration = time;
		}

		previousTime = time;
		
		double numMessages = Utils.roundDoubleValue(currentRate * duration);
		numMessages += overflowMessages;
		overflowMessages = (float)(numMessages - (int) numMessages);
		
		return (int)numMessages;
	}

	@Override
	public float distributeData(int oldNumInstance, int newNumInstance) {
		this.dataRate = this.dataRate * (float)oldNumInstance /(float)newNumInstance;
		return this.dataRate;
	}

	@Override
	public DataSource clone(DataflowProcessingElement sink) {
		return new SmoothRandomDataSource(dataRate, sink);
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new SmoothRandomDataSource(dataRate, this.sinkPE);
	}
}
