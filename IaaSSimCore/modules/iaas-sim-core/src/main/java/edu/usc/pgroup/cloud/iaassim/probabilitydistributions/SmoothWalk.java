package edu.usc.pgroup.cloud.iaassim.probabilitydistributions;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

public class SmoothWalk implements ContinuousDistribution{

	double max;
	double mean;
	double lastValue;
	
	Random value;
	Random coin;
	private double dampingFactor;
	public SmoothWalk(double max, double mean)
	{
		this.max = max;
		this.mean = mean;
		lastValue = max;
		
		long seed = getLongSeed();
		value = new Random(seed);
		coin = new Random(seed/2);
		this.dampingFactor = 10;
	}
	
	
	public SmoothWalk(double max, double mean, double initial, double dampingFactor) {
		this(max,mean);
		lastValue = initial;
		this.dampingFactor = dampingFactor;
	}


	private long getLongSeed() {
		SecureRandom sec = new SecureRandom();
		byte[] sbuf = sec.generateSeed(8);
		ByteBuffer bb = ByteBuffer.wrap(sbuf);
		return bb.getLong();
	}
		
		
		
	@Override
	public double sample() {
		double v = value.nextDouble()/dampingFactor;
		int c = coin.nextInt(10);
		
		if(lastValue > mean)
		{
			if(c>=5 && lastValue - v > 0)
			{
				lastValue -= v;
			}
			else if(lastValue + v <= max )
			{
				lastValue += v;
			}
		}
		else
		{
			if(c>=5 && lastValue + v <= max)
			{
				lastValue += v;
			}
			else if(lastValue - v > 0)
			{
				lastValue -= v;
			}
		}
		
		//System.out.println("sample,"+lastValue);
		return lastValue;
	}

}
