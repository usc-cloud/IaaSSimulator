package edu.usc.pgroup.cloud.iaassim.probabilitydistributions;

public class ConstantDist implements ContinuousDistribution{

	double value;
	public ConstantDist(double v)
	{
		this.value = v;
	}
	
	@Override
	public double sample() {
		return value;
	}

}
