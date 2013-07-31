package edu.usc.pgroup.cloud.iaassim.vm;

import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.ConstantDist;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.ContinuousDistribution;
import edu.usc.pgroup.cloud.iaassim.probabilitydistributions.SmoothWalk;

class VMMonitor {

	private ContinuousDistribution distribution = new ConstantDist(1);
	private float previousTime = -1;
	private float factor = 1;
	
	public void setVariationDistribution(ContinuousDistribution dist)
	{
		distribution = dist;
	}
	
	public static float getExpectedStartupTime(VMClass vmClass)
	{
		return 60;
	}
	
	public float getExpectedMips(VM vm)
	{
		return vm.getVMClass().getMips(); 
	}
	
	public float getExpectedCoreCoeff(VM vm, float time)
	{
		if(previousTime == -1 || (time - previousTime >= 60 * 2))
		{
			this.factor = (float)distribution.sample();
			this.factor = factor > 1 ? 1 : factor;
			this.previousTime = time;	
		}
		
		return factor * getExpectedMips(vm)/VMClasses.getStandard().getMips();
	}

	public static float getExpectedShutdownTime(VMClass vmClass) {
		return 10;
	}
}
