package edu.usc.pgroup.cloud.iaassim.vm;
/**
 * 
 * @author kumbhare
 * IaaSSim v0.1
 */

public class VMClass
{

	/**
	 * VM Class name e.g. m1.small
	 */
	String vmClassName;

	/**
	 * Million instrstuction per second per core. (core speed)
	 */
	float mips;
	
	/**
	 * Total number of available cores
	 */
	private int coreCount;
	
	
	/**
	 * core coefficient. Compared to the "standard" vm. (this.mips/standard.mips)
	 */
	float coreCoeff;
	
	/**
	 * Disk size in GB. We are assuming disk speed is same across the cluster. Ignore that for now.
	 */
	float diskSpace;
	
	/**
	 * Max nw bandwidth available with this type of VM.
	 */
	float nwBandwidth;
	
	/**
	 * Cost per hour per instance in dollars
	 */
	float costPerHour;


	
	public VMClass(String vmClassName, float mips, int coreCount, float nwBandwidth, float costPerHour)
	{
		this.vmClassName = vmClassName;
		this.mips = mips;
		this.coreCount = coreCount;
		this.nwBandwidth = nwBandwidth;
		this.costPerHour = costPerHour;
		this.coreCoeff = -1;
	}
	
	public VMClass(String vmClass) {
		String[] arrVMClass = vmClass.split(",");
		this.vmClassName = arrVMClass[0];
		this.mips = Float.parseFloat(arrVMClass[1]);
		this.coreCount = Integer.parseInt(arrVMClass[2]);
		this.nwBandwidth = Float.parseFloat(arrVMClass[3]);
		this.costPerHour = Float.parseFloat(arrVMClass[4]);
		this.coreCoeff = -1;
	}

	public String getVmClassName() {
		return vmClassName;
	}
	
	public float getCoreCoeff() {
		if(coreCoeff == -1)
			coreCoeff = this.mips/VMClasses.getStandard().getMips();
		return coreCoeff;
	}
	
	public int getCoreCount() {
		return coreCount;
	}
	
	public float getCostPerHour() {
		return costPerHour;
	}
	
	public float getDiskSpace() {
		return diskSpace;
	}
	
	public float getMips() {
		return mips;
	}
	
	public float getNwBandwidth() {
		return nwBandwidth;
	}

	public float getExpectedStartupTime() {
		return VMMonitor.getExpectedStartupTime(this);
	}

	public float getExpectedShutdownTime() {
		return VMMonitor.getExpectedShutdownTime(this);
	}
	
}