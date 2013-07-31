package edu.usc.pgroup.cloud.iaassim.vm;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.utils.Config;

/**
 * 
 * @author kumbhare
 * IaaSSim v0.1
 */

public class VMClasses
{

	private static Map<String,VMClass> vmClasses = new HashMap<>();
	private static VMClass standard;
	private static VMClass best;
	private static List<VMClass> vmSortedBySize;
	
	
	public static void Initialize()
	{
		String strVmClasses = (Config.getInstance().get("VirtualMachineClass"));
		
		String[] arrVmClasses = strVmClasses.split(";");
		
		vmClasses.clear();
		for(String vmClass: arrVmClasses)
		{
			VMClass c = new VMClass(vmClass);
			vmClasses.put(c.getVmClassName(),c);
		}
		
		String standardVM = (Config.getInstance().get("StandardVMClass"));
		setStandard(standardVM);
	}
	
	
	private static void setStandard(String standardVM) {
		standard = vmClasses.get(standardVM);
	}


	/**
	 * 
	 * @returns the "standard" Virtual Machine class against which all other classes will be compared.
	 * The Standard VM would have been used to run the baseline core requirements for the cloud apps
	 */
	public static VMClass getStandard() {
		return standard;
	}


	public static VMClass get(String vmClassName) {
		return vmClasses.get(vmClassName);
	}


	public static List<VMClass> getAll() {
		return new ArrayList<>(vmClasses.values());
	}


	public static VMClass getBestVMCostPerStandarCore() {
		if(best == null)
		{
			double minCostPerCore = Double.MAX_VALUE;
			for(VMClass vmType : vmClasses.values())
			{
				double corecoeff = vmType.getCoreCoeff();
				int numCores = vmType.getCoreCount();
				double cost = vmType.getCostPerHour();
				
				double costpercore = corecoeff * numCores/cost;
				if(costpercore < minCostPerCore)
				{
					minCostPerCore = costpercore;
					best = vmType;
				}
			}
		}
		
		return best;
	}


	public static List<VMClass> getVMsSortedBySize() {
		if(vmSortedBySize == null)
		{
			vmSortedBySize = new ArrayList<>();
			
			for(VMClass vmType : vmClasses.values())
			{
				double size = vmType.getCoreCount() * vmType.getCoreCoeff();
				
				int i = 0;
				for(;i<vmSortedBySize.size();i++)
				{
					VMClass vmc = vmSortedBySize.get(i);
					if(size > vmc.getCoreCoeff() * vmc.getCoreCount()) break;
				}
				vmSortedBySize.add(i,vmType);
			}
		}
		
		return vmSortedBySize;
	}
}