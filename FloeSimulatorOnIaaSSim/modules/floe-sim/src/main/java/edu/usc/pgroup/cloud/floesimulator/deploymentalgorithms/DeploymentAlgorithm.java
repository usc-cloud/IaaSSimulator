package edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.usc.pgroup.cloud.floesimulator.ApplicationDeployment;
import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.appmodel.Alternate;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.cloudworkflow.DataflowProcessingElement;
import edu.usc.pgroup.cloud.iaassim.vm.VM;
import edu.usc.pgroup.cloud.iaassim.vm.VMClass;

public abstract class DeploymentAlgorithm {

	private int instanceId = 0;


	public abstract ApplicationDeployment getInitialDeployment(
			AbstractDynamicDataflow graph, List<VMClass> resourceClasses,
			Map<Integer, Double> inputDataRate, double sigma,
			double minAppThroughput, double optimizationPeriod);

	public abstract ApplicationDeploymentDelta getDeltaDeployment(
			ApplicationDeployment currentDeployment,
			List<VMClass> resourceClasses, double minOmega);
	
	
	public abstract VMConsolidationInfo consolidateVMs(List<VM> vms);
	
	

	
}
