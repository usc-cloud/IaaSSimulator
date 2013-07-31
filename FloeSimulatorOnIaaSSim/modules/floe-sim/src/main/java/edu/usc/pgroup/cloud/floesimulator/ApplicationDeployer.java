package edu.usc.pgroup.cloud.floesimulator;

import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms.DeploymentAlgorithm;
import edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms.DeploymentAlgorithmFactory;
import edu.usc.pgroup.cloud.floesimulator.utils.AppConfig;
import edu.usc.pgroup.cloud.floesimulator.utils.Utils;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;


public class ApplicationDeployer {

	public static ApplicationDeployment deploy(AbstractDynamicDataflow app) {
		DeploymentAlgorithm algorithm = DeploymentAlgorithmFactory.get();
		
		double dollarAtMaxValue = Double.parseDouble(AppConfig.getInstance().get("costAtMaxValue"));
		double dollarAtMinValue = Double.parseDouble(AppConfig.getInstance().get("costAtMinValue"));
		double sigma = Utils.getSigma(app,dollarAtMaxValue,dollarAtMinValue);
		
		double minAppThroughput = Double.parseDouble(AppConfig.getInstance().get("minAppThroughput"));
		double optimizationPeriod = Double.parseDouble(AppConfig.getInstance().get("optimizationPeriod"));
		ApplicationDeployment deployment = algorithm.getInitialDeployment(app, VMClasses.getAll(), app.getInputDataRateMap(),sigma,minAppThroughput, optimizationPeriod);
		deployment.setDeploymentAlgorithm(algorithm);
		return deployment;
	}

}
