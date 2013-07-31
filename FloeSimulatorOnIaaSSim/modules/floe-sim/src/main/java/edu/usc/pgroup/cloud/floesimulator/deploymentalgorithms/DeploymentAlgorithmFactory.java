package edu.usc.pgroup.cloud.floesimulator.deploymentalgorithms;

import edu.usc.pgroup.cloud.floesimulator.utils.AppConfig;



public class DeploymentAlgorithmFactory {

	public static DeploymentAlgorithm get() {
		String algo = AppConfig.getInstance().get("algo");
		switch(algo)
		{
		case "Simple":
			return new SimpleDeploymentAlgorithm();			
		case "H2":
			return new HeuristicDeployment();			
		case "Brute":
			//return new BruteForceDeployment();
			break;
		}
		return null;
	}

}
