package edu.usc.pgroup.cloud.floesimulator.samplerunner;


import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import edu.usc.pgroup.cloud.floesimulator.appmodel.AbstractDynamicDataflow;
import edu.usc.pgroup.cloud.floesimulator.coordinator.Coordinator;
import edu.usc.pgroup.cloud.floesimulator.utils.AppConfig;
import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;
import edu.usc.pgroup.cloud.iaassim.utils.Log;
import edu.usc.pgroup.cloud.iaassim.utils.PerfLog;
import edu.usc.pgroup.cloud.iaassim.vm.VMClasses;

public class SimulationRunner {
	public static void main(String[] s) {
		VMClasses.Initialize();
		Log.disable();
		
		AppConfig.getInstance().set("algo",s[0]);
		File file=new File(s[1]);
		boolean exists = file.exists();
		if(!exists)
		{
			file.mkdirs();
		}
		PerfLog.setFilter("COORD");
		PerfLog.setOutputDir(s[1]);
		
		AbstractDynamicDataflow app = new AbstractDynamicDataflow();
		try {
			app.initialize("appgraph/appgraph.txt");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(1);
		}
		// System.exit(0);

		
		IaaSSimulator.init(1, Calendar.getInstance(), true);
		IaaSSimulator.setTerminateAt(60 * 60 * 10);
		
		Coordinator coordinator = Coordinator.getInstance();
		long begin = System.currentTimeMillis();

		String deploymentId = coordinator.deployApplication(app);
		

		IaaSSimulator.startSimulation();
		
		/*
		 * long timeValue = 12; Future<Boolean> simulator =
		 * coordinator.simulateApplicationRun(deploymentId, timeValue,
		 * TimeUnit.HOURS);
		 * 
		 * boolean success; try { success = simulator.get();
		 * System.out.println(success);
		 * 
		 * } catch (InterruptedException | ExecutionException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

		
		//coordinator.StopCoordinator();
		
		long end = System.currentTimeMillis();
		long totalTime = (end - begin);
		System.out.println("Total Time (mins):" + totalTime / 1000.0 / 60);
	}
}
