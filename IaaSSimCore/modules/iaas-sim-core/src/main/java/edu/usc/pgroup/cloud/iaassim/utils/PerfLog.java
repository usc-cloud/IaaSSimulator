/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package edu.usc.pgroup.cloud.iaassim.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import edu.usc.pgroup.cloud.iaassim.IaaSSimulator;

/**
 * The Log class used for performing loggin of the simulation process. It provides the ability to
 * substitute the output stream by any OutputStream subclass.
 * 
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 2.0
 */
public class PerfLog {

	/** The Constant LINE_SEPARATOR. */
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	/** The output. */
	private static Map<LogEntity,OutputStream> output = new HashMap<>();

	/** The disable output flag. */
	private static boolean disabled;

	private static String outputDir;

	private static String filter = "";
	/**
	 * Prints the message.
	 * 
	 * @param message the message
	 */
	private static void print(LogEntity entity, String message) {
		if (!isDisabled() && (filter.length()==0 || filter.contains(entity.toString()))) {
			try {
				getOutput(entity).write((new DecimalFormat(".##").format(IaaSSimulator.getClock()) + ", "+ entity.toString() + ", " + message).getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void setFilter(String filter)
	{
		PerfLog.filter = filter;
	}
	private static void print(LogEntity entity, String id, String message) {
		if (!isDisabled() && (filter.length()==0 || filter.contains(entity.toString()))) {
			try {
				getOutput(entity,id).write((new DecimalFormat(".##").format(IaaSSimulator.getClock()) + ", "+ entity.toString() + ", " + message).getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	

	/**
	 * Prints the message passed as a non-String object.
	 * 
	 * @param message the message
	 */
	public static void print(LogEntity entity, Object message) {
		if (!isDisabled()) {
			print(entity, String.valueOf(message));
		}
	}

	/**
	 * Prints the line.
	 * 
	 * @param message the message
	 */
	public static void printLine(LogEntity entity, String message) {
		if (!isDisabled()) {
			print(entity, message + LINE_SEPARATOR);
		}
	}

	private static void printLine(LogEntity entity, String id,
			String message) {
		if (!isDisabled()) {
			print(entity, id, message + LINE_SEPARATOR);
		}
	}
	
	

	/**
	 * Prints the empty line.
	 */
	public static void printLine(LogEntity entity) {
		if (!isDisabled()) {
			print(entity,LINE_SEPARATOR);
		}
	}

	/**
	 * Prints the line passed as a non-String object.
	 * 
	 * @param message the message
	 */
	public static void printLine(LogEntity entity, Object message) {
		if (!isDisabled()) {
			printLine(entity,String.valueOf(message));
		}
	}

	/**
	 * Prints a string formated as in String.format().
	 * 
	 * @param format the format
	 * @param args the args
	 */
	public static void format(LogEntity entity,String format, Object... args) {
		if (!isDisabled()) {
			print(entity,String.format(format, args));
		}
	}

	/**
	 * Prints a line formated as in String.format().
	 * 
	 * @param format the format
	 * @param args the args
	 */
	public static void formatLine(LogEntity entity,String format, Object... args) {
		if (!isDisabled()) {
			printLine(entity,String.format(format, args));
		}
	}

	/**
	 * Sets the output.
	 * 
	 * @param _output the new output
	 */
	public static void setOutputDir(String _output) {
		outputDir = _output;		
	}

	/**
	 * Gets the output.
	 * 
	 * @return the output
	 */
	public static OutputStream getOutput(LogEntity entity) {
		
		
		if(outputDir == null) 
			outputDir = ".";
		
		if(!output.containsKey(entity))
		{
			FileOutputStream fs;
			try {
				fs = new FileOutputStream(outputDir +"/"+ entity.toString() + ".csv");
			
				fs.write(getHead(entity));
				output.put(entity, fs);
			}
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		OutputStream os = output.get(entity);
		if(os == null) {			
			System.exit(1);
		}
		return os;
	}

	static Map<String,FileOutputStream> peStreams = new HashMap<>();
	private static OutputStream getOutput(LogEntity entity, String id) {
		if(entity != LogEntity.PE) return null;
		
		if(outputDir == null) 
			outputDir = ".";
		
		if(!peStreams.containsKey(id))
		{
			FileOutputStream fs;
			try {
				fs = new FileOutputStream(outputDir +"/"+ entity.toString() + id + ".csv");
			
				fs.write(getHead(entity));
				peStreams.put(id, fs);
			}
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		OutputStream os = peStreams.get(id);
		if(os == null) {			
			System.exit(1);
		}
		
		return os;
	}
	
	private static byte[] getHead(LogEntity entity) {
		byte[] head = null;
		switch (entity) {
		case PE:
			head = ("time, type, nodeid, instance id, selectivity, standard core/msg, datarate, inq, out_data_rate, outq, delta que length"+LINE_SEPARATOR).getBytes();
			break;
		case VM:
			head = ("time, type, vmid, starttime, shutdown, available cores, total cores,coeff"+LINE_SEPARATOR).getBytes();
			break;
		case NW:
			head = ("time, type, vmid, vmid2, latency, bandwidth"+LINE_SEPARATOR).getBytes();
			break;
		case COORD:
			head = ("time, type, dataflowid, value, cost, f, maxOutDataRate, currentOutDataRate, omega"+LINE_SEPARATOR).getBytes();
			break;
		default:
			break;
		}
		return head;
	}

	/**
	 * Sets the disable output flag.
	 * 
	 * @param _disabled the new disabled
	 */
	public static void setDisabled(boolean _disabled) {
		disabled = _disabled;
	}

	/**
	 * Checks if the output is disabled.
	 * 
	 * @return true, if is disable
	 */
	public static boolean isDisabled() {
		return disabled;
	}

	/**
	 * Disables the output.
	 */
	public static void disable() {
		setDisabled(true);
	}

	/**
	 * Enables the output.
	 */
	public static void enable() {
		setDisabled(false);
	}

	public static void printCSV(LogEntity entity,Object... objects) {
		String message = "";
		
		for(Object o: objects)
		{
			message+=o+", ";
		}
		if(entity == LogEntity.PE)
		{
			printLine(entity,objects[0].toString(),message);
		}
		else{
			printLine(entity, message);
		}
	}

	

	
}
