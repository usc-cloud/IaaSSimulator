package edu.usc.pgroup.cloud.iaassim.utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import edu.usc.pgroup.cloud.iaassim.ProcessingElement;
import edu.usc.pgroup.cloud.iaassim.vm.VM;

public class Utils {

	// for now assume one tick = 1 sec..
	private static long numTicksPerSecond = 1;

	public static long getNumberofTicks(long timeValue, TimeUnit unit) {
		long seconds = unit.toSeconds(timeValue);
		return seconds * numTicksPerSecond;
	}

	public static <T> List<List<T>> PermutationFinder(List<T> s) {
		List<List<T>> perm = new ArrayList<>();
		if (s == null) {
			// error case
			return null;
		} else if (s.size() == 0) {
			perm.add(new ArrayList<T>());
			// initial
			return perm;
		}
		T initial = s.get(0);

		// first character
		List<T> rem = s.subList(1, s.size());

		// Full string without first character
		List<List<T>> words = PermutationFinder(rem);
		for (List<T> str : words) {
			for (int i = 0; i <= str.size(); i++) {
				perm.add(charinsert(str, initial, i));
			}
		}
		return perm;
	}

	public static <T> List<T> charinsert(List<T> str, T c, int j) {
		// String begin = str.substring(0, j);
		List<T> begin = new ArrayList<>(str.subList(0, j));
		// String end = str.substring(j);
		List<T> end = str.subList(j, str.size());
		begin.add(c);
		begin.addAll(end);
		return begin;
	}

	public static VM getVMById(List<VM> vmRequests, int vmId) {
		for (VM vm : vmRequests) {
			if (vm.getId() == vmId) {
				return vm;
			}
		}
		return null;
	}

	public static ProcessingElement getPEByInstanceId(List<ProcessingElement> peList, int peId) {
		for (ProcessingElement pe : peList) {
			if (pe.getInstanceId() == peId) {
				return pe;
			}
		}
		return null;
	}
	
	public static List<ProcessingElement> getPEsByNodeId(List<ProcessingElement> peList, int peId) {
		List<ProcessingElement> pes = new ArrayList<>();
		for (ProcessingElement pe : peList) {
			if (pe.getNodeId() == peId) {
				pes.add(pe);
			}
		}
		return pes;
	}
	
	public static double roundDoubleValue(double val2Round) {
		/*
		 * Method to round off a recurring double value to a two decimal
		 * precision.
		 */
		BigDecimal bd = new BigDecimal(val2Round);
		bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
		// now to remove zero after decimal
		return bd.doubleValue();
	} // end of method roundDoubleValue
}
