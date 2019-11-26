package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.util.DataUtils;
import org.opensha.refFaultParamDb.vo.FaultSectionPrefData;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;

public class SubsetBetweenCalc {

	public static void main(String[] args) throws IOException {
//		File csvFile = new File("/home/kevin/git/cybershake-analysis/"
//				+ "study_19_3_rsqsim_rot_2585/rotated_ruptures_m7p2_vert_ss_surface/"
//				+ "resources/between_events_m7.2_50km_std_dev_3s_medians_table.csv");
//		File outputFile = new File("/tmp/m7.2_50km_tau_fault_dependence.csv");
		File csvFile = new File("/home/kevin/git/cybershake-analysis/"
				+ "study_19_3_rsqsim_rot_2585/rotated_ruptures_m6p6_vert_ss_surface/"
				+ "resources/between_events_m6.6_50km_std_dev_3s_medians_table.csv");
		File outputFile = new File("/tmp/m6.6_50km_tau_fault_dependence.csv");
		RSQSimCatalog catalog = Catalogs.BRUCE_2585_1MYR.instance();
		CSVFile<String> csv = CSVFile.readFile(csvFile, true);
		
		Map<Integer, Double> eventTerms = new HashMap<>();
		for (int row=1; row<csv.getNumRows(); row++) {
			int eventID = csv.getInt(row, 0);
			double eventTerm = csv.getDouble(row, 1);
			eventTerms.put(eventID, eventTerm);
		}
		
		int[] eventIDs = Ints.toArray(eventTerms.keySet());
		
		Map<Integer, RSQSimEvent> eventsMap = new HashMap<>();
		for (RSQSimEvent event : catalog.loader().byIDs(eventIDs))
			eventsMap.put(event.getID(), event);
		
		System.out.println("loaded "+eventsMap.size()+" events");
		
		Map<Integer, HashSet<Integer>> eventParentsMap = new HashMap<>();
		for (RSQSimEvent event : eventsMap.values()) {
			Integer eventID = event.getID();
			HashSet<Integer> parents = new HashSet<>();
			for (FaultSectionPrefData sect : catalog.getSubSectsForRupture(event))
				parents.add(sect.getParentSectionId());
			eventParentsMap.put(eventID, parents);
		}
		
		CSVFile<String> output = new CSVFile<>(true);
		output.addLine("Max Events Per Fault", "Mean tau", "Median tau", "Min tau",
				"Max tau", "Min # Events", "Max # Events");
		
		int numEach = 500;
		int maxNumEvents = 50;
		
		for (int numPerFault=1; numPerFault<maxNumEvents; numPerFault++) {
			int minCount = Integer.MAX_VALUE;
			int maxCount = 0;
			double[] taus = new double[numEach];
			for (int i=0; i<numEach; i++) {
				List<Integer> myEvents = new ArrayList<>(eventsMap.keySet());
				Collections.shuffle(myEvents);
				
				List<Double> myEventTerms = new ArrayList<>();
				Map<Integer, Integer> parentCountsMap = new HashMap<>();
				
				eventLoop:
				for (Integer eventID : myEvents) {
					HashSet<Integer> myParents = eventParentsMap.get(eventID);
					for (Integer parentID : myParents) {
						if (parentCountsMap.containsKey(parentID)) {
							if (parentCountsMap.get(parentID) == numPerFault)
								continue eventLoop;
						} else {
							parentCountsMap.put(parentID, 0);
						}
					}
					// if we made it this far, we haven't exceeded for any parents
					// increment the count
					for (Integer parentID : myParents)
						parentCountsMap.put(parentID, parentCountsMap.get(parentID)+1);
					
					myEventTerms.add(eventTerms.get(eventID));
				}
				Preconditions.checkState(myEventTerms.size() > 1);
				minCount = Integer.min(minCount, myEventTerms.size());
				maxCount = Integer.max(maxCount, myEventTerms.size());
				
				taus[i] = Math.sqrt(StatUtils.variance(Doubles.toArray(myEventTerms)));
			}
			double mean = StatUtils.mean(taus);
			double median = DataUtils.median(taus);
			double min = StatUtils.min(taus);
			double max = StatUtils.max(taus);
			output.addLine(numPerFault+"", (float)mean+"", (float)median+"",
					(float)min+"", (float)max+"", minCount+"", maxCount+"");
		}
		output.writeToFile(outputFile);
	}

}
