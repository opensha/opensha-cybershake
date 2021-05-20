package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.imr.mod.impl.BaylessSomerville2013DirectivityModifier;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class RuptureVariationDirectivityCalculator {

	public static void main(String[] args) throws IOException {
		if (args.length == 1 && args[0].equals("--hardcoded")) {
			String argsStr = "34 -118 36 6 3,5,10 /tmp/directivity_test.csv";
			args = argsStr.split(" ");
		}
		if (args.length != 6) {
			System.out.println("USAGE: <latitude> <longitude> <erf-id> <rv-scen-id> <periods> <output-file.csv>");
			System.exit(2);
		}
		
		double lat = Double.parseDouble(args[0]);
		double lon = Double.parseDouble(args[1]);
		int erfID = Integer.parseInt(args[2]);
		int rvID = Integer.parseInt(args[3]);
		String periodsStr = args[4];
		File outputFile = new File(args[5]);
		
		Preconditions.checkState(erfID == 35 || erfID == 36, "Only ERF ID's 35 and 36 currently supported");
		
		List<Double> periods = new ArrayList<>();
		for (String str : Splitter.on(",").split(periodsStr.trim()))
			periods.add(Double.parseDouble(str));
		
		
		Location loc = new Location(lat, lon);
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		ERF2DB erf2db = new ERF2DB(db);
		System.out.println("Building ERF");
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		CSVFile<String> csv = new CSVFile<>(true);
		List<String> header = Lists.newArrayList("Source ID", "Rupture ID", "Rupture Variation ID",
				"Hypocenter Lat", "Hypocenter Lon", "Hypocenter Depth");
		for (double p : periods)
			header.add((float)p+"s fd");
		csv.addLine(header);
		
		BaylessSomerville2013DirectivityModifier bayless = new BaylessSomerville2013DirectivityModifier();
		
		System.out.println("Calculating...");
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			System.out.println("Source "+sourceID+"/"+erf.getNumSources());
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				Map<Integer, Location> hypos = erf2db.getHypocenters(erfID, sourceID, rupID, rvID);
				List<Integer> rvIDs = new ArrayList<>(hypos.keySet());
				Collections.sort(rvIDs);
				
				ProbEqkRupture rup = source.getRupture(rupID);
				
				for (int r : rvIDs) {
					List<String> line = new ArrayList<>();
					line.add(sourceID+"");
					line.add(rupID+"");
					line.add(r+"");
					Location hypo = hypos.get(r);
					line.add((float)hypo.getLatitude()+"");
					line.add((float)hypo.getLongitude()+"");
					line.add((float)hypo.getDepth()+"");
					rup.setHypocenterLocation(hypo);
					for (double p : periods)
						line.add((float)bayless.getFd(rup, loc, p)+"");
					csv.addLine(line);
				}
			}
		}
		db.destroy();
		System.out.println("Writing to "+outputFile.getAbsolutePath());
		csv.writeToFile(outputFile);
		System.out.println("DONE");
	}

}
