package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.faultSurface.FaultTrace;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.base.Preconditions;

public class RVInfoCSV {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_17_3_1D;
		CybershakeRun exampleRun = study.runFetcher().fetch().get(0);
		
		AbstractERF erf = study.buildNewERF();
		
		CSVFile<String> rupCSV = new CSVFile<>(false);
		rupCSV.addLine("Source ID", "Rupture ID", "Magnitude", "Upper Depth (km)", "Width (km)", "Dip (degrees)",
				"Rake (degrees)", "Trace Location Count", "Trace Latitude 1", "Trace Longitude 1",
				"...", "...", "Trace Latitude N", "Trace Longitude N");
		CSVFile<String> rvCSV = new CSVFile<>(true);
		rvCSV.addLine("Source ID", "Rupture ID", "Rupture Variation ID",
				"Hypocenter Latitude", "Hypocenter Longitude", "Hypocenter Depth (km)");
		
		File outputDir = new File("/tmp");
		
		ERF2DB erf2db = new ERF2DB(study.getDB());
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			System.out.println("Source "+sourceID);
			for (int rupID=0; rupID<erf.getNumRuptures(sourceID); rupID++) {
				ProbEqkRupture rup = erf.getRupture(sourceID, rupID);
				
				List<String> rupLine = new ArrayList<>();
				
				rupLine.add(sourceID+"");
				rupLine.add(rupID+"");
				rupLine.add((float)rup.getMag()+"");
				RuptureSurface surf = rup.getRuptureSurface();
				double zTOR = surf.getAveRupTopDepth();
				rupLine.add((float)zTOR+"");
				rupLine.add((float)surf.getAveWidth()+"");
				rupLine.add((float)surf.getAveDip()+"");
				rupLine.add((float)rup.getAveRake()+"");
				FaultTrace trace;
				try {
					trace = surf.getUpperEdge();
				} catch (RuntimeException e) {
					trace = surf.getEvenlyDiscritizedUpperEdge();
				}
				rupLine.add(trace.size()+"");
				for (Location loc : trace) {
					rupLine.add((float)loc.lat+"");
					rupLine.add((float)loc.lon+"");
					if (surf.getAveDip() < 90d)
						Preconditions.checkState((float)zTOR == (float)loc.depth, "zTOR=%s mismatch for loc: %s", zTOR, loc);
				}
				rupCSV.addLine(rupLine);
				
				HashMap<Integer, Location> hypos = erf2db.getHypocenters(
						exampleRun.getERFID(), sourceID, rupID, exampleRun.getRupVarScenID());
				Preconditions.checkState(!hypos.isEmpty(), "No hypos for source=%s, rup=%s", sourceID, rupID);
				for (int rvID=0; rvID<hypos.size(); rvID++) {
					Location hypo = hypos.get(rvID);
					List<String> rvLine = new ArrayList<>(6);
					rvLine.add(sourceID+"");
					rvLine.add(rupID+"");
					rvLine.add(rvID+"");
					rvLine.add((float)hypo.lat+"");
					rvLine.add((float)hypo.lon+"");
					rvLine.add((float)hypo.depth+"");
					rvCSV.addLine(rvLine);
				}
			}
		}
		
		study.getDB().destroy();
		
		rupCSV.writeToFile(new File(outputDir, "erf_"+exampleRun.getERFID()+"_rups.csv"));
		rvCSV.writeToFile(new File(outputDir, "erf_"+exampleRun.getERFID()+"_rv"+exampleRun.getRupVarScenID()+"_rup_vars.csv"));
	}

}
