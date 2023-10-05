package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.faultSysSolution.FaultSystemSolution;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.FaultTrace;
import org.opensha.sha.faultSurface.RuptureSurface;

public class U2SimilarToU3RuptureSearch {

	public static void main(String[] args) throws IOException {
		FaultSystemSolution sol = FaultSystemSolution.load(new File("/home/kevin/OpenSHA/UCERF4/rup_sets/fm3_1_ucerf3.zip"));
		
//		int u3RupIndex = 225703; // Newport-Inglewood 7.2
//		double targetMag = 7.2;
//		Location targetHypo = new Location(33.6, -117.9);
		
//		int u3RupIndex = 218307; // Hollywood 6.5
//		double targetMag = 6.5;
//		Location targetHypo = new Location(34.1, -118.5);
		
//		int u3RupIndex = 231731; // Santa Monica 6.5
//		double targetMag = 6.5;
//		Location targetHypo = new Location(34.1, -118.5);
		
		int u3RupIndex = 231451; // Palos Verdes 7
		double targetMag = 7;
		Location targetHypo = new Location(33.75, -118.3);
		
//		int u3RupIndex = 101728; // Haywired 7.0
//		double targetMag = 7d;
//		Location targetHypo = null;
		
		double distTol = 15d;
		double magTol = 0.1;
		double hypoDistTol = 15d;
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		ERF2DB erf2db = targetHypo != null && study != null ? new ERF2DB(study.getDB()) : null;
		
		RuptureSurface u3Surf = sol.getRupSet().getSurfaceForRupture(u3RupIndex, 1d);
		Location start = u3Surf.getUpperEdge().first();
		Location end = u3Surf.getUpperEdge().last();
		
		MeanUCERF2 u2 = new MeanUCERF2();
		u2.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_EXCLUDE);
		u2.updateForecast();
		
		for (int sourceID=0; sourceID<u2.getNumSources(); sourceID++) {
			ProbEqkSource source = u2.getSource(sourceID);
			List<ProbEqkRupture> candidates = new ArrayList<>();
			List<double[]> dists = new ArrayList<>();
			List<Integer> indexes = new ArrayList<>();
			
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup  = source.getRupture(rupID);
				double mag = rup.getMag();
				double magDelta = Math.abs(mag - targetMag);
				if (magDelta > magTol)
					continue;
				FaultTrace trace;
				try {
					trace = rup.getRuptureSurface().getUpperEdge();
				} catch (Exception e) {
					trace = rup.getRuptureSurface().getEvenlyDiscritizedUpperEdge();
				}
				
				double dist1 = Math.min(LocationUtils.horzDistance(start, trace.first()), LocationUtils.horzDistance(start, trace.last()));
				double dist2 = Math.min(LocationUtils.horzDistance(end, trace.first()), LocationUtils.horzDistance(end, trace.last()));
				
				if (dist1 < distTol && dist2 < distTol) {
					dists.add(new double[] {dist1, dist2});
					candidates.add(rup);
					indexes.add(rupID);
				}
			}
			
			if (!candidates.isEmpty()) {
				System.out.println(sourceID+". "+source.getName());
				for (int i=0; i<candidates.size(); i++) {
					ProbEqkRupture rup = candidates.get(i);
					int rupID = indexes.get(i);
					double[] dist = dists.get(i);
					System.out.println("\tRup "+rupID+", M"+(float)rup.getMag()+"; dists: "+(float)dist[0]+", "+(float)dist[1]);
					if (erf2db != null) {
						HashMap<Integer, Location> hypos = erf2db.getHypocenters(
								study.getERF_ID(), sourceID, rupID, study.getRupVarScenarioID());
						System.out.println("\t\tFound "+hypos.size()+" RVs:");
						for (int rvID : hypos.keySet()) {
							Location hypo = hypos.get(rvID);
							double hypoDist = LocationUtils.horzDistanceFast(targetHypo, hypo);
							if (hypoDist < hypoDistTol)
								System.out.println("\t\tRV "+rvID+": "+hypo+" ("+(float)hypoDist+" km away)");
						}
					}
				}
			}
		}
		
		if (erf2db != null)
			study.getDB().destroy();
	}

}
