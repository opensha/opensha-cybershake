package scratch.kevin.cybershake;

import java.io.File;
import java.sql.SQLException;

import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;

import com.google.common.base.Preconditions;

public class ScatterDebug {

	public static void main(String[] args) throws SQLException {
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;

		CybershakeRun run = new Runs2DB(study.getDB()).getRun(9672);
//		CybershakeRun run = study.runFetcher().forSiteNames("STNI").fetch().get(0);
		
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		AbstractERF erf = study.getERF();
		
		CybershakeIM im = CybershakeIM.getSA(CyberShakeComponent.RotD50, 0.1);
		
		Range magRange = new Range(6.5, 7);
		Range distRange = new Range(20d, 40d);
		
		MinMaxAveTracker overallTrack = new MinMaxAveTracker();
		
		int[] minIndexes = null;
		int[] maxIndexes = null;
		
		System.out.println("Run ID: "+run.getRunID());
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, erf);
		
		Location loc = new SiteInfo2DB(study.getDB()).getLocationForSiteID(run.getSiteID());
		
		System.out.println("Loading amps...");
		double[][][] amps = amps2db.getAllIM_Values(run.getRunID(), im);
		System.out.println("Loaded amps");
		
		Site site = new Site(loc);
		
		Preconditions.checkState(amps.length == erf.getNumSources());
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			double[][] sourceAmps = amps[sourceID];
			if (sourceAmps == null)
				continue;
			ProbEqkSource source = erf.getSource(sourceID);
			Preconditions.checkArgument(sourceAmps.length == source.getNumRuptures());
			
			double minDist = source.getMinDistance(site);
			if (minDist > distRange.getUpperBound())
				continue;
			
			MinMaxAveTracker sourceTrack = new MinMaxAveTracker();
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				double[] rupAmps = sourceAmps[rupID];
				if (rupAmps == null)
					continue;
				ProbEqkRupture rup = erf.getRupture(sourceID, rupID);
				if (!magRange.contains(rup.getMag()))
					continue;
				double dist = rup.getRuptureSurface().getDistanceJB(loc);
//				double dist = rup.getRuptureSurface().getDistanceRup(loc);
				if (!distRange.contains(dist))
					continue;
				for (int rvID=0; rvID<rupAmps.length; rvID++) {
					double amp = rupAmps[rvID];
					amp /= HazardCurveComputation.CONVERSION_TO_G;
					if (amp < overallTrack.getMin())
						minIndexes = new int[] {sourceID, rupID, rvID};
					if (amp < overallTrack.getMax())
						maxIndexes = new int[] {sourceID, rupID, rvID};
					sourceTrack.addValue(amp);
					overallTrack.addValue(amp);
				}
			}
			if (sourceTrack.getNum() > 0)
				System.out.println("Amps for "+sourceID+". "+source.getName()+":\n\t"+sourceTrack);
		}
		System.out.println("All amps:\n\t"+overallTrack);
		for (boolean isMin : new boolean[] {true,false}) {
			int[] indexes;
			double val;
			if (isMin) {
				System.out.println("Smallest amp:");
				indexes = minIndexes;
				val = overallTrack.getMin();
			} else {
				System.out.println("Largest amp:");
				indexes = maxIndexes;
				val = overallTrack.getMax();
			}
			ProbEqkSource source = erf.getSource(indexes[0]);
			ProbEqkRupture rup = source.getRupture(indexes[1]);
			System.out.println("\tSource "+indexes[0]+". "+source.getName()+", Rupture "+indexes[1]+", RV "+indexes[2]);
			System.out.println("\tMag: "+(float)rup.getMag()
					+", rJB: "+(float)rup.getRuptureSurface().getDistanceJB(loc)
					+", rRup: "+(float)rup.getRuptureSurface().getDistanceRup(loc));
			System.out.println("\tGM: "+(float)val+" g, "+(float)(val*HazardCurveComputation.CONVERSION_TO_G)+" cm/s");
			
		}
		
		study.getDB().destroy();
	}

}
