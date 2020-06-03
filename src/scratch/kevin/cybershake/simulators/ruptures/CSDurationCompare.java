package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import scratch.kevin.cybershake.simCompare.CSRupture;
import scratch.kevin.cybershake.simCompare.StudyRotDProvider;
import scratch.kevin.simCompare.IMT;

public class CSDurationCompare {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_20_5_RSQSIM_4983;
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		
		AttenRelRef gmpeRef = AttenRelRef.AFSHARI_STEWART_2016;
		IMT selectIMT = IMT.DUR_5_75;
		
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		IMT[] imts = { selectIMT };
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(
				study.getDB(), ampsCacheDir, study.getERF());
		StudyRotDProvider prov = new StudyRotDProvider(study, amps2db, imts, study.getName());
		
		List<CybershakeRun> csRuns = study.runFetcher().fetch();
		System.out.println("Loaded "+csRuns.size()+" runs for "+study.getName()+" (dataset "+study.getDatasetIDs()+")");
		
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study, vs30Source, csRuns);
		
		ScalarIMR gmpe = gmpeRef.instance(null);
		gmpe.setParamDefaults();
		selectIMT.setIMT(gmpe);
		
		double maxZScore = Double.NEGATIVE_INFINITY;
		CSRupture maxRup = null;
		Site maxSite = null;
		
		for (Site site : sites) {
			System.out.println("Doing "+site.getName());
			Collection<CSRupture> rups = prov.getRupturesForSite(site);
			for (CSRupture rup : rups) {
				gmpe.setAll(rup.getRup(), site, gmpe.getIntensityMeasure());
				double val = prov.getValue(site, rup, selectIMT, 0);
				double zScore = (Math.log(val) - gmpe.getMean())/gmpe.getStdDev();
				
				if (zScore > maxZScore) {
					maxZScore = zScore;
					maxRup = rup;
					maxSite = site;
				}
			}
		}
		
		System.out.println("Max z-score: "+maxZScore+" for "+maxRup.getSourceID()+", "
				+maxRup.getRupID()+" and site "+maxSite.getName());
		
		study.getDB().destroy();
	}
	
	

}
