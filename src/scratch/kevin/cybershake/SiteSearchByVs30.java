package scratch.kevin.cybershake;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;

public class SiteSearchByVs30 {

	public static void main(String[] args) {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		double minVs30 = 750;
		double maxVs30 = 1000;
		
		Runs2DB run2db = new Runs2DB(study.getDB());
		SiteInfo2DB sites2db = new SiteInfo2DB(study.getDB());
		
		HazardCurveFetcher curveFetch = new HazardCurveFetcher(study.getDB(), study.getDatasetID(),
				CybershakeIM.getSA(CyberShakeComponent.RotD100, 3d).getID());
		
		List<Integer> runIDs = curveFetch.getRunIDs();
		Collections.shuffle(runIDs);
		
		Map<CybershakeSite, Double> matches = new HashMap<>();
		
		for (int runID : runIDs) {
			CybershakeRun run = run2db.getRun(runID);
			double vs30 = run.getVs30();
			if (vs30 >= minVs30 && vs30 <= maxVs30)
				matches.put(sites2db.getSiteFromDB(run.getSiteID()), vs30);
		}
		
		System.out.println("Found "+matches.size()+" sites");
		
		for (CybershakeSite site : matches.keySet())
			System.out.println(site.short_name+"\t"+matches.get(site)+"\t"+(float)site.lat+"\t"+(float)site.lon);
		
		study.getDB().destroy();
	}

}
