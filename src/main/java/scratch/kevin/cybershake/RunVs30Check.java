package scratch.kevin.cybershake;

import java.util.List;

import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

public class RunVs30Check {

	public static void main(String[] args) {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_12;
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		List<CyberShakeSiteRun> siteRuns = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		
		for (CyberShakeSiteRun site : siteRuns) {
			CybershakeRun run = site.getCS_Run();
			double vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
			System.out.println("Run "+run.getRunID()+", "+site.getName());
			System.out.println("\tVsitop:\t"+run.getMeshVsitop());
			System.out.println("\tModel:\t"+run.getModelVs30());
			System.out.println("\tMin:\t"+run.getMinimumVs());
			System.out.println("\tChosen:\t"+vs30);
		}
		
		study.getDB().destroy();
	}

}
