package scratch.kevin.cybershake;

import java.util.List;

import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;

public class StudyVs30Print {

	public static void main(String[] args) {
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		for (CybershakeRun run : runs)
			System.out.println(run.getRunID()+"\t"+run.getSiteID()+"\tmodel="+run.getModelVs30()
				+"\tmesh="+run.getMeshVsitop()+"\tmin="+run.getMinimumVs()+"\tsrc="+run.getVs30Source());
		
		study.getDB().destroy();
	}

}
