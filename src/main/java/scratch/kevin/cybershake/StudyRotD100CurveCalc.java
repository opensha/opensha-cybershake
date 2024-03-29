package scratch.kevin.cybershake;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

public class StudyRotD100CurveCalc {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		CybershakeIM im = CybershakeIM.getSA(CyberShakeComponent.RotD100, 0.01);
		
		boolean checkOnly = false;
		
		DBAccess.PRINT_ALL_QUERIES = true;
		DBAccess db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(true, true);
		
		int dbID = study.getDatasetIDs()[0];
		
		HazardCurve2DB curves2db = new HazardCurve2DB(db);
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		List<Double> xValList = new ArrayList<>();
		for (Point2D pt : xVals)
			xValList.add(pt.getX());
		HazardCurveComputation calc = new HazardCurveComputation(db);
		calc.setPeakAmpsAccessor(new CachedPeakAmplitudesFromDB(db, null, study.buildNewERF()));
		
		int exitCode = 0;
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		int numAlreadyDone = 0;
		int numRuns = runs.size();
		int numCalculated = 0;
		
		try {
			for (int i=0; i<runs.size(); i++) {
				CybershakeRun run = runs.get(i);
				System.out.println("Processing run "+i+"/"+numRuns+", runID="+run.getRunID());
				int id = curves2db.getHazardCurveID(run.getRunID(), dbID, im.getID());
				if (id >= 0)
					numAlreadyDone++;
				if (id < 0 && !checkOnly) {
					// calculate it
					System.out.println("Calculating for run "+run.getRunID());
					DiscretizedFunc curve = calc.computeHazardCurve(xValList, run, im);
					System.out.println(curve);
					if (!db.isReadOnly()) {
						curves2db.insertHazardCurve(run, im.getID(), curve);
						id = curves2db.getHazardCurveID(run.getRunID(), im.getID());
						System.out.println("Inserted with Curve_ID="+id);
					}
					numCalculated++;
				}
			}
			
			System.out.println("Calculated "+numCalculated+"/"+numRuns+" curves");
			System.out.println(numAlreadyDone+"/"+numRuns+" were already done");
		} catch (Exception e) {
			e.printStackTrace();
			exitCode = 1;
		} finally {
			db.destroy();
			System.exit(exitCode);
		}
	}

}
