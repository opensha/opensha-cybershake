package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;

import com.google.common.base.Preconditions;

import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;

import scratch.kevin.simCompare.IMT;

public class CachePeakAmpsFromSQLite {

	public static void main(String[] args) throws IOException, SQLException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_12;
//		File sqliteFile = new File("/project/scec_608/cybershake/results/sqlite_studies/study_15_12/study_15_12.sqlite");
//		File sqliteFile = new File("/local_tmp/study_15_12.sqlite");
		
		DBAccess regDB = study.getDB();
		DBAccess ampsDB = regDB;
		if (args.length == 1) {
			File sqliteFile = new File(args[0]);
			System.out.println("Using SQLite file: "+sqliteFile.getAbsolutePath());
			Preconditions.checkState(sqliteFile.exists(), "SQLite file not found: %s", sqliteFile.getAbsolutePath());
			ampsDB = Cybershake_OpenSHA_DBApplication.getSQLiteDB(sqliteFile);
		} else {
			System.out.println("Using database directly (supply path to SQLite file to override)");
		}
		File outputDir = new File("/project/scec_608/kmilner/cybershake/amp_cache/study_15_12");
		
		IMT[] imts = { IMT.SA0P1, IMT.SA0P2, IMT.SA0P5, IMT.SA1P0, IMT.SA2P0, IMT.SA3P0, IMT.SA5P0, IMT.SA10P0 };
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(ampsDB, outputDir, study.getERF());
		
		CybershakeIM[] csIMs = new CybershakeIM[imts.length];
		for (int i=0; i<imts.length; i++) {
			IMT imt = imts[i];
			csIMs[i] = CybershakeIM.getSA(CyberShakeComponent.RotD50, imt.getPeriod());
		}
		
		for (int r=0; r<runs.size(); r++) {
			CybershakeRun run = runs.get(r);
			System.out.println("Processing run "+r+"/"+runs.size());
			
			for (CybershakeIM csIM : csIMs)
				amps2db.getAllIM_Values(run.getRunID(), csIM);
		}
		
		regDB.destroy();
		ampsDB.destroy();
	}

}
