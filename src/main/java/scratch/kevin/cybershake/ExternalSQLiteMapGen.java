package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;

import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.RunIDFetcher;
import org.opensha.sha.cybershake.db.CybershakeRun.Status;
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;
import org.opensha.sha.cybershake.maps.GMT_InterpolationSettings;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.cybershake.maps.InterpDiffMap;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;

import com.google.common.base.Preconditions;

public class ExternalSQLiteMapGen {

	public static void main(String[] args) throws IOException {
		if (args.length < 4 || args.length > 5) {
			System.err.println("USAGE: <study> <SQLite file> [<comp SQLite file>] <IM Type ID> <Output Directory>");
			System.exit(1);
		}
		DBAccess mainDB = null;
		DBAccess mainSQLiteDB = null;
		DBAccess compDB = null;
		int exitCode = 0;
		try {
			int index = 0;
			CyberShakeStudy study = CyberShakeStudy.valueOf(args[index++]);
			File mainSQLite = new File(args[index++]);
			Preconditions.checkState(mainSQLite.exists(), "File doesn't exist: %s", mainSQLite.getAbsolutePath());
			File compSQLite = null;
			if (args.length == 5) {
				compSQLite = new File(args[index++]);
				Preconditions.checkState(compSQLite.exists(), "File doesn't exist: %s", compSQLite.getAbsolutePath());
			}
			int imTypeID = Integer.parseInt(args[index++]);
			File outputDir = new File(args[index++]);
			Preconditions.checkState(outputDir.exists() || outputDir.mkdir(),
					"Output dir doesn't exist and couldn't be created: %s", outputDir.getAbsolutePath());
			
			int saveDPI = 300;
			
			boolean isProbAt_IML = false;
			double val = 0.0004;
			String durationLabel = "2% in 50 yrs";
			
			String dbProp = System.getProperty("cybershake.db.host");
			if (dbProp != null && !dbProp.isBlank())
				mainDB = study.getSQLiteDB(new File(dbProp));
			else
				mainDB = study.getDB();
			HazardCurve2DB mainCurvesDB = new HazardCurve2DB(mainDB);
			CybershakeIM im = mainCurvesDB.getIMFromID(imTypeID);
			
			double period = im.getVal();
			String imtLabel = (int)period+"sec "+im.getComponent().getShortName()+" SA";
			System.out.println("Doing "+imtLabel);
			Double customMax = 1d;
			if (period >= 5)
				customMax = 0.6;
			if (period >= 10)
				customMax = 0.4;
			
			String imtPrefix = imtLabel.replaceAll(" ", "_");
			
			RunIDFetcher fetcher = new RunIDFetcher(mainDB).noTestSites().unique(true);
			List<CybershakeRun> runs = fetcher.fetch();
			System.out.println("Fetched "+runs.size()+" runs from "+study);
			if (runs.isEmpty()) {
				System.out.println("Found no runs; debugging the runs table");
				String sql = "SELECT * FROM CyberShake_Runs";
				
				ResultSet rs = mainDB.selectData(sql);
				boolean valid = rs.next();
				
				while (valid) {
					CybershakeRun run = CybershakeRun.fromResultSet(rs);
					System.out.println("Run: "+run);
					runs.add(run);
					
					valid = rs.next();
				}
				
				rs.close();
			}
			Preconditions.checkState(!runs.isEmpty(), "Found no runs for "+study);
			
			mainSQLiteDB = Cybershake_OpenSHA_DBApplication.getSQLiteDB(mainSQLite);
			
			System.out.println("Fetching primary hazard curves (from passed in SQLite: "+mainSQLite.getName()+")");
			HazardCurveFetcher mainFetch = new HazardCurveFetcher(
					study.getDB(), runs, study.getDatasetIDs(), imTypeID, mainSQLiteDB);
			ArbDiscrGeoDataSet mainScatter = HardCodedInterpDiffMapCreator.getMainScatter(
					isProbAt_IML, val, mainFetch, imTypeID, null);
			
			compDB = null;
			HazardCurveFetcher compFetch;
			if (compSQLite == null) {
				System.out.println("Fetching comparison hazard curves (from the regular study DB)");
				compFetch = new HazardCurveFetcher(study.getDB(), runs, study.getDatasetIDs(), imTypeID);
			} else {
				System.out.println("Fetching comparison hazard curves (from passed in SQLite: "+compSQLite.getName()+")");
				compDB = Cybershake_OpenSHA_DBApplication.getSQLiteDB(compSQLite);
				compFetch = new HazardCurveFetcher(study.getDB(), runs, study.getDatasetIDs(), imTypeID, compDB);
			}
			ArbDiscrGeoDataSet compScatter = HardCodedInterpDiffMapCreator.getMainScatter(
					isProbAt_IML, val, compFetch, imTypeID, null);
			
			System.out.println("Writing hazard maps");
			ArbDiscrGeoDataSet.writeXYZFile(mainScatter, new File(outputDir, imtPrefix+"_map_data.xyz"));
			ArbDiscrGeoDataSet.writeXYZFile(compScatter, new File(outputDir, "comp_"+imtPrefix+"_map_data.xyz"));
			
			System.out.println("Plotting ratio and difference maps");
			boolean logPlot = false;
			boolean tightCPTs = false;
			String label = imtLabel;
			String[] addrs = HardCodedInterpDiffMapCreator.getCompareMap(
					logPlot, mainScatter, compScatter, label, tightCPTs, study.getRegion());
			
			String diff = addrs[0];
			String ratio = addrs[1];
			
			System.out.println("Comp map address:\n\tdiff: "+diff+"\n\tratio: "+ratio);
			
			if (outputDir != null) {
				HardCodedInterpDiffMapCreator.fetchPlot(diff, "interpolated_marks.150.png",
						new File(outputDir, "diff_"+imtPrefix+".png"));
				HardCodedInterpDiffMapCreator.fetchPlot(diff, "interpolated_marks.ps",
						new File(outputDir, "diff_"+imtPrefix+".ps"));
				HardCodedInterpDiffMapCreator.fetchPlot(ratio, "interpolated_marks.150.png",
						new File(outputDir, "ratio_"+imtPrefix+".png"));
				HardCodedInterpDiffMapCreator.fetchPlot(ratio, "interpolated_marks.ps",
						new File(outputDir, "ratio_"+imtPrefix+".ps"));
				if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN) {
					FileUtils.deleteRecursive(new File(diff));
					FileUtils.deleteRecursive(new File(ratio));
				}
			}
			
			logPlot = false;
			Double customMin = null;
			if (customMax != null)
				customMin = 0d;
			
			label = imtLabel+", "+durationLabel;

			System.out.println("Making primary (from SQLite) map...");
			GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
			
			InterpDiffMapType[] mapTypes = new InterpDiffMapType[] { InterpDiffMapType.INTERP_NOMARKS,
						InterpDiffMapType.INTERP_MARKS };
			
			CPT cpt = CyberShake_GMT_MapGenerator.getHazardCPT();
			
			double spacing = 0.01;
			
			InterpDiffMap map = new InterpDiffMap(study.getRegion(), null, spacing, cpt, mainScatter, interpSettings, mapTypes);
			map.setCustomLabel(label);
			map.setTopoResolution(TopographicSlopeFile.CA_THREE);
			map.setLogPlot(logPlot);
			map.setDpi(300);
			map.setXyzFileName("primary_map_data.xyz");
			map.setCustomScaleMin(customMin);
			map.setCustomScaleMax(customMax);
			
			String metadata = label;
			
			String addr;
			if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
				addr = HardCodedInterpDiffMapCreator.plotLocally(map);
			else
				addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
			
			System.out.println("Map address: " + addr);
			if (outputDir != null) {
				String prefix = imtPrefix;
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated_marks."+saveDPI+".png",
							new File(outputDir, prefix+"_marks.png"));
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated."+saveDPI+".png",
						new File(outputDir, prefix+".png"));
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated.ps",
						new File(outputDir, prefix+".ps"));
				if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
					FileUtils.deleteRecursive(new File(addr));
			}
			
			if (compSQLite == null)
				System.out.println("Making comparison (from original DB) map...");
			else
				System.out.println("Making comparison (from SQLite) map...");
			map = new InterpDiffMap(study.getRegion(), null, spacing, cpt, mainScatter, interpSettings, mapTypes);
			map.setCustomLabel(label);
			map.setTopoResolution(TopographicSlopeFile.CA_THREE);
			map.setLogPlot(logPlot);
			map.setDpi(300);
			map.setXyzFileName("comparison_map_data.xyz");
			map.setCustomScaleMin(customMin);
			map.setCustomScaleMax(customMax);
			
			metadata = label;
			
			if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
				addr = HardCodedInterpDiffMapCreator.plotLocally(map);
			else
				addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
			
			System.out.println("Map address: " + addr);
			if (outputDir != null) {
				String prefix = "comp_"+imtPrefix;
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated_marks."+saveDPI+".png",
							new File(outputDir, prefix+"_marks.png"));
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated."+saveDPI+".png",
						new File(outputDir, prefix+".png"));
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated.ps",
						new File(outputDir, prefix+".ps"));
				if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
					FileUtils.deleteRecursive(new File(addr));
			}
		} catch (Exception e) {
			e.printStackTrace();
			exitCode = 1;
		} finally {
			if (mainDB != null)
				mainDB.destroy();
			if (mainSQLiteDB != null)
				mainSQLiteDB.destroy();
			if (compDB != null)
				compDB.destroy();
		}
		System.exit(exitCode);
	}

}
