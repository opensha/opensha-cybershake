package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF.RSQSimRotatedRuptureSource;
import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.Quantity;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.RotationSpec;

public class RotRupSimCompare {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_19_3_RSQSIM_ROT_2585;
		RSQSimCatalog catalog = study.getRSQSimCatalog();

		File csRotDir = new File(catalog.getCatalogDir(), "cybershake_rotation_inputs");
		Map<Scenario, RotatedRupVariabilityConfig> rotConfigs =
				RSQSimRotatedRuptureFakeERF.loadRotationConfigs(catalog, csRotDir, true);
		RSQSimRotatedRuptureFakeERF erf = new RSQSimRotatedRuptureFakeERF(catalog, rotConfigs);
		
		File outputDir = new File(csRotDir, "run_debug");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		int runID1 = 7014;
		int runID2 = 7054;
		
		CybershakeIM im = CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d);
		
		DBAccess db = study.getDB();
		Runs2DB runs2db = new Runs2DB(db);
		
		CybershakeRun run1 = runs2db.getRun(runID1);
		CybershakeRun run2 = runs2db.getRun(runID2);
		
		Preconditions.checkNotNull(run1);
		Preconditions.checkNotNull(run2);
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, null, study.getERF());
		
		double[][][] amps1;
		double[][][] amps2;
		try {
			amps1 = amps2db.getAllIM_Values(run1.getRunID(), im);
			amps2 = amps2db.getAllIM_Values(run2.getRunID(), im);
		} catch (SQLException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		study.getDB().destroy();
		
		Table<Quantity, Object, MinMaxAveTracker> qValDiffTracks = HashBasedTable.create();
		Table<Quantity, Object, MinMaxAveTracker> qValRatioTracks = HashBasedTable.create();
		Quantity[] quantities = {Quantity.EVENT_ID, Quantity.DISTANCE,
				Quantity.SOURCE_AZIMUTH, Quantity.SITE_TO_SOURTH_AZIMUTH};
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			if (amps1[sourceID] == null || amps1[sourceID].length == 0)
				continue;
			RSQSimRotatedRuptureSource source = erf.getSource(sourceID);
			System.out.println("Source "+sourceID+": "+source.getName());
			List<RotationSpec> rots = source.getRotations();
			Preconditions.checkState(rots.size() == amps1[sourceID].length);
			for (int rupID=0; rupID<rots.size(); rupID++) {
				RotationSpec rot = rots.get(rupID);
				
				double v1 = amps1[sourceID][rupID][0];
				double v2 = amps2[sourceID][rupID][0];
				
				double absDiff = Math.abs(v1 - v2);
				double ratio = v1 > v2 ? v1/v2 : v2/v1;
				
				for (Quantity q : quantities) {
					Object value = rot.getValue(q);
					if (value == null)
						value = 0f;
					MinMaxAveTracker diffTrack = qValDiffTracks.get(q, value);
					if (diffTrack == null) {
						diffTrack = new MinMaxAveTracker();
						qValDiffTracks.put(q, value, diffTrack);
						qValRatioTracks.put(q, value, new MinMaxAveTracker());
					}
					MinMaxAveTracker ratioTrack = qValRatioTracks.get(q, value);
					diffTrack.addValue(absDiff);
					ratioTrack.addValue(ratio);
				}
			}
		}
		
		Map<Scenario, RotatedRupVariabilityConfig> configMap = erf.getConfigMap();
		List<Scenario> scenarios = new ArrayList<>();
		for (Scenario s : Scenario.values())
			if (configMap.containsKey(s))
				scenarios.add(s);
		for (Quantity q : quantities) {
			List<Object> values = new ArrayList<>();
			if (q == Quantity.EVENT_ID) {
				for (Scenario s : scenarios)
					values.addAll(configMap.get(s).getQuantitiesMap().get(q));
			} else {
				values.addAll(configMap.get(scenarios.get(0)).getQuantitiesMap().get(q));
			}
			CSVFile<String> csv = new CSVFile<>(true);
			csv.addLine("Value", "Mean Abs Diff", "Max Abs Diff", "Mean Ratio", "Max Ratio");
			for (Object value : values) {
				List<String> line = new ArrayList<>();
				line.add(value.toString());
				MinMaxAveTracker diffTrack = qValDiffTracks.get(q, value);
				MinMaxAveTracker ratioTrack = qValRatioTracks.get(q, value);
				line.add((float)diffTrack.getAverage()+"");
				line.add((float)diffTrack.getMax()+"");
				line.add((float)ratioTrack.getAverage()+"");
				line.add((float)ratioTrack.getMax()+"");
				csv.addLine(line);
			}
			csv.writeToFile(new File(outputDir, q.name()+".csv"));
		}
	}

}
