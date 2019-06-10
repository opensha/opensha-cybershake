package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.Quantity;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.RotationSpec;

public class CSRotatedRupCSVWriter {

	public static void main(String[] args) throws IOException {
		File rotDir = new File("/home/kevin/Simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_19_3_RSQSIM_ROT_2585;
		RSQSimRotatedRuptureFakeERF erf = (RSQSimRotatedRuptureFakeERF) study.getERF();
		erf.setLoadRuptures(false);
		Map<Scenario, RotatedRupVariabilityConfig> rotConfigs = erf.getConfigMap();
		
		File outputDir = new File(rotDir, "result_csvs");
		
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		String[] siteNames = { "USC", "PAS", "SBSM", "WNGC", "STNI", "SMCA" };
		List<Site> sites = null;
		
		double[] periods = { 3, 5, 10 };
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, erf);
		CSRotatedRupSimProv simProv = new CSRotatedRupSimProv(study, amps2db, periods);
		
		for (Scenario scenario : rotConfigs.keySet()) {
			RotatedRupVariabilityConfig config = rotConfigs.get(scenario);
			List<CSVFile<String>> csvs = new ArrayList<>();
			
			if (sites == null) {
				sites = new ArrayList<>(config.getValues(Site.class, Quantity.SITE));
				for (int i=sites.size(); --i>=0;) {
					String name = sites.get(i).getName();
					boolean found = false;
					for (String siteName : siteNames)
						if (name.equals(siteName))
							found = true;
					if (!found)
						sites.remove(i);
				}
			}
			
			List<String> header = new ArrayList<>();
			header.add("Event ID");
			header.add("Source Rotation Azimuth");
			header.add("Site-To-Source Azimuth");
			header.add("Distance");
			for (Site site : sites)
				header.add(site.getName());
			
			for (int i=0; i<periods.length; i++) {
				CSVFile<String> csv = new CSVFile<>(true);
				csv.addLine(header);
				csvs.add(csv);
			}
			
			for (int eventID : config.getValues(Integer.class, Quantity.EVENT_ID)) {
				for (Float sourceAz : config.getValues(Float.class, Quantity.SOURCE_AZIMUTH)) {
					for (Float siteSourceAz : config.getValues(Float.class, Quantity.SITE_TO_SOURTH_AZIMUTH)) {
						for (Float distance : config.getValues(Float.class, Quantity.DISTANCE)) {
							List<String> line = new ArrayList<>();
							line.add(eventID+"");
							line.add(sourceAz == null ? "0.0" : sourceAz.toString());
							line.add(siteSourceAz == null ? "0.0" : siteSourceAz.toString());
							line.add(distance == null ? "0.0" : distance.toString());
							for (int p=0; p<periods.length; p++) {
								List<String> periodLine = new ArrayList<>(line);
								for (Site site : sites) {
									RotationSpec rot = new RotationSpec(-1, site, eventID, distance, sourceAz, siteSourceAz);
									DiscretizedFunc func = simProv.getRotD50(site, rot, 0);
									periodLine.add((float)func.getY(periods[p])+"");
								}
								csvs.get(p).addLine(periodLine);
							}
						}
					}
				}
			}
			
			for (int p=0; p<periods.length; p++) {
				CSVFile<String> csv = csvs.get(p);
				csv.writeToFile(new File(outputDir, scenario.getPrefix()+"_"+(float)periods[p]+"s.csv"));
			}
		}
		
		study.getDB().destroy();
	}

}
