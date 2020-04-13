package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeRun;

import com.google.common.base.Preconditions;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;
import scratch.kevin.simulators.ruptures.rotation.RSQSimRotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.Quantity;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.RotationSpec;

public class RSQSimCurveDataCSVWriter {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_20_2_RSQSIM_ROT_4860_10X;
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		
		double[] periods = { 3d };
		
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		RSQSimRotatedRuptureFakeERF erf = (RSQSimRotatedRuptureFakeERF)study.getERF();
		erf.setLoadRuptures(false);
		
		String[] siteNames = { "USC", "SMCA", "OSI", "WSS", "SBSM",
				"LAF", "s022", "STNI", "WNGC", "PDE" };
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, study.getERF());
		CSRotatedRupSimProv simProv = new CSRotatedRupSimProv(study, amps2db, periods);
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		for (String siteName : siteNames) {
			List<CybershakeRun> matchingRuns = study.runFetcher().forSiteNames(siteName).fetch();
			Preconditions.checkState(!matchingRuns.isEmpty(), "Must have at least 1 run for the given site/study");
			Site site = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, matchingRuns).get(0);
			
			Map<Scenario, RSQSimRotatedRupVariabilityConfig> configMap = erf.getConfigMap();
			
			for (Scenario scen : configMap.keySet()) {
				RSQSimRotatedRupVariabilityConfig config = configMap.get(scen);
				List<Site> sites = new ArrayList<>();
				sites.add(site);
				config = config.forSites(sites);
				List<RotationSpec> rots = config.getRotations();
				System.out.println("Have "+rots.size()+" rots for "+scen.getName()+", "+site.getName());
				CSVFile<String> csv = new CSVFile<>(true);
				List<String> header = new ArrayList<>();
				header.add("Event ID");
//				header.add("Magniutde");
				header.add("Distance Rup (km)");
				header.add("Source Rotation Azimuth (degrees)");
				header.add("Site-To-Source Path Azimuth (degrees)");
				for (double period : periods) {
					String periodStr = (period == Math.floor(period) ? (int)period : (float)period) + "s";
					header.add("ln("+periodStr+" RotD50)");
				}
				csv.addLine(header);
				for (RotationSpec spec : rots) {
					List<String> line = new ArrayList<>();
					line.add(spec.eventID+"");
//					line.add((float)simProv.getMagnitude(spec)+"");
					line.add(spec.distance+"");
					Float sourceAz = spec.sourceAz;
					if (sourceAz == null)
						sourceAz = 0f;
					line.add(sourceAz.toString());
					Float pathAz = spec.siteToSourceAz;
					if (pathAz == null)
						pathAz = 0f;
					line.add(pathAz.toString());
					DiscretizedFunc rd50 = simProv.getRotD50(site, spec, 0);
					for (double period : periods)
						line.add((float)Math.log(rd50.getInterpolatedY(period))+"");
					csv.addLine(line);
				}
				File rotDir = new File(studyDir, "rotated_ruptures_"+scen.getPrefix());
				Preconditions.checkState(rotDir.exists() || rotDir.mkdir());
				File resourcesDir = new File(rotDir, "resources");
				Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
				File outputFile = new File(resourcesDir, site.getName()+"_"+scen.getPrefix()+"_rd50s.csv.gz");
				System.out.println("Writing to: "+outputFile.getAbsolutePath());
				csv.writeToStream(new GZIPOutputStream(new FileOutputStream(outputFile)));
			}
		}
		
		study.getDB().destroy();
	}

}
