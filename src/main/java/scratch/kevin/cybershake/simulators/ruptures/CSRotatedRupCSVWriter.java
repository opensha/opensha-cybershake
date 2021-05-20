package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import scratch.kevin.simCompare.IMT;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.ruptures.ASK_EventData;
import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;
import scratch.kevin.simulators.ruptures.rotation.RSQSimRotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.Quantity;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.RotationSpec;

public class CSRotatedRupCSVWriter {

	public static void main(String[] args) throws IOException {
		File rotDir = new File("/home/kevin/Simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_19_3_RSQSIM_ROT_2585;
		RSQSimRotatedRuptureFakeERF erf = (RSQSimRotatedRuptureFakeERF) study.getERF();
		erf.setLoadRuptures(false);
		Map<Scenario, RSQSimRotatedRupVariabilityConfig> rotConfigs = erf.getConfigMap();
		
		File outputDir = new File(rotDir, "result_csvs");
		
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		String[] siteNames = { "USC", "PAS", "SBSM", "WNGC", "STNI", "SMCA" };
		List<Site> sites = null;
		
//		double[] periods = { 3, 5, 10 };
//		double[] periods = { 3 };
		IMT[] imts = { IMT.SA3P0 };
		
		int numDownsamples = 100;
		List<Map<Integer, List<ASK_EventData>>> askDatas = null;
		if (numDownsamples > 0) {
			askDatas = new ArrayList<>();
			for (IMT imt : imts) {
				if (imt.getParamName().equals(SA_Param.NAME))
					askDatas.add(ASK_EventData.load(imt.getPeriod()));
				else
					askDatas.add(null);
			}
		}
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, erf);
		CSRotatedRupSimProv simProv = new CSRotatedRupSimProv(study, amps2db, imts);
		
		for (Scenario scenario : rotConfigs.keySet()) {
			RSQSimRotatedRupVariabilityConfig config = rotConfigs.get(scenario);
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
			
			for (int i=0; i<imts.length; i++) {
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
							for (int p=0; p<imts.length; p++) {
								List<String> periodLine = new ArrayList<>(line);
								for (Site site : sites) {
									RotationSpec rot = new RotationSpec(-1, site, eventID, distance, sourceAz, siteSourceAz);
									periodLine.add((float)simProv.getValue(site, rot, imts[0], 0)+"");
								}
								csvs.get(p).addLine(periodLine);
							}
						}
					}
				}
			}
			
			for (int p=0; p<imts.length; p++) {
				CSVFile<String> csv = csvs.get(p);
				csv.writeToFile(new File(outputDir, scenario.getPrefix()+"_"+imts[p].getPrefix()+".csv"));
			}
			
			if (numDownsamples > 0) {
				double dm = 0.2;
				double minMag = scenario.getMagnitude()-dm;
				double maxMag = scenario.getMagnitude()+dm;
				for (int p=0; p<imts.length; p++) {
					File dsDir = new File(outputDir, scenario.getPrefix()+"_"+imts[p].getPrefix()+"_downsampled");
					Preconditions.checkState(dsDir.exists() || dsDir.mkdir());
					Map<Integer, List<ASK_EventData>> askData = askDatas.get(p);
					askData = ASK_EventData.getMatches(askData,
							com.google.common.collect.Range.closed(minMag, maxMag),
							null, null, 0d);
					List<List<ASK_EventData>> askDataList = new ArrayList<>();
					for (List<ASK_EventData> value : askData.values())
						askDataList.add(value);
					for (int i=0; i<numDownsamples; i++) {
						CSVFile<String> csv = new CSVFile<>(true);
						csv.addLine(header);
						List<Integer> eventIDs = config.getValues(Integer.class, Quantity.EVENT_ID);
						// draw random set of events
						if (eventIDs.size() > askData.size()) {
							eventIDs = new ArrayList<>(eventIDs);
							Collections.shuffle(eventIDs);
							eventIDs = eventIDs.subList(0, askData.size());
						}
						
						for (int j=0; j<eventIDs.size(); j++) {
							int eventID = eventIDs.get(j);
							List<ASK_EventData> data = askDataList.get(j);
							for (Float distance : config.getValues(Float.class, Quantity.DISTANCE)) {
								double dr = distance == null || distance > 80 ? 20 : 10;
								Range<Double> dRange = Range.closed(distance-dr, distance+dr);
								int numData = 0;
								for (ASK_EventData d : data)
									if (dRange.contains(d.rRup))
										numData++;
								if (numData == 0)
									continue;
								Collection<RotationSpec> rots = config.getRotationsForQuantities(Quantity.EVENT_ID, eventID,
										Quantity.DISTANCE, distance, Quantity.SITE, sites.get(0));
								if (rots.size() > numData) {
									// draw random set of rotations
									List<RotationSpec> newRots = new ArrayList<>(rots);
									Collections.shuffle(newRots);
									rots = newRots.subList(0, numData);
								}
								for (RotationSpec rot : rots) {
									List<String> line = new ArrayList<>();
									line.add(eventID+"");
									line.add(rot.sourceAz == null ? "0.0" : rot.sourceAz.toString());
									line.add(rot.siteToSourceAz == null ? "0.0" : rot.siteToSourceAz.toString());
									line.add(distance == null ? "0.0" : distance.toString());
									for (Site site : sites) {
										rot = new RotationSpec(-1, site, eventID, distance, rot.sourceAz, rot.siteToSourceAz);
										line.add((float)simProv.getValue(site, rot, imts[p], 0)+"");
									}
									csv.addLine(line);
								}
							}
						}
						csv.writeToFile(new File(dsDir, i+".csv"));
					}
				}
			}
		}
		
		study.getDB().destroy();
	}

}
