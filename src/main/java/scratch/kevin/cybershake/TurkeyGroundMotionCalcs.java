package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.geo.json.Feature;
import org.opensha.commons.geo.json.FeatureCollection;
import org.opensha.commons.geo.json.FeatureProperties;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGA_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.util.component.ComponentConverter;
import org.opensha.sha.util.component.ComponentTranslation;

import com.google.common.base.Preconditions;

public class TurkeyGroundMotionCalcs {

	public static void main(String[] args) throws IOException, SQLException {
		File outputDir = new File("/home/kevin/CyberShake/2023_turkey_comps");
		
		System.out.println("Parsing turkey shakemap data");
		// from: https://earthquake.usgs.gov/product/shakemap/us6000jllz/us/1678396661938/download/stationlist.json
		File stationsFile = new File(outputDir, "stationlist.json");
		
		FeatureCollection features = FeatureCollection.read(stationsFile);
		
		double maxPGA = 0d;
		String maxPGAStation = null;
		double maxSA0p3 = 0d;
		String maxSA0p3Station = null;
		double maxSA3 = 0d;
		String maxSA3Station = null;
		for (Feature feature : features) {
			FeatureProperties props = feature.properties;
			String type = props.get("station_type", null);
			if (type == null || !type.equals("seismic"))
				continue;
			
			double pga = props.getDouble("pga", Double.NaN)/100d;
//			System.out.println(feature.id);
//			System.out.println("\tPGA: "+(float)pga);
			
			if (pga > maxPGA) {
				maxPGA = pga;
				maxPGAStation = feature.id.toString();
			}
			
			List<FeatureProperties> channels = props.getPropertiesList("channels", null);
			if (channels != null) {
				for (FeatureProperties channel : channels) {
					List<FeatureProperties> amplitudes = channel.getPropertiesList("amplitudes", channels);
					if (amplitudes != null) {
						for (FeatureProperties amp : amplitudes) {
							String ampName = amp.get("name", null);
							if (ampName.equals("sa(3.0)")) {
								// 3s SA
								String units = amp.get("units", null);
								if (units.equals("%g")) {
									double sa3 = amp.getDouble("value", Double.NaN)/100d;
									if (sa3 > maxSA3) {
										maxSA3 = sa3;
										maxSA3Station = feature.id.toString();
									}
								}
							} else if (ampName.equals("sa(0.3)")) {
								// 3s SA
								String units = amp.get("units", null);
								if (units.equals("%g")) {
									double sa0p3 = amp.getDouble("value", Double.NaN)/100d;
									if (sa0p3 > maxSA0p3) {
										maxSA0p3 = sa0p3;
										maxSA0p3Station = feature.id.toString();
									}
								}
							}
						}
					}
				}
			}
		}

		System.out.println("Max PGA: "+(float)maxPGA+" at "+maxPGAStation);
		System.out.println("Max 0.3s SA: "+(float)maxSA0p3+" at "+maxSA0p3Station);
		System.out.println("Max 3s SA: "+(float)maxSA3+" at "+maxSA3Station);
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		
		CybershakeIM ims[] = {
			CybershakeIM.getSA(CyberShakeComponent.RotD100, 0.01),
			CybershakeIM.getSA(CyberShakeComponent.RotD100, 3d)
		};
		
		List<CybershakeRun> runs = study.runFetcher().forSiteNames("SBSM", "LADT", "LAPD", "PAS", "CCP").fetch();
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Thompson2020, runs);
		Preconditions.checkState(sites.size() == runs.size());
		
		PeakAmplitudesFromDB amps2db = new PeakAmplitudesFromDB(study.getDB());
		
		// UCERF2 sources
		
		List<int[]> scenarios = new ArrayList<>();
		
		boolean highStressDrop = false;
		scenarios.add(new int[] {57, 2}); // SAF
		scenarios.add(new int[] {111, 3}); // SJC
		scenarios.add(new int[] {242, 29}); // Puente
		scenarios.add(new int[] {273, 69}); // Sierra Madre Connected
		scenarios.add(new int[] {218, 267}); // Newport Inglewood
		
//		boolean highStressDrop = true;
//		scenarios.add(new int[] {57, 5}); // SAF
//		scenarios.add(new int[] {111, 5}); // SJC
		
		AbstractERF erf = study.getERF();
		
		ScalarIMR gmm = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.get();
		
		CSVFile<String> csv = new CSVFile<>(false);
		
		ComponentTranslation compTrans = ComponentConverter.getConverter(Component.RotD50, Component.RotD100);
		
		for (int[] scenario : scenarios) {
			ProbEqkSource source = erf.getSource(scenario[0]);
			ProbEqkRupture rup = source.getRupture(scenario[1]);
			
			gmm.setEqkRupture(rup);
			
			System.out.println(source.getName()+" M"+(float)rup.getMag());
			
			if (csv.getNumRows() > 0)
				csv.addLine(List.of(""));
			csv.addLine("Scenario");
			csv.addLine(source.getName());
			csv.addLine("UCERF2 Magnitude", "UCERF2 Source ID", "UCERF2 Rupture ID");
			csv.addLine((float)rup.getMag()+"", scenario[0]+"", scenario[1]+"");
			for (int r=0; r<runs.size(); r++) {
				CybershakeRun run = runs.get(r);
				Site site = sites.get(r);
				gmm.setSite(site);
				csv.addLine("Site Name", "Site Latitude", "Site Longitude");
				csv.addLine(site.getName(), (float)site.getLocation().getLatitude()+"",
						(float)site.getLocation().getLongitude()+"");
				csv.addLine("Period", "CyberShake mean (g)", "min (g)", "max (g)", "-1 sigma(g)", "+1 sigma(g)",
						"Empirical mean (g)", "-1 sigma(g)", "+1 sigma(g)");
				boolean first = r == 0;
				for (CybershakeIM im : ims) {
					List<Double> amps = amps2db.getIM_Values(run.getRunID(), scenario[0], scenario[1], im);
					if (first) {
						System.out.println("Rupture has "+amps.size()+" variations");
						first = false;
					}
					double[] lnAmps = new double[amps.size()];
					for (int i=0; i<amps.size(); i++)
						lnAmps[i] = Math.log(amps.get(i)/HazardCurveComputation.CONVERSION_TO_G);
					double lnMean = StatUtils.mean(lnAmps);
					double lnStdDev = Math.sqrt(StatUtils.variance(lnAmps));
					double mean = Math.exp(lnMean);
					double lower = Math.exp(lnMean - lnStdDev);
					double upper = Math.exp(lnMean + lnStdDev);
					double max = Math.exp(StatUtils.max(lnAmps));
					double min = Math.exp(StatUtils.min(lnAmps));
					
					double period = im.getVal();
					gmm.setIntensityMeasure(SA_Param.NAME);
					SA_Param.setPeriodInSA_Param(gmm.getIntensityMeasure(), im.getVal());
					
					double linearScale = compTrans.getScalingFactor(period);
					
					double gmmLnMean = gmm.getMean();
					double gmmLnStdDev = gmm.getStdDev();
					double gmmMean = Math.exp(gmmLnMean)*linearScale;
					double gmmLower = Math.exp(gmmLnMean-gmmLnStdDev)*linearScale;
					double gmmUpper = Math.exp(gmmLnMean+gmmLnStdDev)*linearScale;
					
					csv.addLine((float)period+"", (float)mean+"", (float)min+"", (float)max+"", (float)lower+"",
							(float)upper+"", (float)gmmMean+"", (float)gmmLower+"", (float)gmmUpper+"");
				}
			}
		}
		
		if (highStressDrop)
			csv.writeToFile(new File(outputDir, "cs_scenario_amplitudes_high.csv"));
		else
			csv.writeToFile(new File(outputDir, "cs_scenario_amplitudes.csv"));
		
		study.getDB().destroy();
	}

}
