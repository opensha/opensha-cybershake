package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.siteData.impl.ThompsonVs30_2020;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.param.Parameter;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.faultSurface.CompoundSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

import com.google.common.base.Preconditions;

import scratch.kevin.bbp.BBP_SourceFile;

public class MarisolGMMCalc {

	public static void main(String[] args) throws IOException {
		File dir = new File("/tmp/rupture_geometries");
		
		List<File> inputCSVs = new ArrayList<>();
		List<File[]> inputGeoms = new ArrayList<>();
		
		inputCSVs.add(new File(dir, "df_InputDataPred_EQreal_Hector_Mine_10s_plus_CS_sites_Scott_REVIEW.csv"));
		inputGeoms.add(new File[] {new File(dir, "hectormine_v19_05_1.src")});
		
		inputCSVs.add(new File(dir, "df_InputDataPred_EQreal_Landers_10s_plus_CS_sites_Scott_REVIEW.csv"));
		inputGeoms.add(new File[] {
				new File(dir, "landers_v16_08_1_seg1.src"),
				new File(dir, "landers_v16_08_1_seg2.src"),
				new File(dir, "landers_v16_08_1_seg3.src")
				});
		
		inputCSVs.add(new File(dir, "df_InputDataPred_EQreal_North_Palm_Springs_10s_plus_CS_sites_Scott_REVIEW.csv"));
		inputGeoms.add(new File[] {new File(dir, "northps_v13_05_1.src")});
		
		inputCSVs.add(new File(dir, "df_InputDataPred_EQreal_Northridge_10s_plus_CS_sites_Scott_REVIEW.csv"));
		inputGeoms.add(new File[] {new File(dir, "northridge_v14_02_1.src")});
		
		inputCSVs.add(new File(dir, "df_InputDataPred_EQreal_Whittier_10s_plus_CS_sites_Scott_REVIEW.csv"));
		inputGeoms.add(new File[] {new File(dir, "whittier_v12_11_0_fs.src")});
		
		AttenRelRef gmmRef = AttenRelRef.ASK_2014;
		ScalarIMR gmm = gmmRef.get();
		gmm.setParamDefaults();
		
		gmm.setIntensityMeasure(SA_Param.NAME);
		
		double[] periods = { 2, 3, 5, 10 };
		
		ThompsonVs30_2020 vs30Model = new ThompsonVs30_2020();
		
		DecimalFormat oDF = new DecimalFormat("0.#");
		
		double defaultVS30 = 180d; // used for NaNs, which happen in water
		
		for (int i=0; i<inputCSVs.size(); i++) {
			CSVFile<String> inputCSV = CSVFile.readFile(inputCSVs.get(i), true);
			
			List<Site> sites = new ArrayList<>();
			LocationList siteLocs = new LocationList();
			for (int row=1; row<inputCSV.getNumRows(); row++) {
				Location loc = new Location(inputCSV.getDouble(row, 1), inputCSV.getDouble(row, 2));
				Site site = new Site(loc);
				for (Parameter<?> param : gmm.getSiteParams())
					site.addParameter((Parameter<?>) param.clone());
				sites.add(site);
				siteLocs.add(loc);
			}
			
			List<Double> vs30s = vs30Model.getValues(siteLocs);
			Preconditions.checkState(vs30s.size() == sites.size());
			
			// build rupture
			File[] geomFiles = inputGeoms.get(i);
			List<RuptureSurface> surfs = new ArrayList<>();
			BBP_SourceFile src = null;
			for (File geomFile : geomFiles) {
				BBP_SourceFile mySrc = BBP_SourceFile.readFile(geomFile);
				if (src != null) {
					// additional surface
					Preconditions.checkState(mySrc.getMag() == src.getMag());
				} else {
					src = mySrc;
				}
				surfs.add(mySrc.getSurface().getQuadSurface());
			}
			RuptureSurface surf;
			if (surfs.size() == 1)
				surf = surfs.get(0);
			else
				surf = new CompoundSurface(surfs);
			EqkRupture rup = new EqkRupture(src.getMag(), src.getFocalMechanism().getRake(), surf, src.getHypoLoc());
			gmm.setEqkRupture(rup);
			
			CSVFile<String> outputCSV = new CSVFile<>(true);
			
			List<String> header = new ArrayList<>();
			header.add("Site Index");
			header.add("Site Latitude");
			header.add("Site Longitude");
			header.add("Site Vs30");
			for (double period : periods) {
				header.add(gmm.getShortName()+" "+oDF.format(period)+"s Ln(Mean)");
				header.add("Std. Dev.");
			}
			
			outputCSV.addLine(header);
			
			for (int s=0; s<sites.size(); s++) {
				Site site = sites.get(s);
				// set vs30
				double vs30 = vs30s.get(s);
//				Preconditions.checkState(vs30Model.isValueValid(vs30), "Vs30 invalid for %s: %s", site.getLocation(), vs30);
				if (!vs30Model.isValueValid(vs30)) {
					System.err.println("WARNING: using default vs30="+(float)defaultVS30+" for site at "+site.getLocation()+", was "+vs30);
					vs30 = defaultVS30;
				}
				site.getParameter(Double.class, Vs30_Param.NAME).setValue(vs30);
				
				gmm.setSite(site);
				
				List<String> line = new ArrayList<>(header.size());
				line.add(s+"");
				line.add((float)site.getLocation().getLatitude()+"");
				line.add((float)site.getLocation().getLongitude()+"");
				line.add(site.getParameter(Double.class, Vs30_Param.NAME).getValue().floatValue()+"");
				
				for (int p=0; p<periods.length; p++) {
					SA_Param.setPeriodInSA_Param(gmm.getIntensityMeasure(), periods[p]);
					line.add(gmm.getMean()+"");
					line.add(gmm.getStdDev()+"");
				}
				
				outputCSV.addLine(line);
			}
			
			File outputFile = new File(dir, inputCSVs.get(i).getName().replace(".csv", "_"+gmmRef.name()+".csv"));
			outputCSV.writeToFile(outputFile);
		}
	}

}
