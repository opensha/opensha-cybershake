package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSetMath;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;

import com.google.common.base.Preconditions;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class BackSeisPlotter {

	public static void main(String[] args) throws Exception {
//		String modelName = "UCERF2";
//		String[] types = { "Excluding", "Including" };
//		int refIndex = 1;
//		File dataMainDir = new File("/home/kevin/CyberShake/baseMaps/2018_01_18-statewide-nobasin-backSeisTest");
//		File[] curveFiles = { new File(dataMainDir, "ngaw2_exclude/curves/imrs1.bin"),
//				new File(dataMainDir, "ngaw2_include/curves/imrs1.bin")};
//		double spacing = 0.05;
//		String imt = "2s SA";
		
		String modelName = "UCERF3";
		String[] types = { "Supra Only", "Sub+Supra", "Sub+Supra+Off" };
		int refIndex = 2;
		File dataMainDir = new File("/home/kevin/CyberShake/baseMaps/2018_01_18-statewide-nobasin-u3-backSeisTest");
		File[] curveFiles = { new File(dataMainDir, "ngaw2_supra_only/curves/imrs1.bin"),
				new File(dataMainDir, "ngaw2_sub_and_supra/curves/imrs1.bin"), new File(dataMainDir, "ngaw2_including/curves/imrs1.bin")};
		double spacing = 0.05;
		String imt = "2s SA";
		
		double[] probLevels = { 0.002, 0.0004, 0.0002 };
		String[] probLabels = { "10% in 50yrs", "2% in 50yrs", "1% in 50yrs" };
		String[] probFileLabels = { "10p_in_50", "2p_in_50", "1p_in_50" };
		
		Double customMin = 0d;
		Double customMax = 1d;
		
		Region region = new CaliforniaRegions.RELM_TESTING();
		File outputDir = new File("/home/kevin/CyberShake/gridded_importance/"+modelName);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		GeoDataSet[][] maps = new GeoDataSet[types.length][probLevels.length];
		
		CPT hazardCPT = CyberShake_GMT_MapGenerator.getHazardCPT();
		CPT ratioCPT = CyberShake_GMT_MapGenerator.getRatioCPT();
		
		for (int i=0; i<types.length; i++) {
			System.out.println("Loading "+curveFiles[i].getAbsolutePath());
			BinaryHazardCurveReader reader = new BinaryHazardCurveReader(curveFiles[i].getAbsolutePath());
			Map<Location, ArbitrarilyDiscretizedFunc> curves = reader.getCurveMap();
			
			for (int j=0; j<probLevels.length; j++) {
				maps[i][j] = HazardDataSetLoader.extractPointFromCurves(curves, false, probLevels[j]);
				
				System.out.println("Plotting "+probLabels[j]);
				
				String prefix = toFileLabel(modelName+"_"+types[i])+"_"+probFileLabels[j];
				String label = modelName+", "+types[i]+". "+probLabels[j]+", "+imt;
				plot(outputDir, prefix, maps[i][j], region, spacing, customMin, customMax, label, hazardCPT, true);
			}
		}
		
		for (int i=0; i<types.length; i++) {
			if (i == refIndex)
				continue;
			System.out.println("Plotting ratios of "+types[i]+" to "+types[refIndex]);
			for (int j=0; j<probLevels.length; j++) {
				GeoDataSet map1 = maps[i][j];
				GeoDataSet map2 = maps[refIndex][j];
				
				GeoDataSet ratio = GeoDataSetMath.divide(map1, map2);
				
				System.out.println("Plotting "+probLabels[j]);
				
				String prefix = "ratio_"+toFileLabel(modelName+"_"+types[i]+"_vs_"+types[refIndex])+"_"+probFileLabels[j];
				String label = modelName+", Ratio "+types[i]+"/"+types[refIndex]+". "+probLabels[j]+", "+imt;
				plot(outputDir, prefix, ratio, region, spacing, null, null, label, ratioCPT, false);
			}
		}
	}
	
	private static String toFileLabel(String str) {
		return str.replaceAll(" ", "_").toLowerCase();
	}
	
	private static void plot(File outputDir, String prefix, GeoDataSet baseMap, Region region, double spacing,
			Double customMin, Double customMax, String label, CPT cpt, boolean rescaleCPT)
					throws IOException, ClassNotFoundException, GMT_MapException {
		GMT_Map map = new GMT_Map(region, baseMap, spacing, cpt);
		map.setCustomLabel(label);
		map.setTopoResolution(null);
		map.setTopoResolution(null);
		map.setUseGMTSmoothing(false);
		map.setLogPlot(false);
		map.setDpi(300);
		map.setXyzFileName("base_map.xyz");
		map.setCustomScaleMin(customMin);
		map.setCustomScaleMax(customMax);
		map.setBlackBackground(false);
		map.setRescaleCPT(rescaleCPT);
		map.setJPGFileName(null);
		map.setPDFFileName(null);
		
		if (!rescaleCPT) {
			map.setCPTEqualSpacing(false);
			map.setCPTCustomInterval(0.1);
		}
		
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
	}

}
