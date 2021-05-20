package scratch.kevin.cybershake.ugms;

import java.awt.Color;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.maps.GMT_InterpolationSettings;

import com.google.common.base.Preconditions;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class ScatterDiffPlotter {

	public static void main(String[] args) throws FileNotFoundException, IOException, GMT_MapException {
		String[] periodStrs = { "2s", "3s", "4s", "5s", "7.5s", "10s" };
		File mainOutputDir = new File("/tmp/mcer_scatter_compare");
		Preconditions.checkState(mainOutputDir.exists() || mainOutputDir.mkdir());
		
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		
		String[] newPrefixes = { "mcer",		 	"det_lower",		"det",		"prob" };
		String[] oldPrefixes = { "combined_mcer",	"det_lower_limit",	"det_mcer",	"prob_mcer" };
		
		File baseDir = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100");
		for (String periodStr : periodStrs) {
			for (int i=0; i<newPrefixes.length; i++) {
				File outputDir = new File(mainOutputDir, newPrefixes[i]);
				Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
				System.out.println(periodStr+" "+newPrefixes[i]);
				File origFile = new File(baseDir, periodStr+"/sa/"+oldPrefixes[i]+"_sa_map_data_scatter.txt");
				File newFile = new File(baseDir, "scatter/"+newPrefixes[i]+"_"+periodStr+".xyz");
				
				GeoDataSet origScatter = ArbDiscrGeoDataSet.loadXYZFile(origFile.getAbsolutePath(), true);
				GeoDataSet newScatter = ArbDiscrGeoDataSet.loadXYZFile(newFile.getAbsolutePath(), true);
				
				GeoDataSet ratioScatter = new ArbDiscrGeoDataSet(false);
				GeoDataSet diffScatter = new ArbDiscrGeoDataSet(false);
				
				MinMaxAveTracker ratioTrack = new MinMaxAveTracker();
				MinMaxAveTracker diffTrack = new MinMaxAveTracker();
				
				for (Location loc : origScatter.getLocationList()) {
					double origVal = origScatter.get(loc);
					double newVal = newScatter.get(loc);
					
					double ratio = newVal / origVal;
					double diff = newVal - origVal;
					
					ratioTrack.addValue(ratio);
					diffTrack.addValue(diff);
					
					ratioScatter.set(loc, ratio);
					diffScatter.set(loc, diff);
				}
				
				System.out.println("\tRatios: "+ratioTrack);
				System.out.println("\tDiffs: "+diffTrack);
				
				GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
				
				CPT diffCPT = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(-0.1, 0.1);
				diffCPT.setNanColor(Color.GRAY);
				GMT_Map diffMap = new GMT_Map(region, diffScatter, 0.02, diffCPT);
				diffMap.setCustomScaleMin((double)diffCPT.getMinValue());
				diffMap.setCustomScaleMax((double)diffCPT.getMaxValue());
				diffMap.setInterpSettings(interpSettings);
				diffMap.setMaskIfNotRectangular(true);
				diffMap.setBlackBackground(false);
				diffMap.setCustomLabel(periodStr+" "+newPrefixes[i].replaceAll("_", " ").toUpperCase()+", Difference");
				FaultBasedMapGen.plotMap(outputDir, periodStr+"_diff", false, diffMap);
				
				CPT ratioCPT = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(0.8, 1.2);
				ratioCPT.setNanColor(Color.GRAY);
				GMT_Map ratioMap = new GMT_Map(region, ratioScatter, 0.02, ratioCPT);
				ratioMap.setCustomScaleMin((double)ratioCPT.getMinValue());
				ratioMap.setCustomScaleMax((double)ratioCPT.getMaxValue());
				ratioMap.setInterpSettings(interpSettings);
				ratioMap.setMaskIfNotRectangular(true);
				ratioMap.setBlackBackground(false);
				ratioMap.setCustomLabel(periodStr+" "+newPrefixes[i].replaceAll("_", " ").toUpperCase()+", Ratio");
				FaultBasedMapGen.plotMap(outputDir, periodStr+"_ratio", false, ratioMap);
			}
		}
	}

}
