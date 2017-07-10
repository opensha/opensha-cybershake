package org.opensha.sha.cybershake.maps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.AbstractGeoDataSet;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.ModProbConfig;
import org.opensha.sha.cybershake.bombay.ModProbConfigFactory;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class MultiDatasetMapGen {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		HardCodedInterpDiffMapCreator.LOCAL_MAPGEN = true;
		
//		File outputDir = new File("/home/kevin/CyberShake/maps/combined_17_3_and_15_4");
		File outputDir = new File("/home/kevin/CyberShake/maps/combined_17_3_and_15_4_nobasemap");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		int studyID1 = 81;
		DBAccess db1 = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		Region region1 = new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION();
		String name1 = "Study 17.3";
		
		int studyID2 = 57;
		DBAccess db2 = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		Region region2 = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		String name2 = "Study 15.4";
		
		int velModelIDforGMPE = -1;
		DBAccess gmpeDB = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		boolean plotIntersection = true;
		
		Region intersection = Region.intersect(region1, region2); // null if no overlap
		
//		int imTypeID = 167; // 2 sec SA, RotD50
//		String imtLabel = "2sec SA";
//		Double customMax = 1.0;
////		File baseMapFile = new File("/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-2sec/"
////				+ "NGAWest_2014_NoIdr/curves/imrs1.bin");
//		File baseMapFile = null;
		
//		int imTypeID = 162; // 3 sec SA, RotD50
//		String imtLabel = "3sec SA";
//		Double customMax = 1.0;
////		File baseMapFile = new File("/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-3sec/"
////				+ "NGAWest_2014_NoIdr/curves/imrs1.bin");
//		File baseMapFile = null;
		
//		int imTypeID = 158; // 5 sec SA, RotD50
//		String imtLabel = "5sec SA";
//		Double customMax = 0.6;
////		File baseMapFile = new File("/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-5sec/"
////				+ "NGAWest_2014_NoIdr/curves/imrs1.bin");
//		File baseMapFile = null;
		
		int imTypeID = 152; // 10 sec SA, RotD50
		String imtLabel = "10sec SA";
		Double customMax = 0.4;
//		File baseMapFile = new File("/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-10sec/"
//				+ "NGAWest_2014_NoIdr/curves/imrs1.bin");
		File baseMapFile = null;
		
//		int imTypeID = 21; // 3 sec SA, GEOM
//		String imtLabel = "3sec SA GEOM";
//		Double customMax = 1.0;
		
		String imtPrefix = imtLabel.replaceAll(" ", "_");
		
		// the point on the hazard curve we are plotting
		boolean isProbAt_IML = false;
		double val = 0.0004;
		String durationLabel = "2% in 50 yrs";
		
//		File baseMapFile = null;
//		ScalarIMR baseMapIMR = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		ScalarIMR baseMapIMR = null;
		double basemapSpacing = 0.005;
//		double basemapSpacing = 0.01;
		GriddedRegion basemapReg = new CaliforniaRegions.RELM_TESTING_GRIDDED(basemapSpacing);
		// GMPE params
		if (baseMapIMR != null) {
			baseMapIMR.setParamDefaults();
			HardCodedInterpDiffMapCreator.setTruncation(baseMapIMR, 3.0);
		}
		
		// get CyberShake data
		System.out.println("Getting CyberShake curves for "+name1);
		HardCodedInterpDiffMapCreator.cs_db = db1;
		ArbDiscrGeoDataSet scatter1 = HardCodedInterpDiffMapCreator.getMainScatter(
				isProbAt_IML, val, Lists.newArrayList(studyID1), imTypeID, null);
		System.out.println("Getting CyberShake curves for "+name2);
		HardCodedInterpDiffMapCreator.cs_db = db2;
		ArbDiscrGeoDataSet scatter2 = HardCodedInterpDiffMapCreator.getMainScatter(
				isProbAt_IML, val, Lists.newArrayList(studyID2), imTypeID, null);
		
		if (intersection != null) {
			int num1not2 = 0;
			int num2not1 = 0;
			for (Location loc : scatter1.getLocationList())
				if (intersection.contains(loc) && !scatter2.contains(loc))
					num1not2++;
			for (Location loc : scatter2.getLocationList())
				if (intersection.contains(loc) && !scatter1.contains(loc))
					num2not1++;
			// TODO remove?
			System.out.println("Intersection region has "+num1not2+" points in "+name1+" but not in "+name2);
			System.out.println("Intersection region has "+num2not1+" points in "+name2+" but not in "+name1);
			
			if (plotIntersection) {
				System.out.println("Plotting ratio");
				plotIntersectionRatio(scatter1, scatter2, intersection, outputDir, name1, name2, imtLabel, imtPrefix);
			}
		}
		
		Region combRegion = Region.union(region1, region2);
		if (combRegion == null)
			combRegion = new Region(new Location(Math.max(region1.getMaxLat(), region2.getMaxLat()),
					Math.max(region1.getMaxLon(), region2.getMaxLon())),
					new Location(Math.min(region1.getMinLat(), region2.getMinLat()),
							Math.min(region1.getMinLon(), region2.getMinLon())));
		
		HashSet<Location> allLocs = new HashSet<Location>();
		allLocs.addAll(scatter1.getLocationList());
		allLocs.addAll(scatter2.getLocationList());
		ArbDiscrGeoDataSet combScatter = new ArbDiscrGeoDataSet(scatter1.isLatitudeX());
		int overlaps = 0;
		for (Location loc : allLocs) {
			Double val1 = null, val2 = null;
			if (scatter1.contains(loc))
				val1 = scatter1.get(loc);
			if (scatter2.contains(loc))
				val2 = scatter2.get(loc);
			double mapVal;
			if (val1 == null)
				mapVal = val2;
			else if (val2 == null)
				mapVal = val1;
			else {
				// both
				mapVal = 0.5*val1 + 0.5*val2;
				overlaps++;
			}
			combScatter.set(loc, mapVal);
		}
		System.out.println("Averaged at "+overlaps+" overlap sites");

		System.out.println("Getting GMPE curves");
		HardCodedInterpDiffMapCreator.cs_db = gmpeDB;
		GeoDataSet basemap = null;
		if (baseMapFile != null) {
			System.out.println("Loading basemap from "+baseMapFile.getAbsolutePath());
			BinaryHazardCurveReader reader = new BinaryHazardCurveReader(baseMapFile.getAbsolutePath());
			Map<Location, ArbitrarilyDiscretizedFunc> curves = reader.getCurveMap();
			basemap = new GriddedGeoDataSet(basemapReg, scatter1.isLatitudeX());
			for (Location loc : curves.keySet())
				basemap.set(loc, HazardDataSetLoader.getCurveVal(curves.get(loc), isProbAt_IML, val));
		} else if (baseMapIMR != null) {
			basemap = HardCodedInterpDiffMapCreator.loadBaseMap(
					baseMapIMR, isProbAt_IML, val, velModelIDforGMPE, imTypeID, combRegion);
			GriddedGeoDataSet gridData = new GriddedGeoDataSet(basemapReg, scatter1.isLatitudeX());
			for (int i=0; i<basemap.size(); i++)
				gridData.set(basemap.getLocation(i), basemap.get(i));
			basemap = gridData;
			
			for (int i=0; i<basemap.size(); i++) {
				Location loc = basemap.getLocation(i);
				// mask outside of region
				if (!region1.contains(loc) && !region2.contains(loc))
					basemap.set(i, Double.NaN);
			}
		}
		
//		combRegion = region2;
//		combScatter = scatter1;
		plotCombinedMap(combRegion, basemapSpacing, combScatter, basemap, outputDir, durationLabel, imtLabel, imtPrefix, customMax);
		
		db1.destroy();
		db2.destroy();
	}
	
	private static void plotIntersectionRatio(GeoDataSet scatter1, GeoDataSet scatter2, Region intersection,
			File outputDir, String name1, String name2, String imtLabel, String imtPrefix)
					throws FileNotFoundException, ClassNotFoundException, IOException, GMT_MapException, SQLException {
		boolean logPlot = false;
		boolean tightCPTs = false;
		String label = imtLabel+" "+name1+" vs "+name2;
		String[] addrs = HardCodedInterpDiffMapCreator.getCompareMap(
				logPlot, scatter1, scatter2, label, tightCPTs, intersection);
		
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
	}
	
	private static void plotCombinedMap(Region region, double spacing, GeoDataSet scatterData, GeoDataSet basemap,
			File outputDir, String durationLabel, String imtLabel, String imtPrefix, Double customMax)
					throws ClassNotFoundException, IOException, GMT_MapException {
		boolean logPlot = false;
		Double customMin = null;
		if (customMax != null)
			customMin = 0d;
		
		String label = imtLabel+", "+durationLabel;
		
		System.out.println("Creating map instance...");
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		
		InterpDiffMapType[] mapTypes = HardCodedInterpDiffMapCreator.normPlotTypes;
		if (basemap == null)
			mapTypes = new InterpDiffMapType[] { InterpDiffMapType.INTERP_NOMARKS,
					InterpDiffMapType.INTERP_MARKS };
		
		CPT cpt = CyberShake_GMT_MapGenerator.getHazardCPT();
		
		InterpDiffMap map = new InterpDiffMap(region, basemap, spacing, cpt, scatterData, interpSettings, mapTypes);
		map.setCustomLabel(label);
		map.setTopoResolution(TopographicSlopeFile.CA_THREE);
		map.setLogPlot(logPlot);
		map.setDpi(300);
		map.setXyzFileName("base_map.xyz");
		map.setCustomScaleMin(customMin);
		map.setCustomScaleMax(customMax);
		
		String metadata = label;
		
		System.out.println("Making map...");
		String addr;
		if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
			addr = HardCodedInterpDiffMapCreator.plotLocally(map);
		else
			addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
		
		System.out.println("Map address: " + addr);
		if (outputDir != null) {
			String prefix = "combined_"+imtPrefix;
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated_marks.150.png",
						new File(outputDir, prefix+"_marks.png"));
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated.150.png",
					new File(outputDir, prefix+".png"));
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated.ps",
					new File(outputDir, prefix+".ps"));
			if (basemap != null)
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "basemap.150.png",
						new File(outputDir, "basemap_"+imtPrefix+".png"));
			if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
				FileUtils.deleteRecursive(new File(addr));
		}
	}

}
