package scratch.kevin.cybershake;

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
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;
import org.opensha.sha.cybershake.maps.GMT_InterpolationSettings;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.cybershake.maps.InterpDiffMap;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class ZoomedCSMapGen {
	
	private static final int retrieve_dpi = 300; // 72, 150, or 300

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		HardCodedInterpDiffMapCreator.LOCAL_MAPGEN = true;
		
		File outputDir = new File("/home/kevin/CyberShake/maps/17_3_zoomed_santa_barbara");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		Region plotRegion = new Region(new Location(34.25, -119.25), new Location(34.75, -120.25));
		
		int studyID = 81;
		HardCodedInterpDiffMapCreator.cs_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		HardCodedInterpDiffMapCreator.gmpe_db = HardCodedInterpDiffMapCreator.cs_db;
		Region csRegion = new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION();
		
//		int velModelIDforGMPE = 10;
		int velModelIDforGMPE = -1;
		DBAccess gmpeDB = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		int imTypeID = 167; // 2 sec SA, RotD50
		String imtLabel = "2sec SA";
		File baseMapFile = new File("/home/kevin/CyberShake/baseMaps/2018_04_02-cca06-nga2d-santa-barbara-zoom-2s/"
				+ "NGAWest_2014_NoIdr/curves/imrs1.bin");
		double basemapSpacing = 0.001;
		GriddedRegion basemapReg = new GriddedRegion(plotRegion, basemapSpacing, null);
		Double customMax = 1.0;
		
//		int imTypeID = 162; // 3 sec SA, RotD50
//		String imtLabel = "3sec SA";
//		File baseMapFile = new File("/home/kevin/CyberShake/baseMaps/2018_04_02-cca06-nga2d-santa-barbara-zoom-3s/"
//				+ "NGAWest_2014_NoIdr/curves/imrs1.bin");
//		double basemapSpacing = 0.001;
//		GriddedRegion basemapReg = new GriddedRegion(plotRegion, basemapSpacing, null);
//		Double customMax = 1.0;
		
//		int imTypeID = 158; // 5 sec SA, RotD50
//		String imtLabel = "5sec SA";
//		double basemapSpacing = 0.005;
//		Double customMax = 0.6;
		
//		int imTypeID = 152; // 10 sec SA, RotD50
//		String imtLabel = "10sec SA";
//		Double customMax = 0.4;
		
		String imtPrefix = imtLabel.replaceAll(" ", "_");
		
		// the point on the hazard curve we are plotting
		boolean isProbAt_IML = false;
		double val = 0.0004;
		String durationLabel = "2% in 50 yrs";
		
//		File baseMapFile = null;
		ScalarIMR baseMapIMR = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
//		ScalarIMR baseMapIMR = null;
//		double basemapSpacing = 0.005;
//		double basemapSpacing = 0.01;
//		GriddedRegion basemapReg = new CaliforniaRegions.RELM_TESTING_GRIDDED(basemapSpacing);
		// GMPE params
		if (baseMapIMR != null) {
			baseMapIMR.setParamDefaults();
			HardCodedInterpDiffMapCreator.setTruncation(baseMapIMR, 3.0);
		}
		
		// get CyberShake data
		System.out.println("Getting CyberShake curves");
		ArbDiscrGeoDataSet scatter = HardCodedInterpDiffMapCreator.getMainScatter(
				isProbAt_IML, val, Lists.newArrayList(studyID), imTypeID, null);

		System.out.println("Getting GMPE curves");
		HardCodedInterpDiffMapCreator.cs_db = gmpeDB;
		GeoDataSet basemap = null;
		if (baseMapFile != null) {
			System.out.println("Loading basemap from "+baseMapFile.getAbsolutePath());
			BinaryHazardCurveReader reader = new BinaryHazardCurveReader(baseMapFile.getAbsolutePath());
			Map<Location, ArbitrarilyDiscretizedFunc> curves = reader.getCurveMap();
			basemap = new GriddedGeoDataSet(basemapReg, scatter.isLatitudeX());
			for (Location loc : curves.keySet())
				basemap.set(loc, HazardDataSetLoader.getCurveVal(curves.get(loc), isProbAt_IML, val));
		} else if (baseMapIMR != null) {
			basemap = HardCodedInterpDiffMapCreator.loadBaseMap(
					baseMapIMR, isProbAt_IML, val, velModelIDforGMPE, imTypeID, csRegion);
			GriddedGeoDataSet gridData = new GriddedGeoDataSet(basemapReg, scatter.isLatitudeX());
			for (int i=0; i<basemap.size(); i++)
				gridData.set(basemap.getLocation(i), basemap.get(i));
			basemap = gridData;
			
			for (int i=0; i<basemap.size(); i++) {
				Location loc = basemap.getLocation(i);
				// mask outside of region
				if (!csRegion.contains(loc))
					basemap.set(i, Double.NaN);
			}
		}
		
//		combRegion = region2;
//		combScatter = scatter1;
		plotCombinedMap(plotRegion, basemapSpacing, scatter, basemap, outputDir, durationLabel, imtLabel, imtPrefix, customMax);
		
		HardCodedInterpDiffMapCreator.cs_db.destroy();
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
//		map.setTopoResolution(null);
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
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated_marks."+retrieve_dpi+".png",
						new File(outputDir, prefix+"_marks.png"));
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated."+retrieve_dpi+".png",
					new File(outputDir, prefix+".png"));
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated.ps",
					new File(outputDir, prefix+".ps"));
			if (basemap != null)
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "basemap."+retrieve_dpi+".png",
						new File(outputDir, "basemap_"+imtPrefix+".png"));
			if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
				FileUtils.deleteRecursive(new File(addr));
		}
	}

}
