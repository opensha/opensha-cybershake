package org.opensha.sha.cybershake.maps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.StatUtils;
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
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.ModProbConfig;
import org.opensha.sha.cybershake.bombay.ModProbConfigFactory;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

public class MultiStudyHazardMapPageGen {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		HardCodedInterpDiffMapCreator.LOCAL_MAPGEN = true;
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");
		
		CyberShakeStudy[] studies = {
				CyberShakeStudy.STUDY_15_4,
				CyberShakeStudy.STUDY_17_3_3D,
				CyberShakeStudy.STUDY_18_8
		};
		
		double[] periods = { 2d, 3d, 5d, 10d };
		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		
		CyberShakeStudy lastStudy = null;
		for (CyberShakeStudy study : studies) {
			if (lastStudy == null) {
				lastStudy = study;
			} else {
				long prevTime = lastStudy.getDate().getTimeInMillis();
				long curTime = study.getDate().getTimeInMillis();
				if (curTime > prevTime)
					lastStudy = study;
			}
		}
		
		File baseMapsDir = new File("/home/kevin/CyberShake/baseMaps/");
		Map<Double, File> baseMapFiles = new HashMap<>();
		baseMapFiles.put(2d, new File(baseMapsDir,
				"2017_04_12-statewide-nobasin-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin"));
		baseMapFiles.put(3d, new File(baseMapsDir,
				"2017_04_12-statewide-nobasin-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin"));
		baseMapFiles.put(5d, new File(baseMapsDir,
				"2017_04_12-statewide-nobasin-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin"));
		baseMapFiles.put(10d, new File(baseMapsDir,
				"2017_04_12-statewide-nobasin-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin"));
		
		File studyDir = new File(mainOutputDir, lastStudy.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		File outputDir = new File(studyDir, "multi_study_hazard_maps");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		int saveDPI = 300;
		boolean plotIntersections = true;
		
		int velModelIDforGMPE = -1;
		DBAccess gmpeDB = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
//		ScalarIMR baseMapIMR = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		ScalarIMR baseMapIMR = null;
		double basemapSpacing = 0.005;
//		double basemapSpacing = 0.01;
		GriddedRegion basemapReg = new CaliforniaRegions.RELM_TESTING_GRIDDED(basemapSpacing);
		
		// the point on the hazard curve we are plotting
		boolean isProbAt_IML = false;
		double val = 0.0004;
		String durationLabel = "2% in 50 yrs";
		
		// GMPE params
		if (baseMapIMR != null) {
			baseMapIMR.setParamDefaults();
			HardCodedInterpDiffMapCreator.setTruncation(baseMapIMR, 3.0);
		}
		
		Region combinedRegion = null;
		
		System.out.println("Building combined region");
		for (CyberShakeStudy study : studies) {
			Region reg = study.getRegion();
			if (combinedRegion == null) {
				combinedRegion = reg;
			} else {
				Region newCombinedRegion = Region.union(combinedRegion, reg);
				if (newCombinedRegion == null)
					combinedRegion = new Region(
							new Location(Math.max(combinedRegion.getMaxLat(), reg.getMaxLat()),
									Math.max(combinedRegion.getMaxLon(), reg.getMaxLon())),
							new Location(Math.min(combinedRegion.getMinLat(), reg.getMinLat()),
									Math.min(combinedRegion.getMinLon(), reg.getMinLon())));
				else
					combinedRegion = newCombinedRegion;
			}
		}
		
		System.out.println("Fetching runs");
		List<List<CybershakeRun>> studyRuns = new ArrayList<>();
		for (CyberShakeStudy study : studies)
			studyRuns.add(study.runFetcher().fetch());
		
		for (double period : periods) {
			for (CyberShakeComponent component : components) {
				CybershakeIM im = CybershakeIM.getSA(component, period);
				int imTypeID = im.getID();
				String imtLabel = (int)im.getVal()+"sec "+component.getShortName()+" SA";
				System.out.println("Doing "+imtLabel);
				Double customMax = 1d;
				if (period >= 5)
					customMax = 0.6;
				if (period >= 10)
					customMax = 0.4;
				
				String imtPrefix = imtLabel.replaceAll(" ", "_");
				
				ArbDiscrGeoDataSet[] scatters = new ArbDiscrGeoDataSet[studies.length];
				HashSet<Location> allLocs = new HashSet<Location>();
				for (int i=0; i<studies.length; i++) {
					System.out.println("Fetching curves for "+studies[i].getName());
					HazardCurveFetcher fetch = new HazardCurveFetcher(
							studies[i].getDB(), studyRuns.get(i), studies[i].getDatasetIDs(), imTypeID);
					scatters[i] = HardCodedInterpDiffMapCreator.getMainScatter(
							isProbAt_IML, val, fetch, imTypeID, null);
					allLocs.addAll(scatters[i].getLocationList());
				}
				
				for (int i=studies.length; --i>0;) {
					CyberShakeStudy study1 = studies[i];
					ArbDiscrGeoDataSet scatter1 = scatters[i];
					String name1 = study1.getName();
					
					for (int j=i; --j>=0;) {
						CyberShakeStudy study2 = studies[j];
						ArbDiscrGeoDataSet scatter2 = scatters[j];
						String name2 = study2.getName();
						
						Region intersection = Region.intersect(study1.getRegion(), study2.getRegion());
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

							System.out.println("Plotting ratio");
							String prefix = study1.getDirName()+"_"+study2.getDirName()+"_"+imtPrefix;
							plotIntersectionRatio(scatter1, scatter2, intersection, resourcesDir, name1, name2, imtLabel, prefix);
						}
					}
				}
				
				ArbDiscrGeoDataSet combScatter = new ArbDiscrGeoDataSet(scatters[0].isLatitudeX());
				int overlaps = 0;
				for (Location loc : allLocs) {
					List<Double> vals = new ArrayList<>();
					for (ArbDiscrGeoDataSet scatter : scatters)
						if (scatter.contains(loc))
							vals.add(scatter.get(loc));
					Preconditions.checkState(!vals.isEmpty());
					
					double mapVal;
					if (vals.size() == 1) {
						mapVal = vals.get(0);
					} else {
						mapVal = StatUtils.mean(Doubles.toArray(vals));
						overlaps++;
					}
					combScatter.set(loc, mapVal);
				}
				System.out.println("Averaged at "+overlaps+" overlap sites");

				System.out.println("Getting GMPE curves");
				HardCodedInterpDiffMapCreator.cs_db = gmpeDB;
				GeoDataSet basemap = null;
				File baseMapFile = baseMapFiles.get(period);
				if (baseMapFile != null) {
					System.out.println("Loading basemap from "+baseMapFile.getAbsolutePath());
					BinaryHazardCurveReader reader = new BinaryHazardCurveReader(baseMapFile.getAbsolutePath());
					Map<Location, ArbitrarilyDiscretizedFunc> curves = reader.getCurveMap();
					basemap = new GriddedGeoDataSet(basemapReg, scatters[0].isLatitudeX());
					for (Location loc : curves.keySet())
						basemap.set(loc, HazardDataSetLoader.getCurveVal(curves.get(loc), isProbAt_IML, val));
				} else if (baseMapIMR != null) {
					basemap = HardCodedInterpDiffMapCreator.loadBaseMap(
							baseMapIMR, isProbAt_IML, val, velModelIDforGMPE, imTypeID, basemapReg);
					GriddedGeoDataSet gridData = new GriddedGeoDataSet(basemapReg, scatters[0].isLatitudeX());
					for (int i=0; i<basemap.size(); i++)
						gridData.set(basemap.getLocation(i), basemap.get(i));
					basemap = gridData;
					
					for (int i=0; i<basemap.size(); i++) {
						Location loc = basemap.getLocation(i);
						// mask outside of region
						if (!combinedRegion.contains(loc))
							basemap.set(i, Double.NaN);
					}
				}
				
				plotCombinedMap(combinedRegion, basemapSpacing, combScatter, basemap, resourcesDir, durationLabel, imtLabel, imtPrefix, customMax, saveDPI);
			}
		}
		
		for (CyberShakeStudy study : studies)
			study.getDB().destroy();
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
			File outputDir, String durationLabel, String imtLabel, String imtPrefix, Double customMax, int saveDPI)
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
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated_marks."+saveDPI+".png",
						new File(outputDir, prefix+"_marks.png"));
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated."+saveDPI+".png",
					new File(outputDir, prefix+".png"));
			HardCodedInterpDiffMapCreator.fetchPlot(addr, "interpolated.ps",
					new File(outputDir, prefix+".ps"));
			if (basemap != null)
				HardCodedInterpDiffMapCreator.fetchPlot(addr, "basemap."+saveDPI+".png",
						new File(outputDir, "basemap_"+imtPrefix+".png"));
			if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN)
				FileUtils.deleteRecursive(new File(addr));
		}
	}

}
