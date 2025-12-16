package org.opensha.sha.cybershake.maps;

import java.awt.Color;
import java.awt.geom.Point2D;
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
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.mapping.gmt.elements.PSText;
import org.opensha.commons.mapping.gmt.elements.PSXYPolygon;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.mapping.gmt.elements.PSText.Justify;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol.Symbol;
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
import org.opensha.sha.faultSurface.FaultSection;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.util.NEHRP_TestCity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

import scratch.UCERF3.enumTreeBranches.FaultModels;

public class MultiStudyHazardMapPageGen {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
//		HardCodedInterpDiffMapCreator.LOCAL_MAPGEN = true;
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");
		
		boolean includeU2Faults = false;
		Map<String, Location> cities = new HashMap<>();

//		CyberShakeStudy[] studies = {
//				CyberShakeStudy.STUDY_15_4,
//				CyberShakeStudy.STUDY_17_3_3D,
//				CyberShakeStudy.STUDY_18_8
//		};
//		CyberShakeStudy[] studies = {
//				CyberShakeStudy.STUDY_15_4,
//				CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz
//		};
//		CyberShakeStudy[] studies = {
//				CyberShakeStudy.STUDY_15_4,
////				CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz,
////				CyberShakeStudy.STUDY_22_3_RSQSIM_5413
//				CyberShakeStudy.STUDY_22_12_LF
//		};
		CyberShakeStudy[] studies = {
				CyberShakeStudy.STUDY_22_12_LF,
				CyberShakeStudy.STUDY_17_3_3D,
				CyberShakeStudy.STUDY_18_8,
				CyberShakeStudy.STUDY_24_8_LF,
		};
		
		cities.put("San Francisco", NEHRP_TestCity.SAN_FRANCISCO.location());
		cities.put("San Jose", NEHRP_TestCity.SAN_JOSE.location());
		cities.put("Los Angeles", NEHRP_TestCity.LOS_ANGELES.location());
		cities.put("Bakersfield", new Location(35.37, -119.02));
		
		boolean overlapUseLatest = true;
		boolean plotOverlap = false;
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
		
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		
		Region combinedRegion = null;
		
		System.out.println("Building combined region");
		for (CyberShakeStudy study : studies) {
			Region reg = study.getRegion();
			if (combinedRegion == null) {
				combinedRegion = reg;
			} else {
				Region newCombinedRegion = Region.union(combinedRegion, reg);
				if (newCombinedRegion == null) {
					combinedRegion = new Region(
							new Location(Math.max(combinedRegion.getMaxLat(), reg.getMaxLat()),
									Math.max(combinedRegion.getMaxLon(), reg.getMaxLon())),
							new Location(Math.min(combinedRegion.getMinLat(), reg.getMinLat()),
									Math.min(combinedRegion.getMinLon(), reg.getMinLon())));
					System.err.println("Failed to union region for "+study);
				} else {
					combinedRegion = newCombinedRegion;
				}
			}
		}
		
		System.out.println("Fetching runs");
		List<List<CybershakeRun>> studyRuns = new ArrayList<>();
		for (CyberShakeStudy study : studies) {
			System.out.println("Fetching runs for "+study.getName()+" from "+study.getDBHost());
			List<CybershakeRun> runs = study.runFetcher().fetch();
			System.out.println("\tLoaded "+runs.size()+" runs");
			Preconditions.checkState(!runs.isEmpty(), "No runs found");
			studyRuns.add(runs);
		}
		
		for (double period : periods) {
			for (CyberShakeComponent component : components) {
				CybershakeIM im = CybershakeIM.getSA(component, period);
				int imTypeID = im.getID();
				String imtLabel = (int)im.getVal()+"sec "+component.getShortName()+" SA";
				System.out.println("Doing "+imtLabel+", imTypeID="+imTypeID);
				Double customMax = 1d;
				if (period >= 5)
					customMax = 0.6;
				if (period >= 10)
					customMax = 0.4;
				
				String imtPrefix = imtLabel.replaceAll(" ", "_");
				
				ArbDiscrGeoDataSet[] scatters = new ArbDiscrGeoDataSet[studies.length];
				HashSet<Location> allLocs = new HashSet<Location>();
				for (int i=0; i<studies.length; i++) {
					List<CybershakeRun> runs = studyRuns.get(i);
					System.out.println("Fetching curves for "+studies[i].getName()+" with "+runs.size()+" runs");
					HazardCurveFetcher fetch = new HazardCurveFetcher(
							studies[i].getDB(), runs, studies[i].getDatasetIDs(), imTypeID);
					scatters[i] = HardCodedInterpDiffMapCreator.getMainScatter(
							isProbAt_IML, val, fetch, imTypeID, null);
					LocationList locs = scatters[i].getLocationList();
					System.out.println("Loaded curves for "+locs.size()+" locations");
					allLocs.addAll(locs);
				}
				
				if (plotOverlap) {
					for (int i=studies.length; --i>0;) {
						CyberShakeStudy study1 = studies[i];
						ArbDiscrGeoDataSet scatter1 = scatters[i];
						String name1 = study1.getName();
						
						for (int j=i; --j>=0;) {
							CyberShakeStudy study2 = studies[j];
							ArbDiscrGeoDataSet scatter2 = scatters[j];
							String name2 = study2.getName();
							
							System.out.println("Doing "+name1+" vs "+name2);
							
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
								plotIntersectionRatio(scatter1, scatter2, intersection, resourcesDir, name1, name2, imtLabel, prefix, interpSettings);
							}
						}
					}
				}
				
				System.out.println("Doing combined");
				
				ArbDiscrGeoDataSet combScatter = new ArbDiscrGeoDataSet(scatters[0].isLatitudeX());
				int overlaps = 0;
				for (Location loc : allLocs) {
					List<Double> vals = new ArrayList<>();
					List<Long> times = new ArrayList<>();
					for (int s=0; s<studies.length; s++) {
						ArbDiscrGeoDataSet scatter = scatters[s];
						if (scatter.contains(loc)) {
							vals.add(scatter.get(loc));
							times.add(studies[s].getDate().getTimeInMillis());
						}
					}
					Preconditions.checkState(!vals.isEmpty());
					
					double mapVal;
					if (vals.size() == 1) {
						mapVal = vals.get(0);
					} else {
						if (overlapUseLatest) {
							long lastTime = Long.MIN_VALUE;
							mapVal = Double.NaN;
							for (int i=0; i<vals.size(); i++) {
								long time = times.get(i);
								if (time > lastTime) {
									lastTime = time;
									mapVal = vals.get(i);
								}
							}
						} else {
							mapVal = StatUtils.mean(Doubles.toArray(vals));
						}
						overlaps++;
					}
					combScatter.set(loc, mapVal);
				}
				if (overlapUseLatest)
					System.out.println("Used latest at "+overlaps+" overlap sites");
				else
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
					for (int i=0; i<basemap.size(); i++)
						basemap.set(i, Double.NaN);
					int numSkipped = 0;
					for (Location loc : curves.keySet()) {
						int index = basemap.indexOf(loc);
						if (index >= 0)
							basemap.set(index, HazardDataSetLoader.getCurveVal(curves.get(loc), isProbAt_IML, val));
						else
							numSkipped++;
					}
					if (numSkipped > 0)
						System.err.println("WARNING: Skipped "+numSkipped+" basemap locs outside of gridded region");
				} else if (baseMapIMR != null) {
					basemap = HardCodedInterpDiffMapCreator.loadBaseMap(
							baseMapIMR, isProbAt_IML, val, studies[0].getERF_ID(), velModelIDforGMPE, imTypeID, basemapReg);
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
				
				plotCombinedMap(combinedRegion, basemapSpacing, combScatter, basemap, resourcesDir, durationLabel,
						imtLabel, imtPrefix, customMax, saveDPI, includeU2Faults, cities);
			}
		}
		
		for (CyberShakeStudy study : studies)
			study.getDB().destroy();
	}
	
	static void plotIntersectionRatio(GeoDataSet scatter1, GeoDataSet scatter2, Region intersection,
			File outputDir, String name1, String name2, String imtLabel, String imtPrefix, GMT_InterpolationSettings interpSettings)
					throws FileNotFoundException, ClassNotFoundException, IOException, GMT_MapException, SQLException {
		boolean logPlot = false;
		boolean tightCPTs = false;
		String label = imtLabel+" "+name1+" vs "+name2;
		String[] addrs = HardCodedInterpDiffMapCreator.getCompareMap(
				logPlot, scatter1, scatter2, label, tightCPTs, intersection, interpSettings);
		
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
			File outputDir, String durationLabel, String imtLabel, String imtPrefix, Double customMax, int saveDPI,
			boolean includeU2Faults, Map<String, Location> cities)
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
		
		if (includeU2Faults) {
			List<FaultSection> sects = FaultModels.FM2_1.getFaultSections();
			for (FaultSection sect : sects) {
				PSXYPolygon line = new PSXYPolygon(sect.getFaultTrace());
				line.setFillColor(null);
				line.setPenColor(Color.BLACK);
				line.setPenWidth(1);
				line.setLineType(PlotLineType.SOLID);
				map.addPolys(line);
			}
		}
		
		for (String name : cities.keySet()) {
			Location loc = cities.get(name);
			// good for smaller maps
//			double lonOffset = 0.02;
//			double circleSize = 0.1;
//			int fontSize = 18;
			// good for smaller maps
			double lonOffset = 0.05;
			double circleSize = 0.06;
			int fontSize = 15;
			PSText text = new PSText(new Point2D.Double(loc.lon+lonOffset, loc.lat), Color.WHITE, fontSize, name, Justify.LEFT_BOTTOM);
			map.addText(text);
			PSXYSymbol symbol = new PSXYSymbol(new Point2D.Double(loc.lon, loc.lat), Symbol.CIRCLE, circleSize);
			symbol.setFillColor(Color.WHITE);
			symbol.setPenColor(Color.BLACK);
			symbol.setPenWidth(0.4);
			map.addSymbol(symbol);
		}
		
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
