package org.opensha.sha.cybershake.maps;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.CS_Study18_8_BasinDepth;
import org.opensha.commons.data.siteData.impl.CS_Study24_8_BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM4i26BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM4i26_M01_TaperBasinDepth;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.data.siteData.impl.ThompsonVs30_2020;
import org.opensha.commons.data.siteData.impl.WillsMap2006;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.data.xyz.AbstractGeoDataSet;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.ModProbConfig;
import org.opensha.sha.cybershake.bombay.ModProbConfigFactory;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.AttenRels2DB;
import org.opensha.sha.cybershake.db.CybershakeHazardDataset;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardDataset2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;

import scratch.kevin.cybershake.BatchBaseMapPlot;

public class StudyHazardMapPageGen {
	
	private static final boolean LOCAL_MAPGEN = false;

	public static void main(String[] args) throws IOException {
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");

		int vmOverride = -1;
		int erfOverride = -1;
		CyberShakeStudy compStudy = null;
		Region basemapRegion = null;
		double baseMapRes = 0.005;
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		HardCodedInterpDiffMapCreator.gmpe_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
////		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_LF;
////		double[] periods = { 2d, 3d, 5d, 10d };
////		compStudy = CyberShakeStudy.STUDY_15_4;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
//		double[] periods = { 0.1, 0.2, 0.5, 1d, 2d, 3d, 5d, 10d };
//		compStudy = CyberShakeStudy.STUDY_15_12;
//		vmOverride = 5;
//		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
////		ScalarIMR baseMapGMPE = null;
////		ScalarIMR backgroundGMPE = baseMapGMPE;
//		ScalarIMR backgroundGMPE = null;
//		SiteData<?>[] siteDatas = { new ThompsonVs30_2020(), new CVM4i26_M01_TaperBasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CVM4i26_M01_TaperBasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		Region zoomRegion = null;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_24_8_LF;
//		double[] periods = { 2d, 3d, 5d, 10d };
//		compStudy = CyberShakeStudy.STUDY_18_8;
		CyberShakeStudy study = CyberShakeStudy.STUDY_24_8_BB;
		double[] periods = { 0.1, 0.2, 0.5, 1d, 2d, 3d, 5d, 10d };
		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
//		ScalarIMR baseMapGMPE = null;
//		ScalarIMR backgroundGMPE = baseMapGMPE;
		ScalarIMR backgroundGMPE = null;
//		vmOverride = compStudy.getVelocityModelID();
//		erfOverride = compStudy.getERF_ID();
		SiteData<?>[] siteDatas = { new ThompsonVs30_2020(), new CS_Study24_8_BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
				new CS_Study24_8_BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		basemapRegion = compStudy.getRegion();
//		baseMapRes = 0.02;
		// for some reason the upsampling step is failing for this region, this disables it
		interpSettings.setInterpSpacing(baseMapRes);
		Region zoomRegion = null;
		HardCodedInterpDiffMapCreator.gmpe_db = study.getDB();
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
////		CyberShakeStudy study = CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz;
//		double[] periods = { 2d, 3d, 5d, 10d };
////		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
////		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
////		ScalarIMR baseMapGMPE = null;
////		ScalarIMR backgroundGMPE = baseMapGMPE;
//		ScalarIMR backgroundGMPE = null;
//		SiteData<?>[] siteDatas = { new ThompsonVs30_2020(), new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		Region zoomRegion = null;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
//		double[] periods = { 2d, 3d, 5d, 10d };
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
//		SiteData<?>[] siteDatas = { new WillsMap2015(), new CS_Study18_8_BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CS_Study18_8_BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		ScalarIMR backgroundGMPE = baseMapGMPE;
//		Region zoomRegion = new Region(new Location(38.5, -121.5), new Location(37, -123));
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_17_3_3D;
//		double[] periods = { 2d, 3d, 5d, 10d };
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
//		SiteData<?>[] siteDatas = { new WillsMap2015(), new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		Region zoomRegion = null;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_17_3_1D;
//		double[] periods = { 2d, 3d, 5d, 10d };
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
//		SiteData<?>[] siteDatas = null;
//		Region zoomRegion = null;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
//		double[] periods = { 2d, 3d, 5d, 10d };
////		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
////		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
////		ScalarIMR backgroundGMPE = baseMapGMPE;
//		ScalarIMR backgroundGMPE = null;
//		SiteData<?>[] siteDatas = { new WillsMap2015(), new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		Region zoomRegion = null;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_15_12;
//		double[] periods = { 0.1, 0.2, 0.5, 1d, 2d, 3d, 5d, 10d };
//		vmOverride = 13; // THIS IS TEMPORARY, NO GMPE BASEMAPS FOR <2s and vmID=5
////		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
////		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
////		ScalarIMR backgroundGMPE = baseMapGMPE;
//		ScalarIMR backgroundGMPE = null;
//		SiteData<?>[] siteDatas = { new WillsMap2015(), new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		Region zoomRegion = null;
////		DBAccess.PRINT_ALL_QUERIES = true;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_14_2_CVM_S426;
//		double[] periods = { 3d, 5d, 10d };
//		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
//		SiteData<?>[] siteDatas = { new WillsMap2006(), new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0),
//				new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5) };
//		Region zoomRegion = null;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_14_2_1D;
//		double[] periods = { 3d, 5d, 10d };
//		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
//		SiteData<?>[] siteDatas = null;
//		Region zoomRegion = null;
		
		boolean replot = true;
		
		List<Double> probLevels = new ArrayList<>();
		List<String> probLabels = new ArrayList<>();
		List<String> probFileLables = new ArrayList<>();
		
		probLevels.add(4e-4);
		probLabels.add("2% in 50 yr");
		probFileLables.add("2in50");
		
		if (backgroundGMPE != null) {
			probLevels.add(0.002);
			probLabels.add("10% in 50 yr");
			probFileLables.add("10in50");
			
			probLevels.add(1d/10000);
			probLabels.add("10000 yr");
			probFileLables.add("10000yr");
		}
		
		boolean isProbAt_IML = false;
		
		CPT hazardCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
//		CPT hazardCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
//				"/resources/cpt/MaxSpectrum2.cpt"));
		hazardCPT.setNanColor(CyberShake_GMT_MapGenerator.OUTSIDE_REGION_COLOR);
		boolean logPlot = false;
		
//		CPT ratioCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
//				"/org/opensha/sha/cybershake/conf/cpt/cptFile_ratio_tighter.cpt"));
////		CPT ratioCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
////				"/org/opensha/sha/cybershake/conf/cpt/cptFile_ratio.cpt"));
//		ratioCPT.setNanColor(CyberShake_GMT_MapGenerator.OUTSIDE_REGION_COLOR);
		
		// GMPE params
		if (baseMapGMPE != null) {
			baseMapGMPE.setParamDefaults();
			HardCodedInterpDiffMapCreator.setTruncation(baseMapGMPE, 3.0);
		}
		if (backgroundGMPE != null && backgroundGMPE != baseMapGMPE) {
			backgroundGMPE.setParamDefaults();
			HardCodedInterpDiffMapCreator.setTruncation(backgroundGMPE, 3.0);
		}
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		File mapsDir;
		if (backgroundGMPE == null)
			mapsDir = new File(studyDir, "hazard_maps");
		else
			mapsDir = new File(studyDir, "hazard_maps_back_seis");
		Preconditions.checkState(mapsDir.exists() || mapsDir.mkdir());
		
		File resourcesDir = new File(mapsDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		List<String> lines = new ArrayList<>();
		if (backgroundGMPE == null)
			lines.add("# "+study.getName()+" Hazard Maps");
		else
			lines.add("# "+study.getName()+" Hazard Maps (with "+backgroundGMPE.getShortName()+" Background Seismicity)");
		lines.add("");
		lines.add("**Study Details**");
		lines.add("");
		lines.addAll(study.getMarkdownMetadataTable());
		lines.add("");
		if (backgroundGMPE != null) {
			lines.add("This map includes background seismicity sources computed with the "+backgroundGMPE.getName()
				+" empirical GMPE.");
			lines.add("");
		}
		if (baseMapGMPE != null) {
			lines.add("**Basemap GMPE:** "+baseMapGMPE.getName());
			lines.add("");
			lines.add("These are interpolated difference maps, where the differences between CyberShake and the GMPE basemap "
					+ "are interpolated and then added to the GMPE basemap. This results in a map which matches the CyberShake "
					+ "values exactly at each CyberShake site, but retains the detail (largely due to inclusion of site effects) "
					+ "of the GMPE basemap.");
			if (backgroundGMPE != null) {
				lines.add("");
				lines.add("Note: the basemap does not include background seismicity (even though the CyberShake map does).");
			}
		} else {
			lines.add("These are diretly interpolated CyberShake maps (without a GMPE basemap)");
		}
		
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		Region region = study.getRegion();
		InterpDiffMapType[] mapTypes;
		InterpDiffMapType[][] typeTable;
		if (backgroundGMPE != null) {
			mapTypes = new InterpDiffMapType [] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS};
			typeTable = new InterpDiffMapType[1][];
			typeTable[0] = new InterpDiffMapType[] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS };
		} else if (baseMapGMPE == null) {
			mapTypes = new InterpDiffMapType [] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS};
			typeTable = new InterpDiffMapType[2][1];
			typeTable[0][0] = InterpDiffMapType.INTERP_NOMARKS;
			typeTable[1][0] = InterpDiffMapType.INTERP_MARKS;
		} else {
			mapTypes = new InterpDiffMapType [] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS,
					InterpDiffMapType.BASEMAP, InterpDiffMapType.DIFF, InterpDiffMapType.RATIO};
			typeTable = new InterpDiffMapType[3][];
			typeTable[0] = new InterpDiffMapType[] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.BASEMAP };
			typeTable[1] = new InterpDiffMapType[] { InterpDiffMapType.INTERP_MARKS, InterpDiffMapType.BASEMAP };
			typeTable[2] = new InterpDiffMapType[] { InterpDiffMapType.DIFF, InterpDiffMapType.RATIO };
		}
		
		HashSet<InterpDiffMapType> psSaveTypes = new HashSet<>();
		psSaveTypes.add(InterpDiffMapType.INTERP_NOMARKS);
		
		HardCodedInterpDiffMapCreator.cs_db = study.getDB();
//		HardCodedInterpDiffMapCreator.gmpe_db = study.getDB();
		
		int exitCode = 0;
		try {
			int[] origIDs;
			int[] dsIDs;
			List<CybershakeRun> runs;
			if (backgroundGMPE == null) {
				dsIDs = study.getDatasetIDs();
				origIDs = dsIDs;
				runs = study.runFetcher().fetch();
			} else {
				origIDs = study.getDatasetIDs();
				dsIDs = new int[origIDs.length];
				System.out.println("Finding back seis dataset IDs");
				int backSeisAttenRelID = new AttenRels2DB(study.getDB()).getAttenRelID(backgroundGMPE);
				Preconditions.checkState(backSeisAttenRelID > 0, "Back seis GMPE not found");
				HazardDataset2DB datasets2db = new HazardDataset2DB(study.getDB());
				for (int j=0; j<origIDs.length; j++) {
					CybershakeHazardDataset ds = datasets2db.getDataset(origIDs[j]);
					
					// see if we already have a bg dataset ID
					int bgDSID = datasets2db.getDatasetID(ds.erfID, ds.rvScenID, ds.sgtVarID, ds.velModelID,
							ds.probModelID, ds.timeSpanID, ds.timeSpanStart, ds.maxFreq, ds.lowFreqCutoff,
							backSeisAttenRelID);
					Preconditions.checkState(bgDSID > 0, "Back seis version of dataset "
							+origIDs[j]+" not found");
					System.out.println("\t"+origIDs[j]+" => "+bgDSID);
					dsIDs[j] = bgDSID;
				}
				runs = study.runFetcher().hasHazardCurves(dsIDs).fetch();
			}
			Preconditions.checkState(!runs.isEmpty(), "no runs found!");
			System.out.println(runs.size()+" runs found");
			
			Map<Integer, CybershakeSite> siteIDMap = new HashMap<>();
			CybershakeSiteInfo2DB sites2db = new CybershakeSiteInfo2DB(study.getDB());
			
			List<CybershakeRun> compRuns = null;
			if (compStudy != null) {
				compRuns = compStudy.runFetcher().fetch();
			}
			
			List<Region> mapRegions = new ArrayList<>();
			mapRegions.add(region);
			if (zoomRegion != null)
				mapRegions.add(zoomRegion);
			for (Region mapRegion : mapRegions) {
				String heading = "##";
				if (mapRegion != region) {
					lines.add(heading+" Zoomed Maps");
					lines.add(topLink); lines.add("");
					heading += "#";
				}
				for (double period : periods) {
					for (CyberShakeComponent component : components) {
						CybershakeIM im = CybershakeIM.getSA(component, period);
						
						String periodLabel = optionalDigitDF.format(period)+"sec SA, "+component.getShortName();
						String periodFileLabel = optionalDigitDF.format(period)+"s_"+component.getShortName();
						
						if (probLevels.size() > 1)
							lines.add(heading+" "+periodLabel);
						
						System.out.println("Doing "+periodLabel);
						
						HazardCurveFetcher fetch = null;
						HazardCurveFetcher origFetch = null;
						
						for (int i=0; i<probLevels.size(); i++) {
							double probLevel = probLevels.get(i);
							String probLabel = probLabels.get(i);
							String probFileLabel = probFileLables.get(i);
							
							String title = periodLabel+", "+probLabel;
							
							String prefix = "map_"+periodFileLabel+"_"+probFileLabel;
							if (mapRegion != region)
								prefix += "_zoomed";
							
							List<InterpDiffMapType> typesToPlot = new ArrayList<>();
							for (InterpDiffMapType type : mapTypes) {
								File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
								if (replot || !pngFile.exists())
									typesToPlot.add(type);
							}
							
							File interpDiffsFile = new File(resourcesDir, prefix+"_interpolated_differences.txt");
							File interpMapFile = new File(resourcesDir, prefix+"_interpolated.txt");
							
							GeoDataSet scatterData = null;
							boolean saveInterp = typesToPlot.contains(InterpDiffMapType.INTERP_NOMARKS);
							GeoDataSet baseMap = null;
							if (!typesToPlot.isEmpty()) {
								System.out.println("Plotting "+typesToPlot.size()+" maps");
								
								if (baseMapGMPE != null) {
									// load the basemap
									System.out.println("Loading basemap");
									int vmID = vmOverride > 0 ? vmOverride : study.getVelocityModelID();
									int erfID = erfOverride > 0 ? erfOverride : study.getERF_ID();
									Region reg = basemapRegion == null ? region : basemapRegion;
									baseMap = HardCodedInterpDiffMapCreator.loadBaseMap(
											baseMapGMPE, isProbAt_IML, probLevel, erfID,
											vmID, im.getID(), reg);
								}
								if (fetch == null) {
									fetch = new HazardCurveFetcher(study.getDB(), runs, dsIDs, im.getID());
									if (backgroundGMPE != null)
										origFetch = new HazardCurveFetcher(study.getDB(), runs, origIDs, im.getID());
								}
								scatterData = HardCodedInterpDiffMapCreator.getMainScatter(
										isProbAt_IML, probLevel, fetch, im.getID(), null);
								
								System.out.println("Creating map instance...");
								
								double cptMax;
								if (period >= 10d)
									cptMax = 0.2d;
								else if (period >= 5d)
									cptMax = 0.4d;
								else if (period >= 2d)
									cptMax = 1d;
								else
									cptMax = 2d;
								
								CPT cpt = hazardCPT.rescale(0d, cptMax);
								
								InterpDiffMap map = new InterpDiffMap(mapRegion, baseMap, baseMapRes, cpt, scatterData,
										interpSettings, typesToPlot.toArray(new InterpDiffMapType[0]));
								map.setCustomLabel(title);
								map.setTopoResolution(TopographicSlopeFile.CA_THREE);
								map.setLogPlot(logPlot);
								map.setDpi(300);
								map.setXyzFileName("base_map.xyz");
								map.setCustomScaleMin(0d);
								map.setCustomScaleMax(cptMax);
								map.getInterpSettings().setSaveInterpSurface(saveInterp);
								
								String metadata = "isProbAt_IML: " + isProbAt_IML + "\n" +
												"val: " + probLevel + "\n" +
												"imTypeID: " + im.getID() + "\n";
								
								System.out.println("Making map...");
								String addr;
								if (LOCAL_MAPGEN)
									addr = HardCodedInterpDiffMapCreator.plotLocally(map);
								else
									addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
								
								System.out.println("Done, downloading");
								
								for (InterpDiffMapType type : typesToPlot) {
									File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
									File psFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".ps");
									File interpOutFile = baseMapGMPE == null ? interpMapFile : interpDiffsFile;
									if (LOCAL_MAPGEN) {
										File inFile = new File(addr, type.getPrefix()+".150.png");
										Preconditions.checkState(inFile.exists(), "In file doesn't exist: %s", inFile.getAbsolutePath());
										Files.copy(inFile, pngFile);
										if (psSaveTypes.contains(type)) {
											inFile = new File(addr, type.getPrefix()+".ps");
											Preconditions.checkState(inFile.exists(), "In file doesn't exist: %s", inFile.getAbsolutePath());
											Files.copy(inFile, psFile);
										}
										if (saveInterp && type == InterpDiffMapType.INTERP_NOMARKS) {
											inFile = new File(addr, "map_data_interpolated.txt");
											Preconditions.checkState(inFile.exists(), "Interpolated file doesn't exist: %s", inFile.getAbsolutePath());
											Files.copy(inFile, interpOutFile);
										}
									} else {
										if (!addr.endsWith("/"))
											addr += "/";
										FileUtils.downloadURL(addr+type.getPrefix()+".150.png", pngFile);
										if (psSaveTypes.contains(type))
											FileUtils.downloadURL(addr+type.getPrefix()+".ps", psFile);
										if (saveInterp && type == InterpDiffMapType.INTERP_NOMARKS)
											FileUtils.downloadURL(addr+"map_data_interpolated.txt", interpOutFile);
									}
								}
							}
							
							String myHeading = heading;
							if (probLevels.size() > 1)
								myHeading += "#";
							
							if (mapRegion == region)
								lines.add(myHeading+" "+title);
							else
								lines.add(myHeading+" Zoomed "+title);
							lines.add(topLink); lines.add("");
							
							if (saveInterp) {
								if (baseMapGMPE == null) {
									// don't need to add to basemap
									Preconditions.checkState(interpMapFile.exists());
								} else {
									// need to add to basemap
									Preconditions.checkState(interpDiffsFile.exists());
									System.out.println("Loading interpolated difference data from "+interpDiffsFile.getAbsolutePath());
									GriddedGeoDataSet interpDiff = GriddedGeoDataSet.loadXYZFile(interpDiffsFile, false);
									System.out.println("Loaded "+interpDiff.size()+" interpolated points");
									Preconditions.checkNotNull(baseMap);
									GeoDataSet interpXYZ = baseMap.copy();
									System.out.println("Interpolating differences on top of base map with "+interpXYZ.size()+" points");
									for (int j=0; j<interpXYZ.size(); j++) {
										Location loc = interpXYZ.getLocation(j);
										double diff = interpDiff.bilinearInterpolation(loc);
										if (!Double.isFinite(diff))
											diff = interpDiff.get(loc);
										interpXYZ.set(j, Math.max(0d, interpXYZ.get(j)+diff));
									}
									System.out.println("Writing final interpolated differences map to "+interpMapFile.getAbsolutePath());
									ArbDiscrGeoDataSet.writeXYZFile(interpXYZ, interpMapFile);
								}
								lines.add("[Download Interpolated Map](resources/"+interpMapFile.getName()+")");
								lines.add("");
							}
							
							TableBuilder table = MarkdownUtils.tableBuilder();
							for (InterpDiffMapType[] row : typeTable) {
								table.initNewLine();
								for (InterpDiffMapType type : row)
									table.addColumn("<p align=\"center\">**"+type.getName()+"**</p>");
								table.finalizeLine();
								table.initNewLine();
								for (InterpDiffMapType type : row) {
									File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
									Preconditions.checkState(pngFile.exists(), "Map doesn't exist: %s", pngFile.getAbsolutePath());
									table.addColumn("!["+type.getName()+"](resources/"+pngFile.getName()+")");
								}
								table.finalizeLine();
							}
							if (backgroundGMPE != null) {
								// now ratio/diff to without background
								
								String diffPrefix = prefix+"_bg_diff";
								String ratioPrefix = prefix+"_bg_ratio";
								
								File diffPNG = new File(resourcesDir, diffPrefix+".png");
								File diffPS = new File(resourcesDir, diffPrefix+".ps");
								File ratioPNG = new File(resourcesDir, ratioPrefix+".png");
								File ratioPS = new File(resourcesDir, ratioPrefix+".ps");
								
								if (!diffPNG.exists() || !diffPS.exists() || !ratioPNG.exists() || !ratioPS.exists()) {
									if (scatterData == null) {
										// need to grab it
										if (fetch == null) {
											fetch = new HazardCurveFetcher(study.getDB(), runs, dsIDs, im.getID());
											if (backgroundGMPE != null)
												origFetch = new HazardCurveFetcher(study.getDB(), runs, origIDs, im.getID());
										}
										scatterData = HardCodedInterpDiffMapCreator.getMainScatter(
												isProbAt_IML, probLevel, fetch, im.getID(), null);
									}
									
									GeoDataSet origScatterData = HardCodedInterpDiffMapCreator.getMainScatter(
											isProbAt_IML, probLevel, origFetch, im.getID(), null);
									boolean tightCPTs = true;
									String customLabel = "Background vs Without Background";
									String[] addrs = HardCodedInterpDiffMapCreator.getCompareMap(false, scatterData,
											origScatterData, customLabel, tightCPTs, mapRegion);
									
									String diff = addrs[0];
									String ratio = addrs[1];
									
									System.out.println("Comp map address:\n\tdiff: "+diff+"\n\tratio: "+ratio);
									
									HardCodedInterpDiffMapCreator.fetchPlot(diff, "interpolated_marks.150.png",
											diffPNG);
									HardCodedInterpDiffMapCreator.fetchPlot(diff, "interpolated_marks.ps",
											diffPS);
									HardCodedInterpDiffMapCreator.fetchPlot(ratio, "interpolated_marks.150.png",
											ratioPNG);
									HardCodedInterpDiffMapCreator.fetchPlot(ratio, "interpolated_marks.ps",
											ratioPS);
									if (HardCodedInterpDiffMapCreator.LOCAL_MAPGEN) {
										FileUtils.deleteRecursive(new File(diff));
										FileUtils.deleteRecursive(new File(ratio));
									}
								}
								
								table.initNewLine();
								table.addColumn("<p align=\"center\">**Background Seismicity Difference**</p>");
								table.addColumn("<p align=\"center\">**Background Seismicity Ratio**</p>");
								table.finalizeLine();
								table.initNewLine();
								table.addColumn("![Difference](resources/"+diffPrefix+".png)");
								table.addColumn("![Ratio](resources/"+ratioPrefix+".png)");
								table.finalizeLine();
							}
//							table.addLine("**Map Type**", "**Map**");
//							for (InterpDiffMapType type : mapTypes) {
//								File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
//								Preconditions.checkState(pngFile.exists(), "Map doesn't exist: %s", pngFile.getAbsolutePath());
//								table.addLine("**"+type.getName()+"**", "!["+type.getName()+"](resources/"+pngFile.getName()+")");
//							}
							if (backgroundGMPE != null) {
								// add hazard curves
								File medianCurvePNG = new File(resourcesDir, prefix+"_median_curve.png");
								File maxCurvePNG = new File(resourcesDir, prefix+"_max_curve.png");
								
								if (!medianCurvePNG.exists() || !maxCurvePNG.exists()) {
									if (fetch == null) {
										fetch = new HazardCurveFetcher(study.getDB(), runs, dsIDs, im.getID());
										if (backgroundGMPE != null)
											origFetch = new HazardCurveFetcher(study.getDB(), runs, origIDs, im.getID());
									}
									List<Double> ratioVals = new ArrayList<>();
									List<DiscretizedFunc> origCurves = new ArrayList<>();
									List<DiscretizedFunc> bgCurves = new ArrayList<>();
									List<String> siteNames = new ArrayList<>();
									
									for (CybershakeRun run : runs) {
										DiscretizedFunc origCurve = origFetch.getCurvesForRun(run.getRunID()).get(0);
										DiscretizedFunc bgCurve = fetch.getCurvesForRun(run.getRunID()).get(0);
										
										double bgVal = HazardDataSetLoader.getCurveVal(bgCurve, isProbAt_IML, probLevel);
										double origVal = HazardDataSetLoader.getCurveVal(origCurve, isProbAt_IML, probLevel);
										double ratio = bgVal/origVal;
										CybershakeSite site = siteIDMap.get(run.getSiteID());
										if (site == null) {
											site = sites2db.getSiteFromDB(run.getSiteID());
											siteIDMap.put(run.getSiteID(), site);
										}
										if (!mapRegion.contains(site.createLocation()))
											continue;
										String siteName = site.short_name+" (Run "+run.getRunID()+")";
										
										int insertionIndex;
										
										if (ratioVals.isEmpty()) {
											insertionIndex = 0;
										} else {
											insertionIndex = Collections.binarySearch(ratioVals, ratio);
											if (insertionIndex < 0)
												insertionIndex = -(insertionIndex+1);
										}
										ratioVals.add(insertionIndex, ratio);
										origCurves.add(insertionIndex, origCurve);
										bgCurves.add(insertionIndex, bgCurve);
										siteNames.add(insertionIndex, siteName);
									}
									
									int medianIndex = (int)(0.5 + ratioVals.size()/2d);
									System.out.println(probLabel+" median ratio="+ratioVals.get(medianIndex));
									
									String curveTitle = "Median "+probLabel+" ratio="
											+optionalDigitDF.format(ratioVals.get(medianIndex))
//											+ratioVals.get(medianIndex).floatValue()
											+", "+siteNames.get(medianIndex);
									plotBGCurves(medianCurvePNG, bgCurves.get(medianIndex),
											origCurves.get(medianIndex), curveTitle, periodLabel,
											probLevel, isProbAt_IML);
									
									int maxIndex = ratioVals.size()-1;
									System.out.println(probLabel+" max ratio="+ratioVals.get(maxIndex));
									
									curveTitle = "Max "+probLabel+" ratio="
											+optionalDigitDF.format(ratioVals.get(maxIndex))
//											+ratioVals.get(maxIndex).floatValue()
											+", "+siteNames.get(maxIndex);
									plotBGCurves(maxCurvePNG, bgCurves.get(maxIndex),
											origCurves.get(maxIndex), curveTitle, periodLabel,
											probLevel, isProbAt_IML);
								}
								
								table.initNewLine();
								table.addColumn("<p align=\"center\">**Curves w/ median ratio**</p>");
								table.addColumn("<p align=\"center\">**Curves w/ max ratio**</p>");
								table.finalizeLine();
								table.initNewLine();
								table.addColumn("![Median](resources/"+medianCurvePNG.getName()+")");
								table.addColumn("![Max](resources/"+maxCurvePNG.getName()+")");
								table.finalizeLine();
							}
							lines.addAll(table.build());
							lines.add("");
							
							if (compRuns != null && !compRuns.isEmpty()) {
								lines.add(myHeading+"# "+compStudy.getName()+" Comparisons, "+title);
								lines.add(topLink); lines.add("");
								
								table = MarkdownUtils.tableBuilder();
								
								table.addLine("Difference: "+study.getName()+" - "+compStudy.getName(),
										"Ratio: "+study.getName()+" / "+compStudy.getName());
								
								HazardCurveFetcher compFetch = new HazardCurveFetcher(compStudy.getDB(), compRuns,
										compStudy.getDatasetIDs(), im.getID());
								
								ArbDiscrGeoDataSet compScatterData = HardCodedInterpDiffMapCreator.getMainScatter(
										isProbAt_IML, probLevel, compFetch, im.getID(), null);
								
								CSVFile<String> csv = new CSVFile<>(true);
								csv.addLine("Latitude", "Longitude", study.getName(), compStudy.getName(),
										"Difference", "Ratio", "% Difference");
								for (int j=0; j<scatterData.size(); j++) {
									Location loc = scatterData.getLocation(j);
									double val = scatterData.get(j);
									if (compScatterData.contains(loc)) {
										double compVal = compScatterData.get(loc);
										List<String> line = new ArrayList<>();
										line.add((float)loc.getLatitude()+"");
										line.add((float)loc.getLongitude()+"");
										line.add(val+"");
										line.add(compVal+"");
										line.add((val - compVal)+"");
										line.add((val/compVal)+"");
										line.add(100d*((val-compVal)/compVal)+"");
										csv.addLine(line);
									}
								}
								
								File csvFile = new File(resourcesDir, "comp_"+prefix+".csv");
								csv.writeToFile(csvFile);
								
								lines.add("Download CSV: ["+csvFile.getName()+"]("+resourcesDir.getName()+"/"+csvFile.getName()+")");
								lines.add("");
								
								MultiStudyHazardMapPageGen.plotIntersectionRatio(scatterData, compScatterData, region,
										resourcesDir, study.getName(), compStudy.getName(), title, "comp_"+prefix, interpSettings);
								
								table.addLine("![Difference]("+resourcesDir.getName()+"/diff_comp_"+prefix+".png)",
										"![Ratio]("+resourcesDir.getName()+"/ratio_comp_"+prefix+".png)");
								
								lines.addAll(table.build());
								lines.add("");
							}
						}
					}
				}
				
				if (siteDatas != null && siteDatas.length > 0) {
					if (mapRegion == region)
						lines.add(heading+" Site Data Maps");
					else
						lines.add(heading+" Zoomed Site Data Maps");
					for (SiteData<?> siteData : siteDatas) {
						System.out.println("Site data: "+siteData.getDataType()+": "+siteData.getName());
						String prefix = siteData.getShortName();
						if (siteData.getDataType().equals(SiteData.TYPE_VS30)) {
							prefix += "_vs30";
						} else if (siteData.getDataType().equals(SiteData.TYPE_DEPTH_TO_1_0)) {
							prefix += "_z1p0";
						} else if (siteData.getDataType().equals(SiteData.TYPE_DEPTH_TO_2_5)) {
							prefix += "_2p5";
						} else {
							throw new IllegalStateException();
						}
						if (mapRegion != region) {
							prefix += "_zoomed";
							lines.add(heading+"# Zoomed "+siteData.getDataType()+": "+siteData.getName());
						} else {
							lines.add(heading+"# "+siteData.getDataType()+": "+siteData.getName());
						}
						lines.add(topLink); lines.add("");
						File pngFile = BatchBaseMapPlot.checkMakeSiteDataPlot(
								(SiteData<Double>)siteData, mapRegion, resourcesDir, prefix, replot);
						lines.add("!["+siteData.getName()+"](resources/"+pngFile.getName()+")");
					}
				}
			}
			
			// add TOC
			lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
			lines.add(tocIndex, "## Table Of Contents");

			// write markdown
			MarkdownUtils.writeReadmeAndHTML(lines, mapsDir);
			
			System.out.println("Done, writing summary");
			study.writeMarkdownSummary(studyDir);
			System.out.println("Writing studies index");
			CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		} catch (Exception e) {
			e.printStackTrace();
			exitCode = 1;
		} finally {
			study.getDB().destroy();
			if (study.getDB() != HardCodedInterpDiffMapCreator.gmpe_db)
				HardCodedInterpDiffMapCreator.gmpe_db.destroy();
			System.exit(exitCode);
		}
	}
	
	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.##");
	
	private static void plotBGCurves(File outputFile, DiscretizedFunc bgCurve,
			DiscretizedFunc origCurve, String title, String xAxisLabel, double level, boolean isProbAtIML)
					throws IOException {
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		bgCurve.setName("Total");
		funcs.add(bgCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 4f, Color.BLACK));
		
		origCurve.setName("CS Faults Only");
		funcs.add(origCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE));
		
		DiscretizedFunc bgOnlyCurve = new ArbitrarilyDiscretizedFunc();
		for (Point2D pt : bgCurve) {
			if (pt.getX() < origCurve.getMinX() || pt.getX() > origCurve.getMaxX())
				continue;
			double csVal = origCurve.getInterpolatedY_inLogXLogYDomain(pt.getX());
			double bgVal = 1d - (1d - pt.getY())/(1d - csVal);
			bgOnlyCurve.set(pt.getX(), bgVal);
		}
		bgOnlyCurve.setName("GMPE Background Only");
		funcs.add(bgOnlyCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED));
		
		Range xRange = new Range(Math.max(1e-3, bgCurve.getMinX()), 1e1);
		Range yRange = new Range(1e-8, 1e0);
		
		XY_DataSet xy = new DefaultXY_DataSet();
		if (isProbAtIML) {
			xy.set(level, yRange.getLowerBound());
			xy.set(level, yRange.getUpperBound());
		} else {
			xy.set(xRange.getLowerBound(), level);
			xy.set(xRange.getUpperBound(), level);
		}
		
		funcs.add(xy);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
		
		PlotSpec spec = new PlotSpec(funcs, chars, title, xAxisLabel, "Annual Probability of Exceedance");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(24);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(24);
		gp.setBackgroundColor(Color.WHITE);
		gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		
		gp.drawGraphPanel(spec, true, true, xRange, yRange);
		
		gp.getChartPanel().setSize(800, 600);
		gp.saveAsPNG(outputFile.getAbsolutePath());
	}

}
