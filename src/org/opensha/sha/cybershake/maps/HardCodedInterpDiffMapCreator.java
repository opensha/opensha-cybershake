package org.opensha.sha.cybershake.maps;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.region.CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION;
import org.opensha.commons.data.xyz.AbstractGeoDataSet;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol.Symbol;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.commons.util.cpt.CPTVal;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.ModProbConfig;
import org.opensha.sha.cybershake.bombay.BombayBeachHazardCurveCalc;
import org.opensha.sha.cybershake.bombay.ModProbConfigFactory;
import org.opensha.sha.cybershake.bombay.ScenarioBasedModProbConfig;
import org.opensha.sha.cybershake.db.AttenRelCurves2DB;
import org.opensha.sha.cybershake.db.AttenRelDataSets2DB;
import org.opensha.sha.cybershake.db.AttenRels2DB;
import org.opensha.sha.cybershake.db.CybershakeHazardCurveRecord;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncLevelParam;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncTypeParam;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class HardCodedInterpDiffMapCreator {
	
	static boolean LOCAL_MAPGEN = true;
	private static CyberShake_GMT_MapGenerator mapGen;
	static File KEVIN_GMT_DATA_DIR = new File("/data/kevin/opensha/gmt");
	
	protected static DBAccess cs_db;
	protected static DBAccess gmpe_db;
	
	private static ArbDiscrGeoDataSet getMainScatter(boolean isProbAt_IML, double val, int datasetID,
			int imTypeID, Collection<Integer> siteTypes) {
		List<Integer> datasetIDs = Lists.newArrayList(datasetID);
		return getMainScatter(isProbAt_IML, val, datasetIDs, imTypeID, siteTypes);
	}
	
	protected static ArbDiscrGeoDataSet getMainScatter(boolean isProbAt_IML, double val,
			List<Integer> datasetIDs, int imTypeID, Collection<Integer> siteTypes) {
		Preconditions.checkArgument(!datasetIDs.isEmpty(), "Must supply at least one dataset ID");
		ArbDiscrGeoDataSet scatterData = new ArbDiscrGeoDataSet(true);
		for (int datasetID : datasetIDs) {
			HazardCurveFetcher fetcher = new HazardCurveFetcher(cs_db, datasetID, imTypeID);
			List<CybershakeSite> sites = fetcher.getCurveSites();
			List<Double> vals = fetcher.getSiteValues(isProbAt_IML, val);
			
			for (int i=0; i<sites.size(); i++) {
				CybershakeSite site = sites.get(i);
				if (siteTypes == null) {
					if (site.type_id == CybershakeSite.TYPE_TEST_SITE)
						continue;
				} else {
					if (!siteTypes.contains(site.type_id)) {
//						System.out.println("Removing: "+site);
						continue;
					}
				}
				Location loc = site.createLocation();
				if (scatterData.contains(loc))
					continue;
				double siteVal = vals.get(i);
				scatterData.set(loc, siteVal);
			}
			System.out.println("Kept "+scatterData.size()+"/"+sites.size()+" sites for dataset "+datasetID);
		}
		return scatterData;
	}
	
	private static ArbDiscrGeoDataSet getCustomScatter(ModProbConfig config, int imTypeID,
			boolean isProbAt_IML, double val) throws FileNotFoundException, IOException {
		if (imTypeID != 21)
			throw new IllegalArgumentException("IM type must be 21 for custom map");
		if (!isProbAt_IML)
			throw new IllegalArgumentException("isProbAt_IML must be true for custom map");
//		String dir = "/home/kevin/CyberShake/interpDiffInputFiles/"+singleName+"/";
//		String fname;
//		if (mod)
//			fname = "mod_";
//		else
//			fname = "orig_";
//		fname += (float)val+"g_singleDay.txt";
//		String fileName = dir + fname;
//		File file = new File(fileName);
//		if (file.exists()) {
//			System.out.println("Loading scatter from: " + fileName);
//			return ArbDiscrGeographicDataSet.loadXYZFile(fileName);
//		} else {
//			return loadCustomMapCurves(singleName, isProbAt_IML, val, mod);
//		}
		return loadCustomMapCurves(config, imTypeID, isProbAt_IML, val);
	}
	
	private static ArbDiscrGeoDataSet loadCustomMapCurves(ModProbConfig config, int imTypeID,
			boolean isProbAt_IML, double val) {
		
		int datasetID = config.getHazardDatasetID(35, 3, 5, 1, null);
		if (datasetID < 0)
			throw new RuntimeException("Couldn't get HC dataset id!");
		
		HazardCurveFetcher fetcher = new HazardCurveFetcher(cs_db, datasetID, imTypeID);
		
		List<DiscretizedFunc> curves = fetcher.getFuncs();
		List<CybershakeSite> sites = fetcher.getCurveSites();
		
		ArbDiscrGeoDataSet xyz = new ArbDiscrGeoDataSet(true);
		
		for (int i=0; i<curves.size(); i++) {
			DiscretizedFunc curve = curves.get(i);
			CybershakeSite site = sites.get(i);
			
//			System.out.println("loaded curve with "+curve.getNum()+" vals");
			
			double zVal = HazardDataSetLoader.getCurveVal(curve, isProbAt_IML, val);
			xyz.set(new Location(site.lat, site.lon), zVal);
		}
		
		return xyz;
	}
	
	private static CybershakeRun getRun(int runID, ArrayList<CybershakeRun> runs) {
		for (CybershakeRun run : runs) {
			if (runID == run.getRunID())
				return run;
		}
		return null;
	}
	
	private static CybershakeSite getSite(int siteID, List<CybershakeSite> sites) {
		for (CybershakeSite site : sites) {
			if (siteID == site.id)
				return site;
		}
		return null;
	}

	private static ArbDiscrGeoDataSet loadCustomMapCurves(
			String singleName, boolean isProbAt_IML, double val, boolean mod)
			throws FileNotFoundException, IOException {
		String curveDir = "/home/kevin/CyberShake/"+singleName+"/";
		if (mod)
			curveDir += "mod";
		else
			curveDir += "orig";
		curveDir += "Curves";
		File curveDirFile = new File(curveDir);
		if (curveDirFile.exists()) {
			Runs2DB runs2db = new Runs2DB(cs_db);
			ArrayList<CybershakeRun> runs = runs2db.getRuns();
			CybershakeSiteInfo2DB sites2db = new CybershakeSiteInfo2DB(cs_db);
			List<CybershakeSite> sites = sites2db.getAllSitesFromDB();
			ArbDiscrGeoDataSet xyz = new ArbDiscrGeoDataSet(true);
			
			for (File curveFile : curveDirFile.listFiles()) {
				if (curveFile.isFile() && curveFile.getName().endsWith(".txt")
						&& curveFile.getName().startsWith("run_")) {
					ArbitrarilyDiscretizedFunc func =
						ArbitrarilyDiscretizedFunc.loadFuncFromSimpleFile(curveFile.getAbsolutePath());
//						System.out.println("Loaded func with "+func.getNum()+" pts from "+curveFile.getName());
					String[] split = curveFile.getName().split("_");
					int runID = Integer.parseInt(split[1]);
					CybershakeRun run = getRun(runID, runs);
					CybershakeSite site = getSite(run.getSiteID(), sites);
					if (site == null)
						throw new RuntimeException("run '"+runID+"' not found!");
					double zVal = HazardDataSetLoader.getCurveVal(func, isProbAt_IML, val);
					xyz.set(new Location(site.lat, site.lon), zVal);
				}
			}
			return xyz;
		} else {
			throw new FileNotFoundException("Couldn't locate file or curve dir for dataset '"+singleName+"'");
		}
	}
	
	protected static GeoDataSet loadBaseMap(
			ScalarIMR imr,
			boolean isProbAt_IML,
			double level,
			int velModelID,
			int imTypeID,
			Region reg) throws SQLException {
		
		AttenRels2DB ar2db = new AttenRels2DB(gmpe_db);
		int attenRelID = ar2db.getAttenRelID(imr);
		
		AttenRelDataSets2DB ds2db = new AttenRelDataSets2DB(gmpe_db);
		int datasetID = ds2db.getDataSetID(attenRelID, 35, velModelID, 1, 1, null);
		if (datasetID < 0)
			datasetID = ds2db.getDataSetID(attenRelID, 36, velModelID, 1, 1, null);
		
		String cacheName = "ar_curves_"+attenRelID+"_"+datasetID+"_"
				+isProbAt_IML+"_"+(float)level+"_"+imTypeID;
		if (reg != null)
			cacheName += "_region_"+(float)reg.getMinLat()+"_"+(float)reg.getMaxLat()
					+"_"+(float)reg.getMinLon()+"_"+(float)reg.getMaxLon();
		
		File cacheFile = new File(getCacheDir(), cacheName+".txt");
		if (cacheFile.exists()) {
			try {
				System.out.println("Loading from "+cacheFile.getAbsolutePath());
				return ArbDiscrGeoDataSet.loadXYZFile(cacheFile.getAbsolutePath(), true);
			} catch (Exception e) {
				// don't fail on cache problem
				e.printStackTrace();
			}
		}
		System.out.println("Loading basemap, will save cache to: "+cacheFile.getAbsolutePath());
		
		AttenRelCurves2DB curves2db = new AttenRelCurves2DB(gmpe_db);
		GeoDataSet xyz = curves2db.fetchMap(datasetID, imTypeID, isProbAt_IML, level, true, reg);
		System.out.println("Got "+xyz.size()+" basemap values!");
		
		try {
			ArbDiscrGeoDataSet.writeXYZFile(xyz, cacheFile.getAbsolutePath());
		} catch (IOException e) {
			// don't fail on cache problem
			e.printStackTrace();
		}
		
		return xyz;
	}
	
	private static File getCacheDir() {
		if (System.getProperties().containsKey("CyberShakeCache")) {
			return new File(System.getProperties().getProperty("CyberShakeCache"));
		}
		return new File("/home/kevin/CyberShake/cache");
	}
	
	private static AbstractGeoDataSet loadBaseMap(boolean singleDay, boolean isProbAt_IML,
			double val, int imTypeID, String name) throws FileNotFoundException, IOException {
		int period;
		if (imTypeID == 11)
			period = 5;
		else if (imTypeID == 21)
			period = 3;
		else if (imTypeID == 26)
			period = 2;
		else
			throw new IllegalArgumentException("Unknown IM type id: " + imTypeID);
		String dir = "/home/kevin/CyberShake/baseMaps/"+name+"/";
		String fname = name+"_base_map_"+period+"sec_";
		if (isProbAt_IML) {
			fname += (float)val+"g";
		} else {
			if (val == 0.0004)
				fname += "2percent";
			else if (val == 0.002)
				fname += "10precent";
			else
				throw new IllegalArgumentException("Unown probability val: " + val);
		}
		if (singleDay)
			fname += "_singleDay";
		fname += "_hiRes.txt";
		String fileName = dir + fname;
		System.out.println("Loading basemap from: " + fileName);
		return ArbDiscrGeoDataSet.loadXYZFile(fileName, true);
	}
	
	private static PSXYSymbol getHypoSymbol(Region region, Location hypo) {
		if (hypo == null)
			return null;
		Location northWest = new Location(region.getMaxLat(), region.getMinLon());
		Location southEast = new Location(region.getMinLat(), region.getMaxLon());
		Region squareReg = new Region(northWest, southEast);
		if (!squareReg.contains(hypo)) {
			System.out.println("Hypocenter: "+hypo+"\nisn't within region: "+region);
			return null;
		}
		Point2D pt = new Point2D.Double(hypo.getLongitude(), hypo.getLatitude());
		double width = 0.4;
		double penWidth = 5;
		Color penColor = Color.WHITE;
		Color fillColor = Color.RED;
		return new PSXYSymbol(pt, Symbol.STAR, width, penWidth, penColor, fillColor);
	}
	
	public static void setTruncation(ScalarIMR imr, double trunc) {
		imr.getParameter(SigmaTruncLevelParam.NAME).setValue(trunc);
		if (trunc < 0)
			imr.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_NONE);
		else
			imr.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_1SIDED);
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args){
		try {
			LOCAL_MAPGEN = false;
			cs_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
//			cs_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
			gmpe_db = cs_db;
			
			boolean logPlot = false;
			
			int imTypeID = 167; // 2 sec SA, RotD50
			String imtLabel = "2sec SA";
			Double customMax = 1.0;
			
//			int imTypeID = 162; // 3 sec SA, RotD50
//			String imtLabel = "3sec SA";
//			Double customMax = 1.0;
			
//			int imTypeID = 158; // 5 sec SA, RotD50
//			String imtLabel = "5sec SA";
//			Double customMax = 0.6;
			
//			int imTypeID = 152; // 10 sec SA, RotD50
//			String imtLabel = "10sec SA";
//			Double customMax = 0.4;
			
//			int imTypeID = 151; // 2 sec SA, RotD100
//			String imtLabel = "2sec SA, RotD100";
//			Double customMax = 1.0;
			
//			int imTypeID = 86;
//			String imtLabel = "1sec SA";
//			Double customMax = 1.5d;
			
//			int imTypeID = 88;
//			String imtLabel = "0.5sec SA";
//			Double customMax = 3d;
			
//			int imTypeID = 94;
//			String imtLabel = "0.2sec SA";
//			Double customMax = 3d;
			
//			int imTypeID = 26; // 2 sec SA, GEOM
//			String imtLabel = "2sec SA";
//			Double customMax = 1.0;
			
//			int imTypeID = 21; // 3 sec SA, GEOM
//			String imtLabel = "3sec SA";
//			Double customMax = 1.0;
			
//			int imTypeID = 11; // 5 sec SA, GEOM
//			String imtLabel = "5sec SA";
//			Double customMax = 0.6;
			
//			int imTypeID = 1; // 10 sec SA, GEOM
//			String imtLabel = "10sec SA";
//			Double customMax = 0.4;
			
			String prefix = "study_17_3_3d_nobasemap";
//			String prefix = "study_17_3_1d";
//			String prefix = "study_15_12";
//			String prefix = "study_15_4";
//			String prefix = "study_14_2";
//			String prefix = "study_14_2_cvm_s426";
//			String prefix = "study_14_2_cvm_s426";
//			String prefix = "study_13_4_cvm_s4";
//			String prefix = "study_13_4_cvm_h119";
			String compPrefix = prefix+"_vs_15_4";
			File downloadDir = new File("/tmp/cs_maps_"+prefix);
			
//			Collection<Integer> siteTypes = CybershakeSite.getTypesExcept(
//					CybershakeSite.TYPE_TEST_SITE, CybershakeSite.TYPE_GRID_05_KM);
			Collection<Integer> siteTypes = null; // will still exclude test if null
			
			// the point on the hazard curve we are plotting
			boolean isProbAt_IML = false;
			double val = 0.0004;
			String durationLabel = "2% in 50 yrs";
			
//			boolean isProbAt_IML = true;
//			double val = 0.2;
			
			/* the main dataset(s) that we're plotting */
			
			// CCAi6 (Study 17.3)
//			int velModelID = 10;
			int velModelID = 9; // use 1D basemap
//			int velModelID = -1; // use Vs30 only basemap
			List<Integer> datasetIDs = Lists.newArrayList(81);
			Region region = new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION();
			
//			// CCA-1D (Study 17.3)
//			int velModelID = 9;
//			List<Integer> datasetIDs = Lists.newArrayList(80);
//			Region region = new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION();
			
//			// CVM-S4i26, AWP GPU w/ Stochastic HF, 1 Hz (Study 15.12)
//			int velModelID = 5;
//			List<Integer> datasetIDs = Lists.newArrayList(61);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
//			// CVM-S4i26, AWP GPU, 1 Hz (Study 15.4)
//			int velModelID = 5;
//			List<Integer> datasetIDs = Lists.newArrayList(57);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
//			// CVM-S4i26, AWP CPU
//			int velModelID = 5;
//			List<Integer> datasetIDs = Lists.newArrayList(37);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// CVM-S4i26, AWP GPU
//			int velModelID = 5;
//			List<Integer> datasetIDs = Lists.newArrayList(35);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// BBP 1D, AWP GPU
//			int velModelID = 8;
//			List<Integer> datasetIDs = Lists.newArrayList(38);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// CVM-S4i26, AWP CPU
//			int velModelID = 7; // wait but this is CVM-H no gtl????
//			List<Integer> datasetIDs = Lists.newArrayList(34);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
//			int velModelID = 5;
//			List<Integer> datasetIDs = Lists.newArrayList(12);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// CVM-S4
//			int velModelID = 1;
//			List<Integer> datasetIDs = Lists.newArrayList(20);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// CVM-H 11.9
//			int velModelID = 2; // actually 4, but this is just for the basemap. Use older CVM H as that's what we have calculated
//			List<Integer> datasetIDs = Lists.newArrayList(26);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// CVM-S4i26
//			int velModelID = 5;
//			List<Integer> datasetIDs = Lists.newArrayList(29);
//			Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
			
			// comparison dataset for ratio maps
//			List<Integer> compDatasetIDs = Lists.newArrayList(35);
//			List<Integer> compDatasetIDs = Lists.newArrayList(57);
			List<Integer> compDatasetIDs = null;
			// color bar limits for hazard maps (can be null to auto scale)
			// in G
			Double customMin = 0d;
////			Double customMax = 1.4;
//			Double customMax = 1.2;
//			Double customMin = -8d;
//			Double customMax = -2d;
			
			
//			boolean isProbAt_IML = true;
//			double val = 0.2;
//			String baseMapName = "cb2008";
//			ModProbConfig config = null;
////			ModProbConfig config = ModProbConfigFactory.getScenarioConfig(BombayBeachHazardCurveCalc.PARKFIELD_LOC);
//			boolean probGain = false;
//			String customLabel;
//			if (probGain)
//				customLabel = "Probability Gain";
//			else
//				customLabel = "POE "+(float)val+"G 3sec SA in 1 day";
//			if (logPlot && !probGain) {
//				customMin = -8.259081006598409;
////				customMax = -3.25;
//				customMax = -2.5;
//			}
			
			// probably always leave this null
			ModProbConfig config = null;
			// GMPE that we are using for the basemap
			// options: NGA 2008 average, or one of the 4: CB 2008, CY 2008, BA 2008, AS 2008
//			ScalarIMR baseMapIMR = AttenRelRef.NGA_2008_4AVG.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.CB_2008.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.CY_2008.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.BA_2008.instance(null);
			ScalarIMR baseMapIMR = null;
//			ScalarIMR baseMapIMR = AttenRelRef.AS_2008.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.ASK_2014.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.BSSA_2014.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.CB_2014.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.CY_2014.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.IDRISS_2014.instance(null);
//			ScalarIMR baseMapIMR = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
			boolean downloadBasemap = true;
			// GMPE params
			if (baseMapIMR != null) {
				baseMapIMR.setParamDefaults();
				setTruncation(baseMapIMR, 3.0);
			}
			// always leave false, used for previous study
			boolean probGain = false;
			
			// map label
			String customLabel = imtLabel+", "+durationLabel;
			
			String addr = getMap(region, logPlot, velModelID, datasetIDs, imTypeID, siteTypes, customMin, customMax,
					isProbAt_IML, val, baseMapIMR, config, probGain,
					customLabel);
			if (prefix == null)
				prefix = "dataset_"+Joiner.on("_").join(datasetIDs);
			
			String imtPrefix = imtLabel.replaceAll(" ", "_");
			
			prefix += "_"+imtPrefix;
			
			System.out.println("Map address: " + addr);
			if (downloadDir != null) {
				Preconditions.checkState(downloadDir.exists() || downloadDir.mkdir());
				
				fetchPlot(addr, "interpolated_marks.150.png", new File(downloadDir, prefix+"_marks.png"));
				fetchPlot(addr, "interpolated.150.png", new File(downloadDir, prefix+".png"));
				fetchPlot(addr, "interpolated.ps", new File(downloadDir, prefix+".ps"));
				if (downloadBasemap && baseMapIMR != null)
					fetchPlot(addr, "basemap.150.png", new File(downloadDir,
							baseMapIMR.getShortName()+"_"+imtPrefix+".png"));
				if (LOCAL_MAPGEN)
					FileUtils.deleteRecursive(new File(addr));
			}
			
			if (compDatasetIDs != null && !compDatasetIDs.isEmpty()) {
				String[] addrs = getCompareMap(logPlot, datasetIDs, compDatasetIDs, imTypeID, siteTypes,
						isProbAt_IML, val, customLabel, region);
				
				String diff = addrs[0];
				String ratio = addrs[1];
				
				System.out.println("Comp map address:\n\tdiff: "+diff+"\n\tratio: "+ratio);
				
				if (downloadDir != null) {
					if (compPrefix == null)
						compPrefix = prefix+"_vs_"+Joiner.on("_").join(compDatasetIDs);
					compPrefix += "_"+imtPrefix;
					
					fetchPlot(addr, "interpolated_marks.150.png", new File(downloadDir, compPrefix+"_diff.png"));
					fetchPlot(addr, "interpolated_marks.ps", new File(downloadDir, compPrefix+"_diff.ps"));
					fetchPlot(addr, "interpolated_marks.150.png", new File(downloadDir, compPrefix+"_ratio.png"));
					fetchPlot(addr, "interpolated_marks.ps", new File(downloadDir, compPrefix+"_ratio.ps"));
					
					if (LOCAL_MAPGEN)
						FileUtils.deleteRecursive(new File(addr));
				}
			}
			
			System.exit(0);
		} catch (Throwable t) {
			// TODO Auto-generated catch block
			t.printStackTrace();
			System.exit(1);
		}
	}
	
//	protected static InterpDiffMapType[] normPlotTypes = null;
	protected static InterpDiffMapType[] normPlotTypes = { InterpDiffMapType.INTERP_NOMARKS,
			InterpDiffMapType.INTERP_MARKS, InterpDiffMapType.BASEMAP, InterpDiffMapType.DIFF,
			InterpDiffMapType.RATIO};
	protected static InterpDiffMapType[] gainPlotTypes = 
			{ InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS};
	
	protected static String getMap(Region region, boolean logPlot, int velModelID, int datasetID, int imTypeID,
			Collection<Integer> siteTypes, Double customMin, Double customMax, boolean isProbAt_IML,
			double val, ScalarIMR baseMapIMR, ModProbConfig config,
			boolean probGain, String customLabel) throws FileNotFoundException,
			IOException, ClassNotFoundException, GMT_MapException, SQLException {
		List<Integer> datasetIDs = Lists.newArrayList(datasetID);
		return getMap(region, logPlot, velModelID, datasetIDs, imTypeID, siteTypes, customMin, customMax,
				isProbAt_IML, val, baseMapIMR, config, probGain, customLabel);
	}
	

	
	public static String getMap(Region region, boolean logPlot, int velModelID, List<Integer> datasetIDs, int imTypeID,
			Collection<Integer> siteTypes, Double customMin, Double customMax, boolean isProbAt_IML,
			double val, ScalarIMR baseMapIMR, ModProbConfig config,
			boolean probGain, String customLabel) throws FileNotFoundException,
			IOException, ClassNotFoundException, GMT_MapException, SQLException {
		boolean singleDay = config != null;
		System.out.println("Fetching curves...");
		AbstractGeoDataSet scatterData;
		Preconditions.checkState(!singleDay || siteTypes == null, "Site types not implemented for single day");
		if (singleDay)
			scatterData = getCustomScatter(config, imTypeID, isProbAt_IML, val);
		else
			scatterData = getMainScatter(isProbAt_IML, val, datasetIDs, imTypeID, siteTypes);
		
		return getMap(region, scatterData, logPlot, velModelID, imTypeID, customMin, customMax, isProbAt_IML,
				val, baseMapIMR, probGain, customLabel);
	}
	
	public static String getMap(Region region, GeoDataSet scatterData, boolean logPlot, int velModelID, int imTypeID,
			Double customMin, Double customMax, boolean isProbAt_IML,
			double val, ScalarIMR baseMapIMR,
			boolean probGain, String customLabel) throws FileNotFoundException,
			IOException, ClassNotFoundException, GMT_MapException, SQLException {
		double baseMapRes = 0.005;
		System.out.println("Loading basemap...");
		GeoDataSet baseMap;
		if (!probGain && baseMapIMR != null) {
			baseMap = loadBaseMap(baseMapIMR, isProbAt_IML, val, velModelID, imTypeID, region);
//			baseMap = loadBaseMap(singleDay, isProbAt_IML, val, imTypeID, baseMapName);
			System.out.println("Basemap has " + baseMap.size() + " points");
		} else {
			baseMap = null;
		}
		
		System.out.println("Creating map instance...");
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		
		InterpDiffMapType[] mapTypes = normPlotTypes;
		
		CPT cpt = CyberShake_GMT_MapGenerator.getHazardCPT();
		
		AbstractGeoDataSet refScatter = null;
		if (probGain) {
			ModProbConfig timeIndepModProb = ModProbConfigFactory.getModProbConfig(1);
			refScatter = getCustomScatter(timeIndepModProb, imTypeID, isProbAt_IML, val);
			scatterData = ProbGainCalc.calcProbGain(refScatter, scatterData);
			mapTypes = gainPlotTypes;
		}
		
		InterpDiffMap map = new InterpDiffMap(region, baseMap, baseMapRes, cpt, scatterData, interpSettings, mapTypes);
		map.setCustomLabel(customLabel);
		map.setTopoResolution(TopographicSlopeFile.CA_THREE);
		map.setLogPlot(logPlot);
		map.setDpi(300);
		map.setXyzFileName("base_map.xyz");
		map.setCustomScaleMin(customMin);
		map.setCustomScaleMax(customMax);
		
		Location hypo = null;
//		if (config != null && config instanceof ScenarioBasedModProbConfig) {
//			hypo = ((ScenarioBasedModProbConfig)config).getHypocenter();
//		}
		PSXYSymbol symbol = getHypoSymbol(region, hypo);
		if (symbol != null) {
			map.addSymbol(symbol);
		}
		
		String metadata = "isProbAt_IML: " + isProbAt_IML + "\n" +
						"val: " + val + "\n" +
						"imTypeID: " + imTypeID + "\n";
		
		System.out.println("Making map...");
		if (LOCAL_MAPGEN)
			return plotLocally(map);
		return CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
	}
	
	protected static String[] getCompareMap(boolean logPlot, List<Integer> dataset1IDs,
			List<Integer> dataset2IDs, int imTypeID, Collection<Integer> siteTypes,
			boolean isProbAt_IML, double val, String customLabel, Region region) throws FileNotFoundException,
			IOException, ClassNotFoundException, GMT_MapException, SQLException {
		System.out.println("Fetching curves...");
		AbstractGeoDataSet scatterData1 = getMainScatter(isProbAt_IML, val, dataset1IDs, imTypeID, siteTypes);
		AbstractGeoDataSet scatterData2 = getMainScatter(isProbAt_IML, val, dataset2IDs, imTypeID, siteTypes);
		
		return getCompareMap(logPlot, scatterData1, scatterData2, customLabel, false, region);
	}
	
	public static String[] getCompareMap(boolean logPlot, GeoDataSet scatterData1, GeoDataSet scatterData2,
			String customLabel, boolean tightCPTs, Region region) throws FileNotFoundException,
			IOException, ClassNotFoundException, GMT_MapException, SQLException {
		System.out.println("Creating map instance...");
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		
		InterpDiffMapType[] mapTypes = gainPlotTypes;
		
		CPT diffCPT = CyberShake_GMT_MapGenerator.getDiffCPT();
		CPT ratioCPT = CyberShake_GMT_MapGenerator.getRatioCPT();
		if (tightCPTs) {
			diffCPT = diffCPT.rescale(-0.1, 0.1);
			ratioCPT = (CPT) ratioCPT.clone();
			Preconditions.checkState(ratioCPT.size() == 6);
			float[] vals = {0.7f, 0.8f, 0.9f, 1.0f, 1.1f, 1.2f, 1.3f};
			for (int i=0; i<vals.length-1; i++) {
				CPTVal val = ratioCPT.get(i);
				val.start = vals[i];
				val.end = vals[i+1];
			}
		}
//		CPT diffCPT = polar.rescale(-0.4, 0.4);
//		CPT ratioCPT = polar.rescale(0.5, 1.5);
		
		AbstractGeoDataSet diffData = ProbGainCalc.calcProbDiff(scatterData2, scatterData1);
		AbstractGeoDataSet ratioData = ProbGainCalc.calcProbGain(scatterData2, scatterData1);
		
		MinMaxAveTracker diffTrack = new MinMaxAveTracker();
		MinMaxAveTracker ratioTrack = new MinMaxAveTracker();
		
		for (int index=0; index<diffData.size(); index++) {
			diffTrack.addValue(diffData.get(index));
			ratioTrack.addValue(ratioData.get(index));
		}
		
		System.out.println("Diff stats: "+diffTrack);
		System.out.println("Ratio stats: "+ratioTrack);
		
		InterpDiffMap map = new InterpDiffMap(region, null, 0.005, diffCPT, diffData, interpSettings, mapTypes);
		map.setCustomLabel("Difference, "+customLabel);
//		map.setTopoResolution(null);
		map.setTopoResolution(TopographicSlopeFile.CA_THREE);
		map.setLogPlot(logPlot);
		map.setDpi(300);
		map.setXyzFileName("diff_map.xyz");
		map.setCustomScaleMin((double)diffCPT.getMinValue());
		map.setCustomScaleMax((double)diffCPT.getMaxValue());
		map.setCPTEqualSpacing(!tightCPTs);
		map.setRescaleCPT(false);
		
		String metadata = "Ration Map\n";
		
		System.out.println("Making map...");
		String diffAddr;
		if (LOCAL_MAPGEN)
			diffAddr = plotLocally(map);
		else
			diffAddr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
		
		map = new InterpDiffMap(region, null, 0.005, ratioCPT, ratioData, interpSettings, mapTypes);
		map.setCustomLabel("Ratio, "+customLabel);
//		map.setTopoResolution(null);
		map.setTopoResolution(TopographicSlopeFile.CA_THREE);
		map.setLogPlot(logPlot);
		map.setDpi(300);
		map.setXyzFileName("ratio_map.xyz");
		map.setCustomScaleMin((double)ratioCPT.getMinValue());
		map.setCustomScaleMax((double)ratioCPT.getMaxValue());
		map.setCPTEqualSpacing(!tightCPTs);
		map.setRescaleCPT(false);
		
		System.out.println("Making map...");
		String ratioAddr;
		if (LOCAL_MAPGEN)
			ratioAddr = plotLocally(map);
		else
			ratioAddr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
		return new String[] {diffAddr, ratioAddr};
	}
	
	static String plotLocally(InterpDiffMap map) throws GMT_MapException, IOException {
		synchronized (HardCodedInterpDiffMapCreator.class) {
			if (mapGen == null)
				mapGen = new CyberShake_GMT_MapGenerator();
		}
		return mapGen.plotLocally(map, KEVIN_GMT_DATA_DIR).getAbsolutePath();
	}
	
	static void fetchPlot(String addr, String inFileName, File outFile) throws IOException {
		if (LOCAL_MAPGEN) {
			File inFile = new File(addr, inFileName);
			Preconditions.checkState(inFile.exists(), "In file doesn't exist: %s", inFile.getAbsolutePath());
			Files.copy(inFile, outFile);
		} else {
			if (!addr.endsWith("/"))
				addr += "/";
			FileUtils.downloadURL(addr+inFileName, outFile);
		}
	}

}
