package org.opensha.sha.cybershake.maps;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.region.SitesInGriddedRegion;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.SiteDataValueList;
import org.opensha.commons.data.siteData.impl.CVM4BasinDepth;
import org.opensha.commons.data.siteData.impl.USGSBayAreaBasinDepth;
import org.opensha.commons.data.siteData.impl.WaldAllenGlobalVs30;
import org.opensha.commons.data.siteData.impl.WillsMap2006;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.ParameterException;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.util.XMLUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.ScenarioShakeMapCalculator;
import org.opensha.sha.cybershake.calc.ShakeMapComputation;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.attenRelImpl.CB_2008_AttenRel;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGA_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGD_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGV_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

public class HardCodedScenarioShakeMapGen {
	
	private static final String cacheDir = "/home/kevin/CyberShake/M8/";
	
	private static EqkRupture getRupture(int sourceID, int rupID, Location hypo) {
		AbstractERF ucerf = MeanUCERF2_ToDB.createUCERF2ERF();
		ucerf.updateForecast();
		EqkRupture rup = ucerf.getRupture(sourceID, rupID);
		rup.setHypocenterLocation(hypo);
		return rup;
	}
	
	private static ArrayList<AttenuationRelationship> getAttenRels(double[] periods) {
		ArrayList<AttenuationRelationship> realAR = new ArrayList<AttenuationRelationship>();
//		attenRels.add(new AS_2008_AttenRel(null));
//		double[] periods = { -2, -1, 0.5, 1.0 };
		
		for (double period : periods) {
//			attenRels.add(new Abrahamson_2000_AttenRel(null));
			
			ArrayList<AttenuationRelationship> attenRels = new ArrayList<AttenuationRelationship>();
//			attenRels.add(new ShakeMap_2003_AttenRel(null));
			attenRels.add(new CB_2008_AttenRel(null));
			for (AttenuationRelationship attenRel : attenRels) {
				attenRel.setParamDefaults();
				if (period == -2) {
					if (attenRel.isIntensityMeasureSupported(PGV_Param.NAME))
						attenRel.setIntensityMeasure(PGV_Param.NAME);
					else
						continue;
				} else if (period == -1) {
					if (attenRel.isIntensityMeasureSupported(PGD_Param.NAME))
						attenRel.setIntensityMeasure(PGD_Param.NAME);
					else
						continue;
				} else if (period == -3) {
					if (attenRel.isIntensityMeasureSupported(PGA_Param.NAME))
						attenRel.setIntensityMeasure(PGA_Param.NAME);
					else
						continue;
				} else {
					attenRel.setIntensityMeasure(SA_Param.NAME);
					try {
						SA_Param.setPeriodInSA_Param(attenRel.getIntensityMeasure(), period);
					} catch (Exception e) {
						continue;
					}
				}
				realAR.add(attenRel);
			}
		}
		return realAR;
	}
	
	private static SiteDataValueList<?> getData(SiteData<?> prov, GriddedRegion region)
	throws DocumentException, IOException {
		String cacheDir = "/home/kevin/CyberShake/M8/region/";
		String fName = cacheDir + prov.getShortName();
		String type = prov.getDataType();
		if (type.equals(SiteData.TYPE_DEPTH_TO_2_5))
			fName += "_2.5";
		else if (type.equals(SiteData.TYPE_DEPTH_TO_1_0))
			fName += "_1.0";
		fName += "_"+region.getName()+".xml";
		
		File file = new File(fName);
		if (file.exists()) {
			System.out.println("Reading: " + fName);
			Document doc = XMLUtils.loadDocument(file.getAbsolutePath());
			Element root = doc.getRootElement();
			return SiteDataValueList.fromXMLMetadata(root.element(SiteDataValueList.XML_METADATA_NAME));
		}
		if (prov.getDataType().equals(SiteData.TYPE_DEPTH_TO_2_5) || prov.getDataType().equals(SiteData.TYPE_DEPTH_TO_1_0)) {
			prov.getAdjustableParameterList().getParameter(CVM4BasinDepth.PARAM_MIN_BASIN_DEPTH_DOUBLE_NAME).setValue(1.0);
		}
		System.out.println("Getting values for: " + prov.getName() + " (" + prov.getDataType() + ")");
		SiteDataValueList<?> vals = prov.getAnnotatedValues(region.getNodeList());
		Document doc = XMLUtils.createDocumentWithRoot();
		Element root = doc.getRootElement();
		vals.toXMLMetadata(root);
		System.out.println("Writing: " + fName);
		XMLUtils.writeDocumentToFile(new File(fName), doc);
		return vals;
	}
	
	private static ArrayList<SiteDataValueList<?>> loadValLists(GriddedRegion region) throws IOException, DocumentException {
		ArrayList<SiteDataValueList<?>> valLists = new ArrayList<SiteDataValueList<?>>();
		
		valLists.add(getData(new WillsMap2006(), region));
		valLists.add(getData(new WaldAllenGlobalVs30(), region));
		valLists.add(getData(new CVM4BasinDepth(SiteData.TYPE_DEPTH_TO_2_5), region));
		valLists.add(getData(new CVM4BasinDepth(SiteData.TYPE_DEPTH_TO_1_0), region));
		valLists.add(getData(new USGSBayAreaBasinDepth(SiteData.TYPE_DEPTH_TO_2_5), region));
		valLists.add(getData(new USGSBayAreaBasinDepth(SiteData.TYPE_DEPTH_TO_1_0), region));
		
		return valLists;
	}
	
	private static SitesInGriddedRegion getSites(AttenuationRelationship attenRel, GriddedRegion region,
			ArrayList<SiteDataValueList<?>> valsLists) throws IOException {
		System.out.println("Num sites: " + region.getNodeCount());
		SitesInGriddedRegion sites = new SitesInGriddedRegion(region);
		
		sites.addSiteParams(attenRel.getSiteParamsIterator());
		sites.setSiteDataValueLists(valsLists);
		
		ArrayList<Parameter<?>> defaultSiteParams = new ArrayList<Parameter<?>>();
		for (Parameter<?> param : attenRel.getSiteParams()) {
			defaultSiteParams.add((Parameter<?>) param.clone());
		}
		sites.setDefaultSiteParams(defaultSiteParams);
		
		return sites;
	}
	
	private static CustomGriddedRegion loadCustomRegion() throws IOException {
		LocationList locs = new LocationList();
		
		String dir = "/home/kevin/CyberShake/M8/region/";
		
		BufferedInputStream lat_fi = new BufferedInputStream(new FileInputStream(new File(dir + "lat")));
		BufferedInputStream lon_fi = new BufferedInputStream(new FileInputStream(new File(dir + "lon")));
		
		byte[] lat_recordBuffer = new byte[4];
		ByteBuffer lat_record = ByteBuffer.wrap(lat_recordBuffer);
		lat_record.order(ByteOrder.LITTLE_ENDIAN);
		FloatBuffer lat_floatBuff = lat_record.asFloatBuffer();
		
		byte[] lon_recordBuffer = new byte[4];
		ByteBuffer lon_record = ByteBuffer.wrap(lon_recordBuffer);
		lon_record.order(ByteOrder.LITTLE_ENDIAN);
		FloatBuffer lon_floatBuff = lon_record.asFloatBuffer();
		
		while (lat_fi.read(lat_recordBuffer) == lat_recordBuffer.length) {
			double lat = lat_floatBuff.get(0);
			lon_fi.read(lon_recordBuffer);
			double lon = lon_floatBuff.get(0);
			
			Location loc = new Location(lat, lon);
			
//			System.out.println("Loaded loc: " + loc);
			
			locs.add(loc);
		}
		
		System.out.println("loaded "+locs.size()+" vals");
		
		return new CustomGriddedRegion(locs);
	}
	
	public static GeoDataSet computeBaseMap(EqkRupture rup,
			AttenuationRelationship attenRel,
			Iterable<Site> sites,
			boolean isProbAtIML, double value) throws ParameterException {
		ScenarioShakeMapCalculator calc = new ScenarioShakeMapCalculator();
		
		ArrayList<AttenuationRelationship> selectedAttenRels = new ArrayList<AttenuationRelationship>();
		selectedAttenRels.add(attenRel);
		ArrayList<Double> attenRelWts = new ArrayList<Double>();
		attenRelWts.add(1.0);
		
		GeoDataSet xyz = calc.getScenarioShakeMapData(selectedAttenRels, attenRelWts, sites, rup, isProbAtIML, value);
		// un log it
		xyz.exp();
		
		return xyz;
	}
	
	private static GeoDataSet getScatterData(int datasetID, int erfID, int rupVarScenID, int imTypeID,
			int sourceID, int rupID, Location hypo,
			boolean isProbAtIML, double value)
			throws FileNotFoundException, IOException {
		if (isProbAtIML || value != 0.5)
			throw new RuntimeException("CS shakemaps can only be computed for IML @ 50% prob.");
		File file = new File(cacheDir+"csScatter_"+datasetID+"_"+imTypeID+"_"+sourceID+"_"
				+rupID+"_"+isProbAtIML+"_"+value+".txt");
		
		if (file.exists()) {
			return ArbDiscrGeoDataSet.loadXYZFile(file.getAbsolutePath(), true);
		} else {
			ShakeMapComputation calc = new ShakeMapComputation(Cybershake_OpenSHA_DBApplication.getDB());
			
			GeoDataSet xyz = calc.getShakeMap(datasetID, erfID, rupVarScenID, imTypeID,
					sourceID, rupID, hypo);
			ArbDiscrGeoDataSet.writeXYZFile(xyz, file.getAbsolutePath());
			
			return xyz;
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParameterException 
	 * @throws ClassNotFoundException 
	 * @throws DocumentException 
	 */
	public static void main(String[] args) throws IOException, ParameterException, ClassNotFoundException, DocumentException {
//		int sourceID = 89;
//		int rupID = 3;
//		Location hypo = new Location(35.849, -120.3858, 15.5655);
//		int sourceID = 242;
//		int rupID = 26;
//		Location hypo = new Location(34.093,-118.0315,13.2411);
		
//		// ShakeOut 2011
//		int sourceID = 254;
//		int rupID = 30;
//		Location hypo = new Location(34.34, -119.17, 11.1);
		
		// ShakeOut 2013
		int sourceID = 77;
		int rupID = 0;
		Location hypo = null;
		
		boolean useBaseMap = true;
		double[] periods = { 3.0 };
		System.out.println("Creating Atten Rels...");
		ArrayList<AttenuationRelationship> attenRels = getAttenRels(periods);
		
		String scatterHardCodeFile = null;
//		String scatterHardCodeFile = "/home/kevin/CyberShake/eew/parkfield/min7.0/shakemap.txt";
//		String scatterHardCodeFile = "/home/kevin/ShakeOut/bband/pga.txt";
		
		String outputDir = "/home/kevin/Documents/2013 ShakeOut/cs_maps";
//		String outputDir = "/home/kevin/CyberShake/ShakeOut/bband/maps";
		
		GriddedRegion region = new CaliforniaRegions.CYBERSHAKE_MAP_GRIDDED(0.005);
		region.setName("CSReg");
//		Location northEast = new Location(38.4664459228515625, -113.125587463378906);
//		Location southWest = new Location(30.7091560363769531, -122.500495910644531);
//		Region region = new Region(northEast, southWest);
//		region.setName("M8Rect");
//		double spacing = 0.02;
//		CustomGriddedRegion region = loadCustomRegion();
//		region.setName("M8Exact");
//		double spacing = region.getSpacing();
		
		
		ArrayList<SiteDataValueList<?>> lists = loadValLists(region);
		
		CPT cpt = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
				"/resources/cpt/MaxSpectrum2.cpt"));
		boolean logPlot = true;
		
//		CPT cpt = GMT_CPT_Files.SHAKEMAP.instance();
//		boolean logPlot = false;
		
//		Double customMin = 0d;
//		Double customMax = 0.5d;
		
//		Double customMin = null;
//		Double customMax = null;
		
		boolean isProbAt_IML = false;
		double val = 0.5;
		
		for (AttenuationRelationship attenRel : attenRels) {
			String imt = attenRel.getIntensityMeasure().getName();
			double period;
			if (imt.equals(SA_Param.NAME)) {
				period = SA_Param.getPeriodInSA_Param(attenRel.getIntensityMeasure());
			} else {
				period = -1;
			}
			GeoDataSet baseMap = null;
			if (useBaseMap) {
				File baseMapFile = new File(cacheDir+"baseMap_"+attenRel.getShortName()
						+"_"+region.getSpacing()+"_"+sourceID+"_"+rupID+"_"+isProbAt_IML+"_"+val+"_"+region.getName()
						+"_"+imt+"_"+period+".txt");
				
				if (baseMapFile.exists()) {
					System.out.println("Loading Base Map Data");
					baseMap = ArbDiscrGeoDataSet.loadXYZFile(baseMapFile.getAbsolutePath(), true);
				} else {
					System.out.println("Getting rupture...");
					EqkRupture rup = getRupture(sourceID, rupID, hypo);
					System.out.println("Rup rake: " + rup.getAveRake());
					System.out.println("Loading sites...");
					SitesInGriddedRegion sites = getSites(attenRel, region, lists);
					
					System.out.println("Generating ShakeMap...");
					baseMap = computeBaseMap(rup, attenRel, sites, isProbAt_IML, val);
					System.out.println("Writing: " + baseMapFile.getAbsolutePath());
					ArbDiscrGeoDataSet.writeXYZFile(baseMap, baseMapFile.getAbsolutePath());
				}
			}
			
			if (region instanceof CustomGriddedRegion)
				continue;
			
			GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
			
			int datasetID = 1;
			int imTypeID = 21;
			int erfID = 35;
			int rupVarScenID = 3;
			
			System.out.println("Loading Scatter Data");
			GeoDataSet scatterData;
			if (scatterHardCodeFile == null)
				scatterData = getScatterData(datasetID, erfID, rupVarScenID, imTypeID,
					sourceID, rupID, hypo, isProbAt_IML, val);
			else
				scatterData = ArbDiscrGeoDataSet.loadXYZFile(scatterHardCodeFile, true);
			
			System.out.println("loaded "+scatterData.size()+" scatter vals");
			
			InterpDiffMapType[] types = { InterpDiffMapType.BASEMAP, InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS };
//			InterpDiffMapType[] types = { InterpDiffMapType.BASEMAP };
//			InterpDiffMapType[] types = { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS };
			InterpDiffMap map = new InterpDiffMap(region, baseMap, region.getSpacing(), cpt, scatterData, interpSettings, types);
			String label;
			if (imt.equals(SA_Param.NAME)) {
				label = (float)period + " Sec SA";
			} else {
				label = imt;
			}
			map.setCustomLabel(label);
			map.setTopoResolution(TopographicSlopeFile.CA_THREE);
			map.setLogPlot(logPlot);
			map.setDpi(300);
			map.setXyzFileName("base_map.xyz");
//			map.setHighwayFile(HighwayFile.ALL);
			
//			if (logPlot) {
//				map.setCustomScaleMin(-1.7);
//				map.setCustomScaleMax(-0.2);
//			} else {
//				map.setCustomScaleMin(0.02);
//				map.setCustomScaleMax(0.6);
//			}
			
			String metadata = "";
			
			System.out.println("Generating map...");
			String url = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
			
//			String baseDir = "/home/kevin/CyberShake/M8/images/";
			
			String baseName = attenRel.getShortName()+"_"+region.getName();
			for (InterpDiffMapType type : types) {
				PosterImageGen.saveCurves(url, outputDir, baseName, type);
			}
			
			System.out.println("Map address: "+url);
			
			System.exit(0);
		}
	}

}

