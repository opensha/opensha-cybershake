package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dom4j.Document;
import org.dom4j.Element;
import org.jfree.data.Range;
import org.jfree.chart.ui.RectangleInsets;
import org.jfree.chart.util.UnitType;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.CVM4i26BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.ComparablePairing;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.Interpolate;
import org.opensha.commons.util.XMLUtils;
import org.opensha.commons.util.binFile.BinaryGeoDatasetRandomAccessFile;
import org.opensha.commons.util.binFile.GeolocatedRectangularBinaryMesh2DCalculator;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.mcer.ASCEDetLowerLimitCalc;
import org.opensha.sha.calc.mcer.CurveBasedMCErProbabilisitCalc;
import org.opensha.sha.calc.mcer.DeterministicResult;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.gui.util.ERFSaver;
import org.opensha.sha.earthquake.ERF;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

public class UGMS_WebToolCalc {
	
	private static DBAccess db;
	
	private Location loc;
	
	private File gmpeDir;
	private double gmpeSpacing;
	private String gmpeERF;
	private List<String> siteClassNames; // user selected class, or classes that bound user Vs30 value
	private double userVs30;
	private double z2p5;
	private double z1p0;
	
	private File outputDir;
	
	public static BiMap<String, Double> vs30Map = HashBiMap.create();
	private static final double minCalcVs30 = 150d;
	private static final double maxCalcVs30 = 1000d;
	private static List<Double> vs30ValsSorted = new ArrayList<>();
	static {
		vs30Map.put("AorB",				1000d);
		vs30Map.put("BBC",				880d);
		vs30Map.put("BC",				760d);
		vs30Map.put("BCC",				662d);
		vs30Map.put("C",				564d);
		vs30Map.put("CCD",				465d);
		vs30Map.put("CD",				366d);
		vs30Map.put("CDD",				320d);
		vs30Map.put("D",				274d);
		vs30Map.put("DDE",				229d);
		vs30Map.put("DE",				183d);
		vs30Map.put("DEE",				166d);
		vs30Map.put("E",				150d);
		vs30Map.put("D_default", 		-1d);
		vs30Map.put("Wills",			-2d);
		for (Double vs30 : vs30Map.values())
			if (vs30 != null && vs30 > 0)
				vs30ValsSorted.add(vs30);
		Collections.sort(vs30ValsSorted);
		vs30ValsSorted = Collections.unmodifiableList(vs30ValsSorted);
	}
	
	private static final double CS_MAX_DIST = 10d;
	private static final double GMPE_MAX_DIST = 5d;
	
	private static final int IM_TYPE_ID_FOR_SEARCH = 146; // RotD100, 3s
	private static final CyberShakeComponent component = CyberShakeComponent.RotD100;
	
	private static final double[] periods = {0.01,0.02,0.03,0.05,0.075,0.1,0.15,0.2,0.25,0.3,0.4,
			0.5,0.75,1.0,1.5,2.0,3.0,4.0,5.0,7.5,10.0};
	
	private CyberShakeSiteRun csRun; // list for future interpolation
	private File csDataDir;
	private double csDataSpacing;
	
	private Runs2DB runs2db;
	private SiteInfo2DB sites2db;
	
	private CyberShakeMCErDeterministicCalc csDetCalc;
	private CyberShakeMCErProbabilisticCalc csProbCalc;
	
	private static CodeVersion CODE_DEFAULT = CodeVersion.MCER;
	private CodeVersion codeVersion = CODE_DEFAULT;
	
	private enum CodeVersion {
		MCER,
		MCER_PROB,
		MCER_DET,
		BSE_N,
		BSE_E,
		LATBSDC
	}
	
	private static final String SPACING_REPLACE_STR = "123SPACING321";
	
	public enum SpectraType {
		MCER("Site Specific MCER", "MCER", "MCER", "mcer_spectrum_"+SPACING_REPLACE_STR+".bin"),
		MCER_PROB("MCER Probabilistic", "Prob", "ProbabilisticMCER", "mcer_prob_spectrum_"+SPACING_REPLACE_STR+".bin"),
		MCER_DET("MCER Deterministic", "Determ", "DeterministicMCER", "mcer_det_spectrum_"+SPACING_REPLACE_STR+".bin"),
		MCER_DET_LOWER("MCER Deterministic Lower Limit", "Determ Lower Limit", "DeterministicLowerLevel", "mcer_det_lower_spectrum_"+SPACING_REPLACE_STR+".bin"),
		MCER_DESIGN("Site Specific Design Response", "Design", "DesignResponseSpectrum"),
		MCER_STANDARD("Standard MCER", "Standard", "StandardMCERSpectrum"),
		MCER_DESIGN_STANDARD("Standard Design Response", "Standard Design", "StandardDesignResponseSpectrum"),
		BSE_2E("Site Specific BSE-2E", "BSE-2E", "BSE_2E", "bse_2e_spectrum_"+SPACING_REPLACE_STR+".bin"),
		BSE_1E("Site Specific BSE-1E", "BSE-1E", "BSE_1E", "bse_1e_spectrum_"+SPACING_REPLACE_STR+".bin"),
		BSE_2N("Site Specific BSE-2N", "BSE-2N", "BSE_2N", "mcer_spectrum_"+SPACING_REPLACE_STR+".bin"),
		BSE_1N("Site Specific BSE-1N", "BSE-1N", "BSE_1N"),
		SLE("Site-Specific Service Level Earthquake", "SLE", "SLE", "sle_spectrum_"+SPACING_REPLACE_STR+".bin");
		
		private String name, shortName, elementName, fileName;
		
		private SpectraType(String name, String shortName, String elementName) {
			this(name, shortName, elementName, null);
		}
		
		private SpectraType(String name, String shortName, String elementName, String fileName) {
			this.name = name;
			this.shortName = shortName;
			this.elementName = elementName;
			this.fileName = fileName;
		}
		
		public String getName() {
			return name;
		}
		
		public String getShortName() {
			return shortName;
		}
		
		public String getElementName() {
			return elementName;
		}
		
		public String getFileName(double spacing) {
			Preconditions.checkNotNull(fileName);
			return fileName.replaceAll(SPACING_REPLACE_STR, (float)spacing+"");
		}
		
		@Override
		public String toString() {
			return getShortName();
		}
	}
	
	public enum SpectraSource {
		GMPE("GMPE"),
		CYBERSHAKE("CyberShake"),
		COMBINED(null);
		
		private String name;
		private SpectraSource(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	}
	
	public enum DesignParameter {
		SDS,
		SD1,
		SMS,
		SM1,
		SXS,
		SX1,
		TL,
		TS,
		T0,
		PGAM() {
			@Override
			public String getFileName(double spacing, SpectraType type, SpectraSource source) {
				Preconditions.checkState(source == SpectraSource.COMBINED, "Only combined supported for PGAM");
				String fileName;
				switch (type) {
				case MCER:
					fileName = "pga_m_"+SPACING_REPLACE_STR+".bin";
					break;
				case MCER_PROB:
					fileName = "pga_m_prob_"+SPACING_REPLACE_STR+".bin";
					break;
				case MCER_DET:
					fileName = "pga_m_det_"+SPACING_REPLACE_STR+".bin";
					break;
				case BSE_2N:
					fileName = "pga_m_"+SPACING_REPLACE_STR+".bin";
					break;
				case BSE_2E:
					fileName = "bse_2e_pga_m_"+SPACING_REPLACE_STR+".bin";
					break;
				case BSE_1E:
					fileName = "bse_1e_pga_m_"+SPACING_REPLACE_STR+".bin";
					break;
				case SLE:
					fileName = "sle_pga_m_"+SPACING_REPLACE_STR+".bin";
					break;

				default:
					throw new IllegalStateException("PGAM not supported for "+type+", "+source);
				}
				return fileName.replaceAll(SPACING_REPLACE_STR, (float)spacing+"");
			}
		};
		
		public String getFileName(double spacing, SpectraType type, SpectraSource source) {
			throw new UnsupportedOperationException("getFileName not applicable to "+name());
		}
	}
	
	private Table<SpectraType, SpectraSource, DiscretizedFunc> spectraCache;
	
	private Document xmlMetadataDoc;
	private Element xmlRoot;
	private Element metadataEl;
	private Element csSiteEl;
	private Element gmpeSiteEl;
	private Element resultsEl;
	
	public UGMS_WebToolCalc(CommandLine cmd) {
		xmlMetadataDoc = XMLUtils.createDocumentWithRoot();
		xmlRoot = xmlMetadataDoc.getRootElement();
		
		metadataEl = xmlRoot.addElement("Metadata");
		metadataEl.addAttribute("startTimeMillis", System.currentTimeMillis()+"");
		metadataEl.addAttribute("dbHost", Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		metadataEl.addAttribute("component", component.name());
		
		if (cmd.hasOption("code-version"))
			codeVersion = CodeVersion.valueOf(cmd.getOptionValue("code-version"));
		System.out.println("Code Version: "+codeVersion);
		metadataEl.addAttribute("codeVersion", codeVersion.name());
		
		outputDir = new File(cmd.getOptionValue("output-dir"));
		Preconditions.checkState(outputDir.exists() || outputDir.mkdirs(),
				"Output dir doesn't exist and couldn't be created: %s", outputDir.getAbsolutePath());
		
		gmpeDir = new File(cmd.getOptionValue("gmpe-dir"));
		Preconditions.checkState(gmpeDir.exists(), "GMPE dir doesn't exist: %s", gmpeDir.getAbsolutePath());
		gmpeSpacing = Double.parseDouble(cmd.getOptionValue("gmpe-spacing"));
		gmpeERF = cmd.getOptionValue("gmpe-erf");
		
		int velModelID;
		if (cmd.hasOption("cs-data-dir")) {
			Preconditions.checkArgument(cmd.hasOption("cs-spacing"), "Must supply spacing with CS data directory");
			csDataDir = new File(cmd.getOptionValue("cs-data-dir"));
			Preconditions.checkState(csDataDir.exists() && csDataDir.isDirectory(), "CS data dir doesn't exist: %s", csDataDir.getAbsolutePath());
			
			csDataSpacing = Double.parseDouble(cmd.getOptionValue("cs-spacing"));
			
			// search by location
			Preconditions.checkState(cmd.hasOption("latitude"), "Must supply latitude when using precomputed CS data files");
			Preconditions.checkState(cmd.hasOption("longitude"), "Must supply latitude when using precomputed CS data files");
			double lat = Double.parseDouble(cmd.getOptionValue("latitude"));
			double lon = Double.parseDouble(cmd.getOptionValue("longitude"));
			loc = new Location(lat, lon);
			System.out.println("User location: "+loc);
			
			Preconditions.checkState(cmd.hasOption("vel-model-id"),
					"Must supply velocity model ID if using precomputed CS data files");
			velModelID = Integer.parseInt(cmd.getOptionValue("vel-model-id"));
		} else {
			try {
				db = new DBAccess(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME, Cybershake_OpenSHA_DBApplication.DATABASE_NAME);
			} catch (IOException e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			runs2db = new Runs2DB(db);
			sites2db = new SiteInfo2DB(db);
			
			if (cmd.hasOption("run-id")) {
				// calculation happening at a CyberShake site
				Preconditions.checkState(!cmd.hasOption("latitude"), "Can't specify both a location and a Run ID");
				
				Integer runID = Integer.parseInt(cmd.getOptionValue("run-id"));
				
				CybershakeRun run = runs2db.getRun(runID);
				Preconditions.checkNotNull(run, "No run found with id=%s", runID);
				CybershakeSite site = sites2db.getSiteFromDB(run.getSiteID());
				loc = site.createLocation();
				
				csRun = new CyberShakeSiteRun(site, run);
			} else {
				// calculation happening at an arbitrary point
				// get all completed runs
				Preconditions.checkState(cmd.hasOption("dataset-id"), "Must specify a CyberShake Dataset ID if no Run ID is specified");
				int datasetID = Integer.parseInt(cmd.getOptionValue("dataset-id"));
				List<CyberShakeSiteRun> completedRuns = getCompletedRunsForDataset(datasetID, IM_TYPE_ID_FOR_SEARCH);
				Preconditions.checkState(!completedRuns.isEmpty(),
						"No runs found for datasetID=%s with imTypeID=%s", datasetID, IM_TYPE_ID_FOR_SEARCH);
				List<CyberShakeSiteRun> csRuns = new ArrayList<>();
				if (cmd.hasOption("site-id")) {
					// search by site ID
					Preconditions.checkState(!cmd.hasOption("latitude"), "Can't specify both a location and a Site ID");
					Preconditions.checkState(!cmd.hasOption("latitude"), "Can't specify both a Site ID and Name");
					Integer siteID = Integer.parseInt(cmd.getOptionValue("site-id"));
					for (CyberShakeSiteRun run : completedRuns)
						if (run.getCS_Site().id == siteID)
							csRuns.add(run);
					Preconditions.checkState(!csRuns.isEmpty(),
							"No runs found for datasetID=%s with imTypeID=%s and siteID=%s", datasetID, IM_TYPE_ID_FOR_SEARCH, siteID);
					loc = csRuns.get(0).getLocation();
				} else if (cmd.hasOption("site-name")) {
					// search by site name
					Preconditions.checkState(!cmd.hasOption("latitude"), "Can't specify both a location and a Site Name");
					String siteName = cmd.getOptionValue("site-name");
					for (CyberShakeSiteRun run : completedRuns)
						if (run.getCS_Site().short_name.equals(siteName) || run.getCS_Site().name.equals(siteName))
							csRuns.add(run);
					Preconditions.checkState(!csRuns.isEmpty(),
							"No runs found for datasetID=%s with imTypeID=%s and siteName='%s'", datasetID, IM_TYPE_ID_FOR_SEARCH, siteName);
					loc = csRuns.get(0).getLocation();
				} else {
					// search by location
					Preconditions.checkState(cmd.hasOption("latitude"), "Must supply latitude (or site/run ID)");
					Preconditions.checkState(cmd.hasOption("longitude"), "Must supply longitude (or site/run ID)");
					double lat = Double.parseDouble(cmd.getOptionValue("latitude"));
					double lon = Double.parseDouble(cmd.getOptionValue("longitude"));
					loc = new Location(lat, lon);
					System.out.println("User location: "+loc);
					
					System.out.println("Searching for nearby completed sites");
					Region reg = new Region(loc, CS_MAX_DIST);
					for (CyberShakeSiteRun run : completedRuns)
						if (reg.contains(run.getLocation()))
							csRuns.add(run);
					Preconditions.checkState(!csRuns.isEmpty(),
							"No runs found for datasetID=%s with imTypeID=%s within %km of %s",
							datasetID, IM_TYPE_ID_FOR_SEARCH, CS_MAX_DIST, loc);
				}
				
				System.out.println(csRuns.size()+" completed nearby sites found: ");
				List<Double> distances = new ArrayList<>();
				for (CyberShakeSiteRun run : csRuns)
					distances.add(LocationUtils.horzDistanceFast(loc, run.getLocation()));
				csRuns = ComparablePairing.getSortedData(distances, csRuns);
				for (CyberShakeSiteRun run : csRuns)
					System.out.println("\tRunID="+run.getCS_Run().getRunID()+"\tSite="+run.getCS_Site().short_name);
				
				csRun = csRuns.get(0);
			}
			velModelID = csRun.getCS_Run().getVelModelID();
			
			System.out.println("Using closest CS Site location: "+csRun.getLocation());
			
			double minDist = LocationUtils.horzDistanceFast(loc, csRun.getLocation());
			System.out.println("Min dist: "+minDist+" km");
			
			csSiteEl = xmlRoot.addElement("CyberShakeRun");
			csSiteEl.addAttribute("runID", csRun.getCS_Run().getRunID()+"");
			csSiteEl.addAttribute("siteID", csRun.getCS_Site().id+"");
			csSiteEl.addAttribute("siteShortName", csRun.getCS_Site().short_name);
			csSiteEl.addAttribute("siteName", csRun.getCS_Site().name);
			csSiteEl.addAttribute("distFromUserLoc", valDF.format(minDist));
			csSiteEl.addAttribute("latitude", latLonDF.format(csRun.getLocation().getLatitude()));
			csSiteEl.addAttribute("longitude", latLonDF.format(csRun.getLocation().getLongitude()));
			
			File csCacheDir = new File(cmd.getOptionValue("cs-dir"));
			Preconditions.checkState(csCacheDir.exists(), "CS cache dir doesn't exist: %s", csCacheDir.getAbsolutePath());
			
			ERF csERF;
			try {
				csERF = ERFSaver.LOAD_ERF_FROM_FILE(UGMS_WebToolCalc.class.getResource("/org/opensha/sha/cybershake/conf/MeanUCERF.xml"));
			} catch (Exception e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			csERF.updateForecast();
			CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, csCacheDir, csERF);
			
			csDetCalc = new CyberShakeMCErDeterministicCalc(amps2db, csERF, component);
			csProbCalc = new CyberShakeMCErProbabilisticCalc(db, component);
		}
		
		metadataEl.addAttribute("latitude", latLonDF.format(loc.getLatitude()));
		metadataEl.addAttribute("longitude", latLonDF.format(loc.getLongitude()));
		
		System.out.println("Loc: "+loc);
		
		if (cmd.hasOption("class")) {
			// site class specified
			String siteClassName = cmd.getOptionValue("class").toUpperCase();
			String matchingClass = null;
			Double matchingVs30 = null;
			for (String key : vs30Map.keySet()) {
				if (key.toUpperCase().equals(siteClassName)) {
					matchingVs30 = vs30Map.get(key);
					matchingClass = key;
					break;
				}
			}
			if (siteClassName.equals("AORB"))
				siteClassName = "AorB"; // for bin file matching
			Preconditions.checkState(matchingVs30 != null, "Unknown site class: %s", matchingClass);
			Preconditions.checkState(!cmd.hasOption("vs30"), "Can't specify site class and Vs30!");
			siteClassNames = new ArrayList<>();
			siteClassNames.add(matchingClass);
			userVs30 = matchingVs30;
		} else if (cmd.hasOption("vs30")) {
			double vs30 = Double.parseDouble(cmd.getOptionValue("vs30"));
			double minDiff = Double.POSITIVE_INFINITY;
			int closestIndex = -1;
			for (int i=0; i<vs30ValsSorted.size(); i++) {
				Double catVs30 = vs30ValsSorted.get(i);
				double diff = Math.abs(vs30 - catVs30);
				if (diff < minDiff) {
					closestIndex = i;
					minDiff = diff;
				}
			}
			double closestVs30 = vs30ValsSorted.get(closestIndex);
			String closestClass = vs30Map.inverse().get(closestVs30);
			System.out.println("Closest GMPE site class "+closestClass+"="+(float)closestVs30
					+" for user Vs30 of "+vs30+" (diff="+minDiff+")");
			siteClassNames = new ArrayList<>();
			siteClassNames.add(closestClass);
			if (vs30 > minCalcVs30 && vs30 < maxCalcVs30 && (float)minDiff > 0f) {
				// only add second point for interp if within range and not an exact match
				int index2;
				// figure out sign
				if (vs30 > closestVs30)
					index2 = closestIndex + 1;
				else
					index2 = closestIndex - 1;
				double boundingVs30 = vs30ValsSorted.get(index2);
				String boundingClass = vs30Map.inverse().get(boundingVs30);
				double boundingDiff = Math.abs(boundingVs30 - vs30);
				System.out.println("\tBounding GMPE site class for interp "+boundingClass+"="+(float)boundingVs30
						+" (diff="+boundingDiff+")");
				siteClassNames.add(boundingClass);
			} else {
				if (vs30 < minCalcVs30)
					vs30 = minCalcVs30;
				else if (vs30 > maxCalcVs30)
					vs30 = maxCalcVs30;
			}
			userVs30 = vs30;
		} else {
			// use Wills map
			System.out.println("Fetching Vs30 from Wills 2015 map");
			try {
				WillsMap2015 wills;
				if (cmd.hasOption("wills-file")) {
					File willsFile = new File(cmd.getOptionValue("wills-file"));
					Preconditions.checkState(willsFile.exists(),
							"Wills 2015 file specified but doesnt exist: %s", willsFile.getAbsolutePath());
					if (cmd.hasOption("wills-header")) {
						System.out.println("Reading custom Wills 2015 header file");
						File hdrFile = new File(cmd.getOptionValue("wills-header"));
						Preconditions.checkState(hdrFile.exists(),
								"Wills 2015 header file specified but doesnt exist: %s", hdrFile.getAbsolutePath());
						GeolocatedRectangularBinaryMesh2DCalculator calc = GeolocatedRectangularBinaryMesh2DCalculator.readHDR(hdrFile);
						wills = new WillsMap2015(willsFile.getAbsolutePath(), calc);
					} else {
						wills = new WillsMap2015(willsFile.getAbsolutePath());
					}
				} else {
					// use servlet
					wills = new WillsMap2015();
				}
				userVs30 = wills.getValue(loc);
			} catch (IOException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			System.out.println("Wills 2015 Vs30: "+userVs30);
		}
		
		File z10File = null;
		File z25File = null;
		if (cmd.hasOption("z25-file"))
			z25File = new File(cmd.getOptionValue("z25-file"));
		if (cmd.hasOption("z10-file"))
			z10File = new File(cmd.getOptionValue("z10-file"));
		Preconditions.checkState((z10File == null && z25File == null) || (z10File != null && z25File != null),
				"Must specify either both or none of Z1.0/2.5 files");
		boolean fileBased = z10File != null;
		SiteData<Double> z10Prov;
		SiteData<Double> z25Prov;
		try {
			switch (velModelID) {
			case 5:
				if (fileBased) {
					z10Prov = new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0, z10File);
					z25Prov = new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5, z25File);
				} else {
					z10Prov = new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0);
					z25Prov = new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5);
				}
				break;
			case 10:
				if (fileBased) {
					z10Prov = new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_1_0, z10File);
					z25Prov = new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_2_5, z25File);
				} else {
					z10Prov = new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_1_0);
					z25Prov = new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_2_5);
				}
				break;

			default:
				throw new IllegalStateException("Unknown or unsupported Velocity Model ID: "+velModelID);
			}
			z1p0 = z10Prov.getValue(loc);
			z2p5 = z25Prov.getValue(loc);
		} catch (IOException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		metadataEl.addAttribute("vs30", vs30DF.format(userVs30));
		metadataEl.addAttribute("z1p0", zDF.format(z1p0));
		metadataEl.addAttribute("z2p5", zDF.format(z2p5));
		if (siteClassNames != null) {
			if (siteClassNames.size() == 1) {
				metadataEl.addAttribute("siteClass", siteClassNames.get(0));
			} else {
				Preconditions.checkState(siteClassNames.size() == 2, "Unexpected number of site classes");
				metadataEl.addAttribute("siteClass", siteClassNames.get(0));
				metadataEl.addAttribute("secondInterpSiteClass", siteClassNames.get(1));
			}
		}
		
		resultsEl = xmlRoot.addElement("Results");
		
		spectraCache = HashBasedTable.create();
	}
	
	private Element getResultsEl(SpectraSource source) {
		switch (source) {
		case COMBINED:
			return resultsEl;
		case CYBERSHAKE:
			return getCreateElement(resultsEl, source.getName());
		case GMPE:
			return getCreateElement(resultsEl, source.getName());

		default:
			throw new IllegalStateException("Unkown source: "+source);
		}
	}
	
	private static Element getCreateElement(Element root, String name) {
		Element el = root.element(name);
		if (el == null)
			el = root.addElement(name);
		return el;
	}
	
	private synchronized DiscretizedFunc getCalcSpectrum(SpectraType type, SpectraSource source) {
		if (spectraCache.contains(type, source))
			return spectraCache.get(type, source);
		
		Element resultsEl = getResultsEl(source);
		
		DiscretizedFunc spectrum;
		switch (source) {
		case CYBERSHAKE:
			spectrum = calcCyberShake(type);
			break;
		case GMPE:
			spectrum = calcGMPE(type);
			break;
		case COMBINED:
			spectrum = calcCombined(type);
			break;

		default:
			throw new IllegalStateException("Unknown spectra source: "+source);
		}
		if (source.getName() == null)
			spectrum.setName(type.getName());
		else
			spectrum.setName(source.getName()+" "+type.getShortName());
		spectrum.toXMLMetadata(resultsEl, type.getElementName(), valDF);
		spectraCache.put(type, source, spectrum);
		return spectrum;
	}
	
	private DiscretizedFunc calcCyberShake(SpectraType type) {
		if (type == SpectraType.BSE_1N) {
			DiscretizedFunc designResponseSpectrum = getCalcSpectrum(SpectraType.BSE_2N, SpectraSource.CYBERSHAKE);
			designResponseSpectrum = designResponseSpectrum.deepClone();
			designResponseSpectrum.scale(2d/3d);
			designResponseSpectrum.setName(type.getName());
			return designResponseSpectrum;
		}
		if (csDataDir != null) {
			System.out.println("Calculating CyberShake "+type+" with precomputed data file");
			String fileName = type.getFileName(csDataSpacing);
			Preconditions.checkState(fileName != null, "Cannot calculate '"+type+"' from only CyberShake precomputed data");
			File csDataFile = new File(csDataDir, fileName);
			Preconditions.checkState(csDataFile.exists(), "CS data file doesn't exist: %s", csDataFile.getAbsolutePath());
			try {
				GriddedSpectrumInterpolator interp = getInterpolator(csDataFile, csDataSpacing);
				return interp.getInterpolated(loc);
			} catch (Exception e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		} else if (type == SpectraType.MCER_DET_LOWER) {
			DiscretizedFunc periods = getCalcSpectrum(SpectraType.MCER_PROB, SpectraSource.CYBERSHAKE);
			return ASCEDetLowerLimitCalc.calc(periods, userVs30, getCalcDesignParam(DesignParameter.TL, null, null));
		} else {
			CyberShakeSiteRun site = csRun;
			
			System.out.println("Calculating CyberShake values on the fly");
			DiscretizedFunc spectrum = new ArbitrarilyDiscretizedFunc();
			
			for (double period : periods) {
				try {
					double value;
					switch (type) {
					case MCER:
						double probVal = getCalcSpectrum(SpectraType.MCER_PROB, SpectraSource.CYBERSHAKE).getY(period);
						double detVal = getCalcSpectrum(SpectraType.MCER_DET, SpectraSource.CYBERSHAKE).getY(period);
						double detLowerVal = getCalcSpectrum(SpectraType.MCER_DET_LOWER, SpectraSource.CYBERSHAKE).getY(period);
						value = MCErCalcUtils.calcMCER(detVal, probVal, detLowerVal);
						break;
					case MCER_PROB:
						DiscretizedFunc curve = csProbCalc.calcHazardCurves(site, Lists.newArrayList(period)).get(period);
						value = CurveBasedMCErProbabilisitCalc.calcRTGM(curve);
						break;
					case MCER_DET:
						DeterministicResult csDet = csDetCalc.calc(site, period);
						Preconditions.checkNotNull(csDet); // will kick down to catch and skip this period if null
						value = csDet.getVal();
					default:
						throw new IllegalStateException("Cannot calc "+type+" from CyberShake only");
					}
					spectrum.set(period, value);
				} catch (RuntimeException e) {
					if (e.getMessage() != null && e.getMessage().startsWith("No CyberShake IM match")
							|| e instanceof NullPointerException) {
//						e.printStackTrace();
//						System.err.flush();
						System.out.println("Skipping period "+period+", no matching CyberShake IM");
//						System.out.flush();
						continue;
					}
					throw e;
				}
			}
			
			return spectrum;
		}
	}
	
	private DiscretizedFunc calcGMPE(SpectraType type) {
		if (type == SpectraType.BSE_1N) {
			DiscretizedFunc designResponseSpectrum = getCalcSpectrum(SpectraType.BSE_2N, SpectraSource.GMPE);
			designResponseSpectrum = designResponseSpectrum.deepClone();
			designResponseSpectrum.scale(2d/3d);
			designResponseSpectrum.setName(type.getName());
			return designResponseSpectrum;
		}
		System.out.println("Calculating GMPE");
		File gmpeDir = new File(this.gmpeDir, gmpeERF);
		Preconditions.checkState(gmpeDir.exists(), "GMPE/ERF dir doesn't exist: %s", gmpeDir.getAbsolutePath());
		
		List<String> dataFileNames = new ArrayList<>();
		List<Double> vs30Vals = new ArrayList<>();
		List<DiscretizedFunc> spectrum = new ArrayList<>();
		String typeFileName = type.getFileName(gmpeSpacing);
		if (siteClassNames == null) {
			dataFileNames.add("Wills_"+typeFileName);
			vs30Vals.add(userVs30);
		} else {
			for (String siteClassName : siteClassNames) {
				dataFileNames.add("class"+siteClassName+"_"+typeFileName);
				vs30Vals.add(vs30Map.get(siteClassName));
			}
		}
		
		for (int i=0; i<dataFileNames.size(); i++) {
			String dataFileName = dataFileNames.get(i);
			File dataFile = new File(gmpeDir, dataFileName);
			System.out.println("Loading GMPE data file: "+dataFile.getAbsolutePath());
			Preconditions.checkState(dataFile.exists(), "Data file doesn't exist: %s", dataFile.getAbsolutePath());
			
			gmpeSiteEl = xmlRoot.addElement("GMPE_Run");
			gmpeSiteEl.addAttribute("dataFile", dataFile.getAbsolutePath());
			
			GriddedSpectrumInterpolator interp;
			try {
				interp = getInterpolator(dataFile, gmpeSpacing);
			} catch (Exception e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			
			Location closestLoc = interp.getClosestGridLoc(loc);
			
			int numInterpPoints;
			try {
				spectrum.add(interp.getInterpolated(loc));
				numInterpPoints = 4;
			} catch (IllegalStateException e) {
				System.out.println("Interpolation failed, falling back to closest defined point."
						+ " This happens if one of the surrounding points is in the ocean.");
				closestLoc = interp.getClosestDefinedGridLoc(loc);
				spectrum.add(interp.getClosest(closestLoc));
				numInterpPoints = 1;
			}
			gmpeSiteEl.addAttribute("numInterpPoints", numInterpPoints+"");
			
			double minDist = LocationUtils.horzDistanceFast(closestLoc, loc);
			System.out.println("Distance to closest point in GMPE data file: "+minDist+" km");
			
			Preconditions.checkState(minDist <= GMPE_MAX_DIST,
					"Closest location in GMPE data file too far from user location: %s > %s", minDist, GMPE_MAX_DIST);
			
			gmpeSiteEl.addAttribute("distFromUserLoc", valDF.format(minDist));
			gmpeSiteEl.addAttribute("latitude", latLonDF.format(closestLoc.getLatitude()));
			gmpeSiteEl.addAttribute("longitude", latLonDF.format(closestLoc.getLongitude()));
			
//			// now PGA
//			String pgaDataFileName = pgaDataFileNames.get(i);
//			File pgaDataFile = new File(gmpeDir, pgaDataFileName);
//			System.out.println("Loading GMPE PGA data file: "+pgaDataFile.getAbsolutePath());
//			Preconditions.checkState(pgaDataFile.exists(), "Data file doesn't exist: %s", pgaDataFile.getAbsolutePath());
//			
//			interp = getPGAInterpolator(pgaDataFile, gmpeSpacing);
//			
//			closestLoc = interp.getClosestGridLoc(loc);
//			
//			try {
//				pgas.add(interp.getInterpolated(loc).getY(0));
//			} catch (IllegalStateException e) {
//				System.out.println("Interpolation failed, falling back to closest defined point."
//						+ " This happens if one of the surrounding points is in the ocean.");
//				closestLoc = interp.getClosestDefinedGridLoc(loc);
//				pgas.add(interp.getClosest(closestLoc).getY(0));
//			}
			
			minDist = LocationUtils.horzDistanceFast(closestLoc, loc);
			System.out.println("Distance to closest point in GMPE PGA data file: "+minDist+" km");
			
			Preconditions.checkState(minDist <= GMPE_MAX_DIST,
					"Closest location in GMPE data file too far from user location: %s > %s", minDist, GMPE_MAX_DIST);
		}
		
		if (spectrum.size() == 1) {
			return spectrum.get(0);
		} else {
			Element interpEl = null;
			if (type == SpectraType.MCER || type == SpectraType.MCER_PROB
					|| type == SpectraType.MCER_DET || type == SpectraType.BSE_1E)
				interpEl = xmlRoot.addElement("GMPE_Interpolation");
			return interpolateSpectraWithVs30(vs30Vals, spectrum, userVs30, interpEl);
		}
	}

	private static DiscretizedFunc interpolateSpectraWithVs30(List<Double> vs30Vals, List<DiscretizedFunc> spectrum,
			double vs30, Element interpEl) {
		// need to interpolate
		Preconditions.checkState(spectrum.size() == 2, "need exactly 2 for interpolation");
		
		double x1 = vs30Vals.get(0);
		DiscretizedFunc s1 = spectrum.get(0);
//			double p1 = pgas.get(0);
		double x2 = vs30Vals.get(1);
		DiscretizedFunc s2 = spectrum.get(1);
//			double p2 = pgas.get(1);
		Preconditions.checkState(s1.size() == s2.size(), "Spectra sizes inconsistent");
		
		if (x2 < x1) {
			// reverse
			double tx = x1;
			DiscretizedFunc ts = s1;
			x1 = x2;
			s1 = s2;
			x2 = tx;
			s2 = ts;
		}
		Preconditions.checkState(x2 > x1);
		Preconditions.checkState(x2 >= vs30 && vs30 >= x1,
				"User supplied Vs30 (%s) not in range [%s %s]", vs30, x1, x2);
		if (interpEl != null) {
			interpEl.addAttribute("vs30lower", vs30DF.format(x1));
			interpEl.addAttribute("vs30upper", vs30DF.format(x2));
			interpEl.addAttribute("userVs30", vs30DF.format(vs30));
			s1.toXMLMetadata(interpEl, "LowerSpectrum", valDF);
			s2.toXMLMetadata(interpEl, "UpperSpectrum", valDF);
		}
		
		double[] xs = {x1, x2};
		DiscretizedFunc interpSpectrum = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<s1.size(); i++) {
			double x = s1.getX(i);
			Preconditions.checkState((float)x == (float)s2.getX(i), "Spectrum x values inconsistent");
			double[] ys = {s1.getY(i), s2.getY(i)};
			// log-log interpolation as requested by Christine, 1/17/17
//				double y = Interpolate.findY(xs, ys, userVs30);
//				double y = Interpolate.findLogY(xs, ys, userVs30);
			double y = Interpolate.findLogLogY(xs, ys, vs30);
//				System.out.println("x="+userVs30+", x1="+x1+", x2="+x2);
//				System.out.println("y="+y+", y1="+ys[0]+", y2="+ys[1]);
			Preconditions.checkState((float)y >= (float)ys[0] && (float)y <= (float)ys[1] || (float)y >= (float)ys[1] && y <= (float)ys[0],
					"Bad interpolation, %s outside of range [%s %s]", y, ys[0], ys[1]);
			interpSpectrum.set(x, y);
		}
		return interpSpectrum;
	}
	
	private DiscretizedFunc calcCombined(SpectraType type) {
		if (type == SpectraType.MCER || type == SpectraType.BSE_2N || type == SpectraType.BSE_2E || type == SpectraType.BSE_1E
				|| type == SpectraType.SLE || type == SpectraType.MCER_PROB || type == SpectraType.MCER_DET || type == SpectraType.MCER_DET_LOWER) {
			// weight average CS and GMPE
			return MCERDataProductsCalc.calcFinalMCER(getCalcSpectrum(type, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(type, SpectraSource.GMPE));
		}
		if (type == SpectraType.MCER_DESIGN) {
			DiscretizedFunc designResponseSpectrum = getCalcSpectrum(SpectraType.MCER, SpectraSource.COMBINED);
			designResponseSpectrum = designResponseSpectrum.deepClone();
			designResponseSpectrum.scale(2d/3d);
			designResponseSpectrum.setName(type.getName());
			return designResponseSpectrum;
		}
		if (type == SpectraType.BSE_1N) {
			DiscretizedFunc designResponseSpectrum = getCalcSpectrum(SpectraType.BSE_2N, SpectraSource.COMBINED);
			designResponseSpectrum = designResponseSpectrum.deepClone();
			designResponseSpectrum.scale(2d/3d);
			designResponseSpectrum.setName(type.getName());
			return designResponseSpectrum;
		}
		// if we're here, then we're calculating from parameters
		if (type == SpectraType.MCER_STANDARD) {
			DiscretizedFunc standardSpectrum = DesignSpectrumCalc.calcSpectrum(
					getCalcDesignParam(DesignParameter.SMS, SpectraType.MCER, SpectraSource.COMBINED),
					getCalcDesignParam(DesignParameter.SM1, SpectraType.MCER, SpectraSource.COMBINED),
					getCalcDesignParam(DesignParameter.TL, null, null));
			standardSpectrum.setName("Standard MCER Spectrum");
			return standardSpectrum;
		}
		if (type == SpectraType.MCER_DESIGN_STANDARD) {
			DiscretizedFunc standardSpectrum = DesignSpectrumCalc.calcSpectrum(
					getCalcDesignParam(DesignParameter.SDS, SpectraType.MCER_DESIGN, SpectraSource.COMBINED),
					getCalcDesignParam(DesignParameter.SD1, SpectraType.MCER_DESIGN, SpectraSource.COMBINED),
					getCalcDesignParam(DesignParameter.TL, null, null));
			standardSpectrum.setName("Standard MCER Spectrum");
			return standardSpectrum;
		}
		throw new IllegalStateException("Cannot calculate combined spectrum for "+type);
	}
	
	private GriddedSpectrumInterpolator getInterpolator(File dataFile, double spacing) throws Exception {
		System.out.println("Loading spectrum from "+dataFile.getAbsolutePath());
		Stopwatch watch = Stopwatch.createStarted();
		BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dataFile.getAbsolutePath());
		Map<Location, ArbitrarilyDiscretizedFunc> map = reader.getCurveMap();
		watch.stop();
		System.out.println("Took "+watch.elapsed(TimeUnit.SECONDS)+" s to load spectrum");
		
		return getInterpolator(map, spacing);
	}
	
	private GriddedSpectrumInterpolator getPGAInterpolator(File dataFile, double spacing) throws Exception {
		System.out.println("Loading PGA from "+dataFile.getAbsolutePath());
		Stopwatch watch = Stopwatch.createStarted();
		ArbDiscrGeoDataSet data = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(dataFile);
		Map<Location, DiscretizedFunc> map = Maps.newHashMap();
		for (Location loc : data.getLocationList()) {
			double val = data.get(loc);
			LightFixedXFunc func = new LightFixedXFunc(new double[] {0d}, new double[] {val});
			map.put(loc, func);
		}
		watch.stop();
		System.out.println("Took "+watch.elapsed(TimeUnit.SECONDS)+" s to load PGA");
		
		return getInterpolator(map, spacing);
	}
	
	private GriddedSpectrumInterpolator getInterpolator(Map<Location, ? extends DiscretizedFunc> map, double spacing) {
		// filter for just surrounding points for faster gridding/interp
		int numBuffer = 50;
		double lat = loc.getLatitude();
		double lon = loc.getLongitude();
		double minLat = lat - spacing*numBuffer;
		double maxLat = lat + spacing*numBuffer;
		double minLon = lon - spacing*numBuffer;
		double maxLon = lon + spacing*numBuffer;
		Map<Location, DiscretizedFunc> filteredMap = Maps.newHashMap();
		for (Location loc : map.keySet()) {
			lat = loc.getLatitude();
			lon = loc.getLongitude();
			if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon)
				filteredMap.put(loc, map.get(loc));
		}
		System.out.println("Filtered input grid from "+map.size()+" to "+filteredMap.size()+" data points surrounding user loc");
		Preconditions.checkState(!filteredMap.isEmpty(), "No locs within region buffer");
		
		Stopwatch watch = Stopwatch.createStarted();
		GriddedSpectrumInterpolator interp = new GriddedSpectrumInterpolator(filteredMap, spacing);
		watch.stop();
		System.out.println("Took "+watch.elapsed(TimeUnit.SECONDS)+" s create interpolator/grid");
		
		return interp;
	}
	
	public double getCalcDesignParam(DesignParameter param, SpectraType type, SpectraSource source) {
		double value;
		switch (param) {
		case SDS:
			Preconditions.checkState(type == SpectraType.MCER_DESIGN);
			value = calcSXS(getCalcSpectrum(SpectraType.MCER_DESIGN, source));
			break;
		case SD1:
			Preconditions.checkState(type == SpectraType.MCER_DESIGN);
			value = calcSX1(getCalcSpectrum(SpectraType.MCER_DESIGN, source));
			break;
		case SMS:
			Preconditions.checkState(type == SpectraType.MCER);
			value = calcSXS(getCalcSpectrum(SpectraType.MCER, source));
			break;
		case SM1:
			value = calcSX1(getCalcSpectrum(SpectraType.MCER, source));
			break;
		case SXS:
			value = calcSXS(getCalcSpectrum(type, source));
			break;
		case SX1:
			value = calcSX1(getCalcSpectrum(type, source));
			break;
		case TS:
			value = calcSX1(getCalcSpectrum(type, source))/calcSXS(getCalcSpectrum(type, source));
			break;
		case T0:
			value = 0.2*getCalcDesignParam(DesignParameter.TS, type, source);
			break;
		case TL:
			value = ASCEDetLowerLimitCalc.getTl(loc);
			break;
		case PGAM:
			value = calcPGAM(DesignParameter.PGAM, type, source);
			break;
		default:
			throw new IllegalStateException("Unknown/unimplemented design param: "+param);
		}
		
		System.out.println("Calculated "+param+": "+(float)value);
		if (type == null) {
			metadataEl.addAttribute(param.name(), valDF.format(value));
		} else {
			Element rootEl;
			if (source == SpectraSource.COMBINED || source == null)
				rootEl = resultsEl;
			else
				rootEl = resultsEl.element(source.name());
			Preconditions.checkNotNull(rootEl);
			Element spectrumEl = rootEl.element(type.getElementName());
			Preconditions.checkNotNull(spectrumEl, "Spectrum XML element not created for %s when computing %s from source %s",
					type, param, source);
			spectrumEl.addAttribute(param.name(), valDF.format(value));
		}
		return value;
	}
	
	private double calcSXS(DiscretizedFunc spectrum) {
		double value = 0;
		for (int i=0; i<spectrum.size(); i++) {
			double x = spectrum.getX(i);
			if (x < 0.2)
				continue;
			if (x > 0.5)
				break;
			double y = spectrum.getY(i);
			value = Math.max(value, 0.9 * y);
		}
		return value;
	}
	
	private double calcSX1(DiscretizedFunc spectrum) {
		double minPeriodSX1 = 1d;
		double maxPeriodSX1;
		if (userVs30 > 365.76) {
			// SD1  = max(T * Sa) for 1s <= T <= 2s
			maxPeriodSX1 = 2d;
		} else {
			// SD1 = max(T * Sa) for 1s <= T <= 5s
			maxPeriodSX1 = 5d;
		}
		double value = 0;
		for (int i=0; i<spectrum.size(); i++) {
			double x = spectrum.getX(i);
			if (x < minPeriodSX1)
				continue;
			if (x > maxPeriodSX1)
				break;
			double y = spectrum.getY(i);
			value = Math.max(value, x * y);
		}
		return value;
	}
	
	private double calcPGAM(DesignParameter param, SpectraType type, SpectraSource source) {
		System.out.println("Calculating "+param);
		File gmpeDir = new File(this.gmpeDir, gmpeERF);
		Preconditions.checkState(gmpeDir.exists(), "GMPE/ERF dir doesn't exist: %s", gmpeDir.getAbsolutePath());
		
		List<String> dataFileNames = new ArrayList<>();
		List<Double> vs30Vals = new ArrayList<>();
		String typeFileName = param.getFileName(gmpeSpacing, type, source);
		if (siteClassNames == null) {
			dataFileNames.add("Wills_"+typeFileName);
			vs30Vals.add(userVs30);
		} else {
			for (String siteClassName : siteClassNames) {
				dataFileNames.add("class"+siteClassName+"_"+typeFileName);
				vs30Vals.add(vs30Map.get(siteClassName));
			}
		}
		
		List<DiscretizedFunc> pgas = new ArrayList<>();
		for (int i=0; i<dataFileNames.size(); i++) {
			String dataFileName = dataFileNames.get(i);
			File dataFile = new File(gmpeDir, dataFileName);
			System.out.println("Loading GMPE "+param+" data file: "+dataFile.getAbsolutePath());
			Preconditions.checkState(dataFile.exists(), "Data file doesn't exist: %s", dataFile.getAbsolutePath());
			
			GriddedSpectrumInterpolator interp;
			try {
				interp = getPGAInterpolator(dataFile, gmpeSpacing);
			} catch (Exception e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			
			Location closestLoc = interp.getClosestGridLoc(loc);
			
			double pga;
			try {
				pga = interp.getInterpolated(loc).getY(0);
			} catch (IllegalStateException e) {
				System.out.println("Interpolation failed, falling back to closest defined point."
						+ " This happens if one of the surrounding points is in the ocean.");
				closestLoc = interp.getClosestDefinedGridLoc(loc);
				pga = interp.getClosest(closestLoc).getY(0);
			}
			DiscretizedFunc fakeSpectrum = new ArbitrarilyDiscretizedFunc();
			fakeSpectrum.set(0d, pga);
			pgas.add(fakeSpectrum);
		}
		
		double interpPGA;
		if (pgas.size() == 1) {
			interpPGA = pgas.get(0).getY(0);
		} else {
			interpPGA = interpolateSpectraWithVs30(vs30Vals, pgas, userVs30, null).getY(0);
		}
		
		return interpPGA;
	}
	
	public void plot() throws IOException {
		
		ParamPlotElem[] params = null;
		boolean psv = false;
		switch (codeVersion) {
		case MCER:
			plot("mcer_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("mcer_sa_design_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_DESIGN, Color.RED, PlotLineType.SOLID, 3f));
			plot("mcer_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.MCER, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.MCER, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f));
			writeCSV("mcer_sa", getCalcSpectrum(SpectraType.MCER, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.MCER, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.MCER, SpectraSource.COMBINED),
					getCalcSpectrum(SpectraType.MCER_DESIGN, SpectraSource.COMBINED));
			
			// enumerating them here puts them in the XML file
			getCalcDesignParam(DesignParameter.TL, null, null);
			getCalcDesignParam(DesignParameter.SMS, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SM1, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SDS, SpectraType.MCER_DESIGN, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SD1, SpectraType.MCER_DESIGN, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.TS, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.T0, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.PGAM, SpectraType.MCER, SpectraSource.COMBINED);
			break;

		case MCER_PROB:
			plot("mcer_prob_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_PROB, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("mcer_prob_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.MCER_PROB, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.MCER_PROB, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_PROB, Color.BLACK, PlotLineType.SOLID, 4f));
			writeCSV("mcer_prob_sa", getCalcSpectrum(SpectraType.MCER_PROB, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.MCER_PROB, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.MCER_PROB, SpectraSource.COMBINED));
			
			// enumerating them here puts them in the XML file
			getCalcDesignParam(DesignParameter.TL, null, null);
			break;

		case MCER_DET:
			plot("mcer_det_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_DET, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("mcer_det_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.MCER_DET, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.MCER_DET, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_DET, Color.BLACK, PlotLineType.SOLID, 4f));
			writeCSV("mcer_det_sa", getCalcSpectrum(SpectraType.MCER_DET, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.MCER_DET, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.MCER_DET, SpectraSource.COMBINED));
			
			// enumerating them here puts them in the XML file
			getCalcDesignParam(DesignParameter.TL, null, null);
			break;

		case BSE_N:
			plot("bse_2n_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_2N, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("bse_1n_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_2N, Color.BLACK, PlotLineType.SOLID, 4f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_1N, Color.RED, PlotLineType.SOLID, 3f));
			plot("bse_2n_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.BSE_2N, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.BSE_2N, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_2N, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("bse_1n_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.BSE_1N, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.BSE_1N, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_1N, Color.BLACK, PlotLineType.SOLID, 4f));
			writeCSV("bse_2n_1n_sa", getCalcSpectrum(SpectraType.BSE_2N, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.BSE_2N, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.BSE_2N, SpectraSource.COMBINED),
					getCalcSpectrum(SpectraType.BSE_1N, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.BSE_1N, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.BSE_1N, SpectraSource.COMBINED));
			
			// enumerating them here puts them in the XML file
			getCalcDesignParam(DesignParameter.TL, null, null);
			getCalcDesignParam(DesignParameter.SXS, SpectraType.BSE_2N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SX1, SpectraType.BSE_2N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.TS, SpectraType.BSE_2N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.T0, SpectraType.BSE_2N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SXS, SpectraType.BSE_1N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SX1, SpectraType.BSE_1N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.TS, SpectraType.BSE_1N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.T0, SpectraType.BSE_1N, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.PGAM, SpectraType.BSE_2N, SpectraSource.COMBINED);
			break;
			
		case BSE_E:
			plot("bse_2e_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_2E, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("bse_1e_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_1E, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("bse_2e_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.BSE_2E, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.BSE_2E, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_2E, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("bse_1e_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.BSE_1E, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.BSE_1E, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.BSE_1E, Color.BLACK, PlotLineType.SOLID, 4f));
			writeCSV("bse_2e_1e_sa", getCalcSpectrum(SpectraType.BSE_2E, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.BSE_2E, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.BSE_2E, SpectraSource.COMBINED),
					getCalcSpectrum(SpectraType.BSE_1E, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.BSE_1E, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.BSE_1E, SpectraSource.COMBINED));
			
			// enumerating them here puts them in the XML file
			getCalcDesignParam(DesignParameter.TL, null, null);
			getCalcDesignParam(DesignParameter.SXS, SpectraType.BSE_2E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SX1, SpectraType.BSE_2E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.TS, SpectraType.BSE_2E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.T0, SpectraType.BSE_2E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.PGAM, SpectraType.BSE_2E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SXS, SpectraType.BSE_1E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SX1, SpectraType.BSE_1E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.TS, SpectraType.BSE_1E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.T0, SpectraType.BSE_1E, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.PGAM, SpectraType.BSE_1E, SpectraSource.COMBINED);
			break;
			
		case LATBSDC:
			plot("mcer_sa_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.SLE, Color.GREEN.darker(), PlotLineType.SOLID, 3f));
			plot("mcer_sa_design_final", psv, params,
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_DESIGN, Color.RED, PlotLineType.SOLID, 3f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.SLE, Color.GREEN.darker(), PlotLineType.SOLID, 3f));
			plot("sle_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.SLE, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.SLE, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.SLE, Color.BLACK, PlotLineType.SOLID, 4f));
			plot("mcer_sa_ingredients", psv, params,
					new SpectrumPlotElem(SpectraSource.GMPE, SpectraType.MCER, Color.BLUE, PlotLineType.SOLID, 2f),
					new SpectrumPlotElem(SpectraSource.CYBERSHAKE, SpectraType.MCER, Color.RED, PlotLineType.DASHED, 2f),
					new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f));
			writeCSV("mcer_sa", getCalcSpectrum(SpectraType.MCER, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.MCER, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.MCER, SpectraSource.COMBINED),
					getCalcSpectrum(SpectraType.MCER_DESIGN, SpectraSource.COMBINED),
					getCalcSpectrum(SpectraType.SLE, SpectraSource.GMPE),
					getCalcSpectrum(SpectraType.SLE, SpectraSource.CYBERSHAKE),
					getCalcSpectrum(SpectraType.SLE, SpectraSource.COMBINED));
			
			// enumerating them here puts them in the XML file
			getCalcDesignParam(DesignParameter.TL, null, null);
			getCalcDesignParam(DesignParameter.SMS, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SM1, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SDS, SpectraType.MCER_DESIGN, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.SD1, SpectraType.MCER_DESIGN, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.TS, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.T0, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.PGAM, SpectraType.MCER, SpectraSource.COMBINED);
			getCalcDesignParam(DesignParameter.PGAM, SpectraType.SLE, SpectraSource.COMBINED);
			break;
			
		default:
			throw new IllegalStateException("Unsupported code version: "+codeVersion);
		}
		
		
//		//	PSV		CS/GM  Final  FinDes  SM      SD
//		plot(false, true, true,   false, false, false);
//		plot(false, false, true,  false, false, false);
//		plot(false, false, true,  true, false, false);
//		plot(false, false, false, true,  false, false);
	}
	
	private static final DecimalFormat valDF = new DecimalFormat("0.000");
	private static final DecimalFormat latLonDF = new DecimalFormat("0.0000");
	private static final DecimalFormat vs30DF = new DecimalFormat("0");
	private static final DecimalFormat zDF = new DecimalFormat("0.00");
	
	private class SpectrumPlotElem {
		private final SpectraSource source;
		private final SpectraType type;
		private final Color color;
		private final PlotLineType line;
		private final double thickness;
		
		public SpectrumPlotElem(SpectraSource source, SpectraType type, Color color, PlotLineType line, double thickness) {
			this.source = source;
			this.type = type;
			this.color = color;
			this.line = line;
			this.thickness = thickness;
		}
	}
	
	private class ParamPlotElem {
		private final DesignParameter param;
		private final SpectraType type;
		private final SpectraSource source;
		private final Color color;
		private final PlotLineType line;
		private final double thickness;
		
		@SuppressWarnings("unused")
		public ParamPlotElem(DesignParameter param, SpectraType type, SpectraSource source, Color color,
				PlotLineType line, double thickness) {
			this.param = param;
			this.type = type;
			this.source = source;
			this.color = color;
			this.line = line;
			this.thickness = thickness;
		}
	}
	
	public void plot(String prefix, boolean psv, ParamPlotElem[] params, SpectrumPlotElem... spectra)
			throws IOException {
		Preconditions.checkState(spectra != null && spectra.length > 0);
		boolean xLog = psv;
		boolean yLog = psv;
		Range xRange = new Range(1e-2, 10d);
		Range yRange;
		
		String yAxisLabel;
		if (psv) {
			yRange = new Range(2e0, 2e3);
			yAxisLabel = "PSV (cm/s)";
		} else {
//			yRange = new Range(1e-2, 1e1);
			yRange = null;
			yAxisLabel = "Sa (g)";
		}

		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		if (!psv && params != null) {
			DiscretizedFunc xVals = getCalcSpectrum(spectra[0].type, spectra[0].source);
			for (ParamPlotElem param : params) {
				double val = getCalcDesignParam(param.param, param.type, param.source);
				DiscretizedFunc paramFunc = new ArbitrarilyDiscretizedFunc();
				paramFunc.setName(param.type.getShortName()+" "+param.param.name()+"="+valDF.format(val));
				for (int i=0; i<xVals.size(); i++) {
					double x = xVals.getX(i);
					paramFunc.set(x, val);
				}
				funcs.add(paramFunc);
				chars.add(new PlotCurveCharacterstics(param.line, (float)param.thickness, param.color));
			}
		}
		
		for (SpectrumPlotElem spectrumPlot : spectra) {
			DiscretizedFunc spectrum = getCalcSpectrum(spectrumPlot.type, spectrumPlot.source);
			
			if (psv)
				spectrum = MCErCalcUtils.saToPsuedoVel(spectrum);
			
			funcs.add(spectrum);
			chars.add(new PlotCurveCharacterstics(spectrumPlot.line, (float)spectrumPlot.thickness, spectrumPlot.color));
		}

//		String title = "MCER Acceleration Response Spectrum";
		String title = null;
		PlotSpec spec = new PlotSpec(funcs, chars, title, "Period (s)", yAxisLabel);
		spec.setLegendVisible(funcs.size() > 1 && funcs.get(0).getName() != null);
//		spec.setLegendVisible(true);

		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
		//			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(20);
		gp.setAxisLabelFontSize(22);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(20);

		gp.drawGraphPanel(spec, xLog, yLog, xRange, yRange);
		gp.getYAxis().setLabelInsets(new RectangleInsets(UnitType.ABSOLUTE, 15d, 0d, 0d, 0d));
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);

		gp.validate();
		gp.repaint();

		File file = new File(outputDir, prefix);
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
//		gp.saveAsTXT(file.getAbsolutePath()+".txt");
	}
	
	private void writeCSV(String prefix, DiscretizedFunc... spectra) throws IOException {
		CSVFile<String> csv = new CSVFile<String>(true);
		
		List<String> header = new ArrayList<>();
		header.add("Period (s)");
		for (DiscretizedFunc spectrum : spectra)
			header.add(spectrum.getName()+", Sa (g)");
		csv.addLine(header);
		
		for (double period : periods) {
			List<String> line = Lists.newArrayList((float)period+"");
			for (DiscretizedFunc spectrum : spectra)
				line.add(MCERDataProductsCalc.getValIfPresent(spectrum, period, valDF));
			
			csv.addLine(line);
		}
		
		csv.writeToFile(new File(outputDir, prefix+".csv"));
	}
	
	public void writeMetadata() throws IOException {
		metadataEl.addAttribute("endTimeMillis", System.currentTimeMillis()+"");
		XMLUtils.writeDocumentToFile(new File(outputDir, "metadata.xml"), xmlMetadataDoc);
	}
	
	/**
	 * Fetches all runs which have a hazard curve completed and inserted into the database for the given IM, with the given dataset ID
	 * @param datasetID
	 * @param imTypeID IM type of interest
	 * @return
	 */
	private List<CyberShakeSiteRun> getCompletedRunsForDataset(int datasetID, int imTypeID) {
		String sql = "SELECT DISTINCT R.*, S.* FROM CyberShake_Runs R JOIN Hazard_Curves C JOIN CyberShake_Sites S"
				+ " ON R.Run_ID=C.Run_ID AND R.Site_ID=S.CS_Site_ID"
				+ " WHERE C.Hazard_Dataset_ID="+datasetID;
		if (imTypeID >= 0)
			sql += " AND C.IM_Type_ID="+imTypeID;
		
		ArrayList<CyberShakeSiteRun> runs = new ArrayList<CyberShakeSiteRun>();
		
		try {
			ResultSet rs = db.selectData(sql);
			boolean valid = rs.next();
			
			while (valid) {
				CybershakeRun run = CybershakeRun.fromResultSet(rs);
				CybershakeSite site = CybershakeSite.fromResultSet(rs);
				
				if (site.type_id != CybershakeSite.TYPE_TEST_SITE)
					runs.add(new CyberShakeSiteRun(site, run));
				
				valid = rs.next();
			}
			
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		System.out.println("Found "+runs.size()+" completed sites for datasetID="+datasetID+", imTypeID="+imTypeID);
		return runs;
	}
	
	private static Options createOptions() {
		Options ops = new Options();
		
		Option run = new Option("r", "run-id", true, "CyberShake run ID, if a specific site is wanted");
		run.setRequired(false);
		ops.addOption(run);
		
		Option site = new Option("s", "site-id", true, "CyberShake site ID, if a specific site is wanted");
		site.setRequired(false);
		ops.addOption(site);
		
		Option siteName = new Option("s", "site-name", true, "CyberShake site short name, if a specific site is wanted");
		siteName.setRequired(false);
		ops.addOption(siteName);
		
		Option datasetID = new Option("d", "dataset-id", true, "CyberShake dataset ID");
		datasetID.setRequired(false);
		ops.addOption(datasetID);
		
		Option lat = new Option("lat", "latitude", true, "Site latitude. Must be specified if a run or site ID is not");
		lat.setRequired(false);
		ops.addOption(lat);
		
		Option lon = new Option("lon", "longitude", true, "Site longitude. Must be specified if a run or site ID is not");
		lon.setRequired(false);
		ops.addOption(lon);
		
		Option siteClass = new Option("c", "class", true,
				"Site class (e.g. CD). If neither this or vs30 is specified, Wills value will be used");
		siteClass.setRequired(false);
		ops.addOption(siteClass);
		
		Option vs30 = new Option("v", "vs30", true,
				"Vs30 (m/s). If neither this or site class is specified, Wills value will be used");
		vs30.setRequired(false);
		ops.addOption(vs30);
		
		Option gmpeDir = new Option("g", "gmpe-dir", true, "Directory containing GMPE precomputed data files");
		gmpeDir.setRequired(true);
		ops.addOption(gmpeDir);
		
		Option gmpeSpacing = new Option("gs", "gmpe-spacing", true, "Grid spacing of GMPE precomputed data files");
		gmpeSpacing.setRequired(true);
		ops.addOption(gmpeSpacing);
		
		Option csDir = new Option("csdir", "cs-dir", true, "Directory containing CS cache files");
		csDir.setRequired(false);
		ops.addOption(csDir);
		
		Option csData = new Option("csdata", "cs-data-dir", true, "CS precomputed data dir");
		csData.setRequired(false);
		ops.addOption(csData);
		
		Option csSpacing = new Option("csspacing", "cs-spacing", true, "Grid spacing of CS precomputed data file");
		csSpacing.setRequired(false);
		ops.addOption(csSpacing);
		
		Option gmpeERF = new Option("g", "gmpe-erf", true, "GMPE ERF ('UCERF2' or 'UCERF3')");
		gmpeERF.setRequired(true);
		ops.addOption(gmpeERF);
		
		Option outputDir = new Option("o", "output-dir", true, "Output directory, will be created if it doesn't exist");
		outputDir.setRequired(true);
		ops.addOption(outputDir);
		
		Option willsFile = new Option("w", "wills-file", true,
				"Path to Wills 2015 data file for local access, otherwise servlet will be used");
		willsFile.setRequired(false);
		ops.addOption(willsFile);
		
		Option willsHeaderFile = new Option("w", "wills-header", true,
				"Path to Wills 2015 header file, which can be used if a different grid is supplied than the full (large) Wills 2015 data file.");
		willsHeaderFile.setRequired(false);
		ops.addOption(willsHeaderFile);
		
		Option velModelID = new Option("vm", "vel-model-id", true,
				"Velocity model ID, required if using precomputed CS data files");
		velModelID.setRequired(false);
		ops.addOption(velModelID);
		
		Option z25File = new Option("z25", "z25-file", true, "Path to Z2.5 binary file");
		z25File.setRequired(false);
		ops.addOption(z25File);
		
		Option z10File = new Option("z10", "z10-file", true, "Path to Z1.0 binary file");
		z10File.setRequired(false);
		ops.addOption(z10File);
		
		Option help = new Option("?", "help", false, "Display this message");
		help.setRequired(false);
		ops.addOption(help);
		
		List<String> codeOptions = new ArrayList<>();
		for (CodeVersion code : CodeVersion.values())
			codeOptions.add(code.name());
		Option codeOption = new Option("cd", "code-version", true, "Code version, one of: "+Joiner.on(",").join(codeOptions)
				+". Default: "+CODE_DEFAULT.name());
		codeOption.setRequired(false);
		ops.addOption(codeOption);
		
		return ops;
	}
	
	public static void printHelp(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( appName, options, true );
		System.exit(2);
	}
	
	public static void printUsage(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter pw = new PrintWriter(System.out);
		formatter.printUsage(pw, 80, appName, options);
		pw.flush();
		System.exit(2);
	}

	public static void main(String[] args) {
		if (args.length == 1 && args[0].equals("--hardcoded")) {
			File backendDir = new File("/home/kevin/git/UGMS-opensha-backend");
			File dataDir = new File(backendDir, "data");
			File siteDataDir = new File(dataDir, "site_params");
			// hardcoded
//			String argStr = "--latitude 34.026414 --longitude -118.300136";
//			String argStr = "--latitude 34.05204 --longitude -118.25713"; // LADT
//			String argStr = "--latitude 34.557 --longitude -118.125"; // LAPD
//			String argStr = "--run-id 3870"; // doesn't require dataset ID if run ID
//			String argStr = "--longitude -118.272049427032 --latitude 34.0407116420786";
//			String argStr = "--longitude -118.069 --latitude 33.745";
//			String argStr = "--longitude -119.26000 --latitude 34.27110"; // this is an interpolation fail site
//			String argStr = "--longitude -118.4761 --latitude 33.9743"; // offshore, still shows 4 interp points
//			String argStr = "--longitude -118.91 --latitude 33.969";
//			String argStr = "--longitude -117.5888 --latitude 33.2976";
//			String argStr = "--longitude -118.369 --latitude 34.043";
//			String argStr = "--longitude -118.25713 --latitude 34.05204";
//			String argStr = "--longitude -118.231510 --latitude 34.052847";
//			String argStr = "--longitude -118.23120 --latitude 34.0600"; // verification site
//			String argStr = "--longitude -118.23 --latitude 34.0600"; // verification site snapped to grid
			String argStr = "--longitude -118.53 --latitude 34.17"; // CB's Tarzana site
//			Location-1: 34.052847, -118.231510
//			Location-2:  34.053704, -118.234069
			
//			String argStr = "--site-name LADT";
//			String argStr = "--site-id 20";
			argStr += " --gmpe-dir "+new File(dataDir, "gmpe");
			argStr += " --gmpe-spacing 0.01";
			argStr += " --cs-data-dir "+new File(dataDir, "cs_study_15_4");
			argStr += " --cs-spacing 0.002";
			argStr += " --vel-model-id 5";
			argStr += " --z10-file "+new File(siteDataDir, "CVM4i26_depth_1.0.bin");
			argStr += " --z25-file "+new File(siteDataDir, "CVM4i26_depth_2.5.bin");
//			argStr += " --vs30 260";
			argStr += " --class D";
//			argStr += " --class D_default";
			argStr += " --gmpe-erf UCERF3";
//			argStr += " --wills-file /data/kevin/opensha/wills2015.flt";
			argStr += " --wills-file "+new File(siteDataDir, "wills_2015_vs30.flt");
			argStr += " --wills-header "+new File(siteDataDir, "wills_2015_vs30.hdr");
			
			argStr += " --output-dir /tmp/ugms_web_tool_compare";
			argStr += " --code-version MCER_PROB";
			
//			argStr += " --output-dir /tmp/ugms_web_tool/bse_n";
//			argStr += " --code-version BSE_N";
			
//			argStr += " --output-dir /tmp/ugms_web_tool/bse_e";
//			argStr += " --code-version BSE_E";
			
//			argStr += " --output-dir /tmp/ugms_web_tool/latbsdc";
//			argStr += " --code-version LATBSDC";
			
			args = Splitter.on(" ").splitToList(argStr).toArray(new String[0]);
		}
		
		try {
			Options options = createOptions();
			
			String appName = ClassUtils.getClassNameWithoutPackage(UGMS_WebToolCalc.class);
			
			CommandLineParser parser = new DefaultParser();
			
			if (args.length == 0) {
				printUsage(options, appName);
			}
			
			try {
				CommandLine cmd = parser.parse( options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					printHelp(options, appName);
				}
				
				UGMS_WebToolCalc calc = new UGMS_WebToolCalc(cmd);
				
				calc.plot();
				calc.writeMetadata();
			} catch (MissingOptionException e) {
				Options helpOps = new Options();
				helpOps.addOption(new Option("h", "help", false, "Display this message"));
				try {
					CommandLine cmd = parser.parse( helpOps, args);
					
					if (cmd.hasOption("help")) {
						printHelp(options, appName);
					}
				} catch (ParseException e1) {}
				System.err.println(e.getMessage());
				printUsage(options, appName);
//			e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
				printUsage(options, appName);
			}
			
			System.out.println("Done!");
			if (db != null)
				db.destroy();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			if (db != null)
				db.destroy();
			System.exit(1);
		}
	}

}
