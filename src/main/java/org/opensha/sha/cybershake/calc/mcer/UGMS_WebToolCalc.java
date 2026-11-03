package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
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
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.CVM4i26BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
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
import org.opensha.commons.util.binFile.GeolocatedRectangularBinaryMesh2DCalculator;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.mcer.CurveBasedMCErProbabilisitCalc;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;

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

/**
 * Backend calculator for the SCEC UGMS web tool using the <b>ASCE 7-22</b> standard through the
 * <a href="https://earthquake.usgs.gov/ws/building-codes/asce7-22">USGS ASCE 7-22 web service</a>.
 * It currently uses <b>CyberShake Study 15.4</b> data and will transition to <b>CyberShake Study 22.12</b>
 * (Phase 2 of the UGMS update).
 *
 * <p>Each side has three components: the probabilistic risk-targeted spectrum, the deterministic
 * 84th-percentile spectrum and the deterministic lower limit (DLL).</p>
 * <ul>
 * <li><b>GMPE side:</b> all three come from the USGS service ({@link USGS_ASCE7_22_Client}).</li>
 * <li><b>CyberShake side:</b> only the RotD100 probabilistic spectrum comes from CyberShake, read from
 * precomputed grid files ({@code --cs-data-dir}) or queried from the database. The 84th-percentile and
 * DLL spectra are copied from the GMPE side.</li>
 * <li><b>Each side's MCER:</b> min(probabilistic, max(84th, DLL)) at each period.</li>
 * <li><b>Merge:</b> the two MCER spectra are merged with the existing
 * {@link MCERDataProductsCalc#calcFinalMCER(DiscretizedFunc, DiscretizedFunc)} (GMPE unless CyberShake is
 * higher, then a period-dependent log-space weighting).</li>
 * <li><b>Design values:</b> SMS, SM1, SDS and SD1 are recomputed from the merged spectrum, not taken from the
 * service. PGAM and TL come from the service.</li>
 * </ul>
 *
 * <p>The service takes ASCE 7-22 site classes only. A user or Wills Vs30 is handled by querying the two
 * bounding classes and interpolating log-log in Vs30. The "Default" class MCER is the per-period envelope of
 * the MCERs of classes C, CD and D, which matches the service's own Default result.</p>
 *
 * <p>The Z1.0/Z2.5 lookups support only velocity models 5 and 10 (Study 15.4) and will need extending for
 * Study 22.12. The ASCE 7-16 version, which must stay on CyberShake 15.4, is
 * {@link Legacy_ASCE7_16_UGMS_WebToolCalc}.</p>
 */
public class UGMS_WebToolCalc {

	private static DBAccess db;

	private Location loc;

	private List<String> siteClassNames; // user selected class, or classes that bound user Vs30 value
	private boolean envelopeClasses; // true for the Default class: envelope of siteClassNames instead of interpolation
	private double userVs30;
	private double z2p5;
	private double z1p0;

	private File outputDir;

	private USGS_ASCE7_22_Client usgsClient = new USGS_ASCE7_22_Client();
	private Map<String, USGS_ASCE7_22_Client.Response> usgsResponses = new HashMap<>();

	/**
	 * ASCE 7-22 site classes and the Vs30 (m/s) the USGS service uses for each. "Default" maps to -1 because it is
	 * built from classes C, CD and D rather than sent to the service.
	 */
	public static BiMap<String, Double> vs30Map = HashBiMap.create();
	private static final double minCalcVs30 = 150d;
	private static final double maxCalcVs30 = 1500d;
	private static List<Double> vs30ValsSorted = new ArrayList<>();
	private static final List<String> DEFAULT_ENVELOPE_CLASSES = List.of("C", "CD", "D");
	static {
		// ASCE 7-22 site classes, with the Vs30 the USGS service uses for each
		vs30Map.put("A",				1500d);
		vs30Map.put("B",				1080d);
		vs30Map.put("BC",				760d);
		vs30Map.put("C",				530d);
		vs30Map.put("CD",				365d);
		vs30Map.put("D",				260d);
		vs30Map.put("DE",				185d);
		vs30Map.put("E",				150d);
		vs30Map.put("Default", 			-1d);
		for (Double vs30 : vs30Map.values())
			if (vs30 != null && vs30 > 0)
				vs30ValsSorted.add(vs30);
		Collections.sort(vs30ValsSorted);
		vs30ValsSorted = Collections.unmodifiableList(vs30ValsSorted);
	}
	
	private static final double CS_MAX_DIST = 10d;
	
	private static final int IM_TYPE_ID_FOR_SEARCH = 146; // RotD100, 3s
	private static final CyberShakeComponent component = CyberShakeComponent.RotD100;
	
	private static final double[] periods = {0.01,0.02,0.03,0.05,0.075,0.1,0.15,0.2,0.25,0.3,0.4,
			0.5,0.75,1.0,1.5,2.0,3.0,4.0,5.0,7.5,10.0};
	
	private CyberShakeSiteRun csRun; // list for future interpolation
	private File csDataDir;
	private double csDataSpacing;
	
	private Runs2DB runs2db;
	private SiteInfo2DB sites2db;
	
	private CyberShakeMCErProbabilisticCalc csProbCalc;

	private static final String SPACING_REPLACE_STR = "123SPACING321";

	/**
	 * Spectrum types this calculator produces. Only {@link #MCER_PROB} has a precomputed CyberShake grid file; the
	 * others come from the USGS service or are derived.
	 */
	public enum SpectraType {
		MCER("Site Specific MCER", "MCER", "MCER"),
		MCER_PROB("MCER Probabilistic", "Prob", "ProbabilisticMCER", "mcer_prob_spectrum_"+SPACING_REPLACE_STR+".bin"),
		MCER_DET("MCER Deterministic", "Determ", "DeterministicMCER"),
		MCER_DET_LOWER("MCER Deterministic Lower Limit", "Determ Lower Limit", "DeterministicLowerLevel"),
		MCER_DESIGN("Site Specific Design Response", "Design", "DesignResponseSpectrum");
		
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
		
		/** @return the long display name, used in plot legends */
		public String getName() {
			return name;
		}
		
		/** @return the short name, combined with the source name in plot legends and CSV headers */
		public String getShortName() {
			return shortName;
		}
		
		/** @return the XML element name for this spectrum in metadata.xml */
		public String getElementName() {
			return elementName;
		}
		
		/**
		 * Returns the precomputed CyberShake grid file name for this spectrum type.
		 * @param spacing grid spacing in degrees
		 * @return the file name
		 * @throws NullPointerException if this type has no grid file
		 */
		public String getFileName(double spacing) {
			Preconditions.checkNotNull(fileName);
			return fileName.replaceAll(SPACING_REPLACE_STR, (float)spacing+"");
		}
		
		/** @return the short name */
		@Override
		public String toString() {
			return getShortName();
		}
	}
	
	/**
	 * Where a spectrum comes from: the GMPE side (the USGS ASCE 7-22 service), CyberShake, or the merge of the two.
	 */
	public enum SpectraSource {
		GMPE("GMPE"),
		CYBERSHAKE("CyberShake"),
		COMBINED(null);
		
		private String name;
		private SpectraSource(String name) {
			this.name = name;
		}
		
		/** @return the display name, or null for the combined spectrum */
		public String getName() {
			return name;
		}
	}
	
	/**
	 * Design values reported for a run. SMS, SM1, SDS and SD1 are recomputed from the merged spectrum, TL and PGAM
	 * come from the USGS service, TS = SD1/SDS and T0 = 0.2 TS.
	 */
	public enum DesignParameter {
		SDS,
		SD1,
		SMS,
		SM1,
		TL,
		TS,
		T0,
		PGAM
	}

	private Table<SpectraType, SpectraSource, DiscretizedFunc> spectraCache;

	private Document xmlMetadataDoc;
	private Element xmlRoot;
	private Element metadataEl;
	private Element csSiteEl;
	private Element resultsEl;
	
	/**
	 * Sets up one calculation from parsed command-line arguments: the location (from coordinates, a CyberShake run or
	 * a CyberShake site), the CyberShake read mode (precomputed grid files or the database), the site class or classes,
	 * and the basin depths for the report. No spectra are computed until {@link #plot()} is called.
	 * @param cmd parsed options; see {@link #main(String[])}
	 */
	public UGMS_WebToolCalc(CommandLine cmd) {
		xmlMetadataDoc = XMLUtils.createDocumentWithRoot();
		xmlRoot = xmlMetadataDoc.getRootElement();
		
		metadataEl = xmlRoot.addElement("Metadata");
		metadataEl.addAttribute("startTimeMillis", System.currentTimeMillis()+"");
		metadataEl.addAttribute("dbHost", Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		metadataEl.addAttribute("component", component.name());
		metadataEl.addAttribute("codeVersion", "MCER");
		metadataEl.addAttribute("standard", "ASCE7-22");

		outputDir = new File(cmd.getOptionValue("output-dir"));
		Preconditions.checkState(outputDir.exists() || outputDir.mkdirs(),
				"Output dir doesn't exist and couldn't be created: %s", outputDir.getAbsolutePath());

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

			// only the probabilistic spectrum comes from CyberShake, so no deterministic calc or ERF is needed
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
			Preconditions.checkState(matchingVs30 != null, "Unknown site class: %s", siteClassName);
			Preconditions.checkState(!cmd.hasOption("vs30"), "Can't specify site class and Vs30!");
			siteClassNames = new ArrayList<>();
			if (matchingVs30 < 0) {
				// Default: the service only returns the envelope, so build it from the underlying classes
				siteClassNames.addAll(DEFAULT_ENVELOPE_CLASSES);
				envelopeClasses = true;
			} else {
				siteClassNames.add(matchingClass);
			}
			userVs30 = matchingVs30;
		} else {
			double vs30;
			if (cmd.hasOption("vs30")) {
				vs30 = Double.parseDouble(cmd.getOptionValue("vs30"));
			} else {
				vs30 = getWillsVs30(cmd);
			}
			Preconditions.checkState(Double.isFinite(vs30) && vs30 > 0, "Bad Vs30: %s", vs30);
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
		if (envelopeClasses) {
			metadataEl.addAttribute("siteClass", "Default");
			metadataEl.addAttribute("envelopeSiteClasses", Joiner.on(",").join(siteClassNames));
		} else {
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
	
	private double getWillsVs30(CommandLine cmd) {
		System.out.println("Fetching Vs30 from Wills 2015 map");
		double vs30;
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
			vs30 = wills.getValue(loc);
		} catch (IOException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		System.out.println("Wills 2015 Vs30: "+vs30);
		metadataEl.addAttribute("willsVs30", vs30DF.format(vs30));
		return vs30;
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
		switch (type) {
		case MCER_DET:
		case MCER_DET_LOWER:
			// ASCE 7-22: CyberShake supplies only the probabilistic spectrum, the deterministic parts come from USGS
			return getCalcSpectrum(type, SpectraSource.GMPE).deepClone();
		case MCER:
			return calcMCER(SpectraSource.CYBERSHAKE);
		case MCER_PROB:
			break;
		default:
			throw new IllegalStateException("Cannot calc "+type+" from CyberShake");
		}
		if (csDataDir != null) {
			System.out.println("Calculating CyberShake "+type+" with precomputed data file");
			String fileName = type.getFileName(csDataSpacing);
			File csDataFile = new File(csDataDir, fileName);
			Preconditions.checkState(csDataFile.exists(), "CS data file doesn't exist: %s", csDataFile.getAbsolutePath());
			try {
				GriddedSpectrumInterpolator interp = getInterpolator(csDataFile, csDataSpacing);
				return interp.getInterpolated(loc);
			} catch (Exception e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
		CyberShakeSiteRun site = csRun;
		
		System.out.println("Calculating CyberShake "+type+" on the fly");
		DiscretizedFunc spectrum = new ArbitrarilyDiscretizedFunc();
		
		for (double period : periods) {
			try {
				DiscretizedFunc curve = csProbCalc.calcHazardCurves(site, Lists.newArrayList(period)).get(period);
				spectrum.set(period, CurveBasedMCErProbabilisitCalc.calcRTGM(curve));
			} catch (RuntimeException e) {
				if (e.getMessage() != null && e.getMessage().startsWith("No CyberShake IM match")
						|| e instanceof NullPointerException) {
					System.out.println("Skipping period "+period+", no matching CyberShake IM");
					continue;
				}
				throw e;
			}
		}
		
		return spectrum;
	}
	
	private DiscretizedFunc calcMCER(SpectraSource source) {
		return calcMCER(getCalcSpectrum(SpectraType.MCER_PROB, source), getCalcSpectrum(SpectraType.MCER_DET, source),
				getCalcSpectrum(SpectraType.MCER_DET_LOWER, source));
	}
	
	/**
	 * MCER for one side: min(probabilistic, max(84th percentile, DLL)) at each period with a probabilistic value
	 */
	static DiscretizedFunc calcMCER(DiscretizedFunc prob, DiscretizedFunc det, DiscretizedFunc detLower) {
		DiscretizedFunc mcer = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<prob.size(); i++) {
			double period = prob.getX(i);
			Preconditions.checkState(det.hasX(period) && detLower.hasX(period),
					"No deterministic value at %s s", period);
			mcer.set(period, MCErCalcUtils.calcMCER(det.getY(period), prob.getY(i), detLower.getY(period)));
		}
		return mcer;
	}
	
	private DiscretizedFunc calcGMPE(SpectraType type) {
		if (type == SpectraType.MCER && envelopeClasses) {
			List<DiscretizedFunc> probs = new ArrayList<>(), dets = new ArrayList<>(), detLowers = new ArrayList<>();
			for (String siteClass : siteClassNames) {
				probs.add(getUSGSSpectrum(siteClass, SpectraType.MCER_PROB));
				dets.add(getUSGSSpectrum(siteClass, SpectraType.MCER_DET));
				detLowers.add(getUSGSSpectrum(siteClass, SpectraType.MCER_DET_LOWER));
			}
			return calcDefaultMCER(probs, dets, detLowers);
		}
		if (type == SpectraType.MCER)
			return calcMCER(SpectraSource.GMPE);
		List<DiscretizedFunc> spectra = new ArrayList<>();
		for (String siteClass : siteClassNames)
			spectra.add(getUSGSSpectrum(siteClass, type));
		return combineSiteClasses(spectra, type);
	}

	/**
	 * Default class MCER: each class's MCER, then the per-period maximum, as ASCE 7-22 and the USGS service define it.
	 * Taking the envelope of each ingredient first can overstate it when different classes control P and Det. The
	 * CyberShake side can keep the enveloped 84th and DLL because its probabilistic spectrum does not depend on class.
	 * @param probs, dets, detLowers per-class probabilistic, 84th-percentile and DLL spectra, in the same class order
	 */
	static DiscretizedFunc calcDefaultMCER(List<DiscretizedFunc> probs, List<DiscretizedFunc> dets,
			List<DiscretizedFunc> detLowers) {
		List<DiscretizedFunc> mcers = new ArrayList<>();
		for (int i=0; i<probs.size(); i++)
			mcers.add(calcMCER(probs.get(i), dets.get(i), detLowers.get(i)));
		return calcEnvelope(mcers);
	}

	/**
	 * One class's USGS spectrum of the given type on our periods, which drops T=0 (PGA is handled by PGAM)
	 */
	private DiscretizedFunc getUSGSSpectrum(String siteClass, SpectraType type) {
		USGS_ASCE7_22_Client.Response usgs = getUSGS(siteClass);
		DiscretizedFunc full;
		switch (type) {
		case MCER_PROB:
			full = usgs.getRiskTargetedSpectrum();
			break;
		case MCER_DET:
			full = usgs.getEightyFourthSpectrum();
			break;
		case MCER_DET_LOWER:
			full = usgs.getDetLowerLimitSpectrum();
			break;
		default:
			throw new IllegalStateException("Cannot calc "+type+" from the USGS service");
		}
		DiscretizedFunc spectrum = new ArbitrarilyDiscretizedFunc();
		for (double period : periods) {
			Preconditions.checkState(full.hasX(period), "USGS %s spectrum has no value at %s s", type, period);
			spectrum.set(period, full.getY(period));
		}
		return spectrum;
	}
	
	/**
	 * Combines per-site-class USGS values: a single class is used as is, the Default class is the per-period
	 * envelope, otherwise the two bounding classes are interpolated log-log in Vs30.
	 * @param type spectrum type, recorded in the XML metadata when interpolating, or null
	 */
	private DiscretizedFunc combineSiteClasses(List<DiscretizedFunc> spectra, SpectraType type) {
		if (spectra.size() == 1)
			return spectra.get(0);
		if (envelopeClasses)
			return calcEnvelope(spectra);
		List<Double> vs30Vals = new ArrayList<>();
		for (String siteClass : siteClassNames)
			vs30Vals.add(vs30Map.get(siteClass));
		Element interpEl = null;
		if (type != null) {
			interpEl = xmlRoot.addElement("GMPE_Interpolation");
			interpEl.addAttribute("type", type.name());
		}
		return interpolateSpectraWithVs30(vs30Vals, spectra, userVs30, interpEl);
	}
	
	/**
	 * Per-period maximum of spectra that share the same periods, used for the Default site class
	 */
	static DiscretizedFunc calcEnvelope(List<DiscretizedFunc> spectra) {
		DiscretizedFunc envelope = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<spectra.get(0).size(); i++) {
			double max = 0d;
			for (DiscretizedFunc spectrum : spectra)
				max = Math.max(max, spectrum.getY(i));
			envelope.set(spectra.get(0).getX(i), max);
		}
		return envelope;
	}
	
	/**
	 * Queries the USGS ASCE 7-22 service once per site class, saving the raw reply to the output directory
	 */
	private synchronized USGS_ASCE7_22_Client.Response getUSGS(String siteClass) {
		USGS_ASCE7_22_Client.Response usgs = usgsResponses.get(siteClass);
		if (usgs == null) {
			System.out.println("Querying USGS ASCE 7-22 service: "+usgsClient.buildURL(loc, siteClass));
			try {
				usgs = usgsClient.calculate(loc, siteClass);
				Files.writeString(new File(outputDir, "usgs_asce7-22_"+siteClass+".json").toPath(), usgs.getJson());
			} catch (IOException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			Element usgsEl = xmlRoot.addElement("USGS_ASCE7_22");
			usgsEl.addAttribute("siteClass", siteClass);
			usgsEl.addAttribute("url", usgs.getURL());
			usgsEl.addAttribute("modelVersion", usgs.getModelVersion());
			usgsEl.addAttribute("vs30", vs30DF.format(usgs.getVs30()));
			usgsResponses.put(siteClass, usgs);
		}
		return usgs;
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
		if (type == SpectraType.MCER_DESIGN) {
			DiscretizedFunc designResponseSpectrum = getCalcSpectrum(SpectraType.MCER, SpectraSource.COMBINED);
			designResponseSpectrum = designResponseSpectrum.deepClone();
			designResponseSpectrum.scale(2d/3d);
			designResponseSpectrum.setName(type.getName());
			return designResponseSpectrum;
		}
		// weight average CS and GMPE
		return MCERDataProductsCalc.calcFinalMCER(getCalcSpectrum(type, SpectraSource.CYBERSHAKE),
				getCalcSpectrum(type, SpectraSource.GMPE));
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
	
	/**
	 * Computes a design value and records it in metadata.xml.
	 * @param param the design value
	 * @param type the spectrum it is computed from (MCER, or MCER_DESIGN for SDS and SD1), or null for TL
	 * @param source the spectrum source; PGAM requires {@link SpectraSource#COMBINED}
	 * @return the value, in g (seconds for TL, TS and T0)
	 */
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
		case TS:
			value = calcSX1(getCalcSpectrum(type, source))/calcSXS(getCalcSpectrum(type, source));
			break;
		case T0:
			value = 0.2*getCalcDesignParam(DesignParameter.TS, type, source);
			break;
		case TL:
			// TL depends only on location, so any class gives the same value
			value = getUSGS(siteClassNames.get(0)).getTL();
			break;
		case PGAM:
			Preconditions.checkState(source == SpectraSource.COMBINED, "Only combined supported for PGAM");
			value = calcPGAM();
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
	
	/**
	 * ASCE 7-22 SMS (or SDS): 0.9 x the largest value from 0.2 to 5 s. This rule reproduces the USGS service's
	 * SMS for all site classes tested, but has not been checked against the text of the standard.
	 */
	static double calcSXS(DiscretizedFunc spectrum) {
		double value = 0;
		for (int i=0; i<spectrum.size(); i++) {
			double x = spectrum.getX(i);
			if (x < 0.2)
				continue;
			if (x > 5d)
				break;
			double y = spectrum.getY(i);
			value = Math.max(value, 0.9 * y);
		}
		return value;
	}
	
	/**
	 * ASCE 7-22 SM1 (or SD1): the larger of the 1 s value and 0.9 x the largest (T x value) from 1 to 5 s. This
	 * rule reproduces the USGS service's SM1 for all site classes tested, but has not been checked against the
	 * text of the standard.
	 */
	static double calcSX1(DiscretizedFunc spectrum) {
		double value = 0;
		for (int i=0; i<spectrum.size(); i++) {
			double x = spectrum.getX(i);
			if (x < 1d)
				continue;
			if (x > 5d)
				break;
			double y = spectrum.getY(i);
			if ((float)x == 1f)
				value = Math.max(value, y);
			value = Math.max(value, 0.9 * x * y);
		}
		return value;
	}
	
	private double calcPGAM() {
		List<DiscretizedFunc> pgas = new ArrayList<>();
		for (String siteClass : siteClassNames) {
			DiscretizedFunc fakeSpectrum = new ArbitrarilyDiscretizedFunc();
			fakeSpectrum.set(0d, getUSGS(siteClass).getPGAM());
			pgas.add(fakeSpectrum);
		}
		return combineSiteClasses(pgas, null).getY(0);
	}
	
	/**
	 * Runs the calculation and writes the outputs to the output directory: the final MCER, design and ingredients
	 * plots (PNG and PDF), mcer_sa.csv, each raw USGS reply, and the design values in the XML metadata.
	 * @throws IOException if an output file cannot be written
	 */
	public void plot() throws IOException {
		plot("mcer_sa_final",
				new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f));
		plot("mcer_sa_design_final",
				new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER, Color.BLACK, PlotLineType.SOLID, 4f),
				new SpectrumPlotElem(SpectraSource.COMBINED, SpectraType.MCER_DESIGN, Color.RED, PlotLineType.SOLID, 3f));
		plot("mcer_sa_ingredients",
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
		
		/**
		 * One spectrum to draw in a plot.
		 * @param source spectrum source
		 * @param type spectrum type
		 * @param color line colour
		 * @param line line style
		 * @param thickness line width
		 */
		public SpectrumPlotElem(SpectraSource source, SpectraType type, Color color, PlotLineType line, double thickness) {
			this.source = source;
			this.type = type;
			this.color = color;
			this.line = line;
			this.thickness = thickness;
		}
	}
	
	/**
	 * Plots the given spectra (Sa against period, linear axes) and saves them as PDF and PNG.
	 * @param prefix output file name without extension
	 * @param spectra the spectra to plot, in drawing order
	 * @throws IOException if a file cannot be written
	 */
	public void plot(String prefix, SpectrumPlotElem... spectra) throws IOException {
		Preconditions.checkState(spectra != null && spectra.length > 0);
		boolean xLog = false;
		boolean yLog = false;
		Range xRange = new Range(1e-2, 10d);
		Range yRange = null;
		String yAxisLabel = "Sa (g)";

		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		for (SpectrumPlotElem spectrumPlot : spectra) {
			DiscretizedFunc spectrum = getCalcSpectrum(spectrumPlot.type, spectrumPlot.source);
			
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
	
	/**
	 * Writes metadata.xml, which records every input, intermediate spectrum and design value, to the output directory. Call after {@link #plot()}.
	 * @throws IOException if the file cannot be written
	 */
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
				"ASCE 7-22 site class (A, B, BC, C, CD, D, DE, E or Default). If neither this or vs30 is specified, Wills value will be used");
		siteClass.setRequired(false);
		ops.addOption(siteClass);
		
		Option vs30 = new Option("v", "vs30", true,
				"Vs30 (m/s). If neither this or site class is specified, Wills value will be used");
		vs30.setRequired(false);
		ops.addOption(vs30);
		
		Option csData = new Option("csdata", "cs-data-dir", true, "CS precomputed data dir");
		csData.setRequired(false);
		ops.addOption(csData);
		
		Option csSpacing = new Option("csspacing", "cs-spacing", true, "Grid spacing of CS precomputed data file");
		csSpacing.setRequired(false);
		ops.addOption(csSpacing);
		
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
		
		return ops;
	}
	
	/**
	 * Prints the full command-line help and exits with status 2.
	 * @param options the command-line options
	 * @param appName the program name to show
	 */
	public static void printHelp(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( appName, options, true );
		System.exit(2);
	}
	
	/**
	 * Prints a short usage summary and exits with status 2.
	 * @param options the command-line options
	 * @param appName the program name to show
	 */
	public static void printUsage(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter pw = new PrintWriter(System.out);
		formatter.printUsage(pw, 80, appName, options);
		pw.flush();
		System.exit(2);
	}

	/**
	 * Command-line entry point for the ASCE 7-22 UGMS calculation. Pass {@code --hardcoded} alone to run a built-in
	 * Tarzana test case with local data paths. Exits with status 0 on success, 1 on error and 2 for bad arguments.
	 * @param args command-line arguments; run with {@code --help} for the list
	 */
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
			argStr += " --cs-data-dir "+new File(dataDir, "cs_study_15_4");
			argStr += " --cs-spacing 0.002";
			argStr += " --vel-model-id 5";
			argStr += " --z10-file "+new File(siteDataDir, "CVM4i26_depth_1.0.bin");
			argStr += " --z25-file "+new File(siteDataDir, "CVM4i26_depth_2.5.bin");
//			argStr += " --vs30 260";
			argStr += " --class D";
//			argStr += " --wills-file /data/kevin/opensha/wills2015.flt";
			argStr += " --wills-file "+new File(siteDataDir, "wills_2015_vs30.flt");
			argStr += " --wills-header "+new File(siteDataDir, "wills_2015_vs30.hdr");
			
			argStr += " --output-dir /tmp/ugms_web_tool_asce7_22";
			
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
