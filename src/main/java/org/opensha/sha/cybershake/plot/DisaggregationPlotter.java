package org.opensha.sha.cybershake.plot;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dom4j.DocumentException;
import org.jfree.chart3d.Chart3DPanel;
import org.jfree.chart3d.export.ExportUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.disaggregation.DisaggregationCalculator;
import org.opensha.sha.calc.disaggregation.chart3d.PureJavaDisaggPlotter;
import org.opensha.sha.calc.hazardMap.HazardCurveSetCalculator;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.gui.util.AttenRelSaver;
import org.opensha.sha.cybershake.gui.util.ERFSaver;
import org.opensha.sha.cybershake.openshaAPIs.CyberShakeIMR;
import org.opensha.sha.cybershake.openshaAPIs.CyberShakeWrapper_ERF;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.gui.infoTools.DisaggregationPlotViewerWindow;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * CyberShake disaggregation driver and command line tool.
 * <p>
 * The disaggregation mathematics (binning by magnitude, distance and epsilon, mean/mode, per-source contributions) is
 * done entirely by {@link DisaggregationCalculator}. This class supplies everything CyberShake-specific that the
 * calculator knows nothing about:
 * <ul>
 * <li><b>Run, site and ERF set-up from the CyberShake database:</b> looks up the run ({@link Runs2DB}), its site
 * ({@link SiteInfo2DB}), builds the {@link Site} with {@link CyberShakeSiteBuilder} (Wills 2015 Vs30 and the run's
 * velocity model, optionally overridden by {@code --force-vs30}) and wraps the ERF in a
 * {@link CyberShakeWrapper_ERF} bound to the run's ERF ID.</li>
 * <li><b>Simulation-based "IMR":</b> the calculator is driven by a {@link CyberShakeIMR}, which reads simulated peak
 * amplitudes from the database ({@link PeakAmplitudesFromDB}, cached) rather than evaluating a GMPE, so the result is a
 * disaggregation of CyberShake simulation results.</li>
 * <li><b>Hazard curve handling:</b> fetches the stored hazard curve for each intensity measure, or computes one
 * ({@link HazardCurveComputation}) if none exists, and converts requested probabilities of exceedance ({@code --probs})
 * into intensity measure levels by log-log interpolation, skipping probabilities outside the curve's range.</li>
 * <li><b>GMPE comparison:</b> optionally repeats the disaggregation for one or more ground motion models
 * ({@code --atten-rel-file}) at the same probability, recomputing the IML from each GMPE's own hazard curve.</li>
 * <li><b>Output conventions:</b> file names of the form
 * {@code <SITE>_ERF<id>_Run<id>_DisaggPOE_<prob>_SA_<period>sec_<component>_<curve date>}, a CyberShake metadata header
 * ({@link HazardCurvePlotter#getCyberShakeCurveInfo}) and mean/mode text, written as PDF, PNG, JPG and/or TXT
 * ({@link PlotType}).</li>
 * <li><b>Command line interface:</b> {@link #main(String[])} shares its common options with {@link HazardCurvePlotter}.</li>
 * </ul>
 * Plots are rendered client-side with {@link PureJavaDisaggPlotter}; no server or GMT installation is needed. (This class
 * previously fetched GMT-rendered images from the OpenSHA {@code DisaggregationPlotServlet}.)
 */
public class DisaggregationPlotter {
	
	public static final PlotType TYPE_DEFAULT = PlotType.PDF;
	
	private DBAccess db;
	private Runs2DB runs2db;
	private HazardCurve2DB curve2db;
	private PeakAmplitudesFromDB amps2db;
	private SiteInfo2DB site2db;
	private HazardCurveComputation curveCalc;
	
	private CybershakeRun run;
	private List<CybershakeIM> ims;
	
	private List<Double> probLevels;
	private List<Double> imlLevels;
	
	private CybershakeSite csSite;
	private Site site;
	
	private CyberShakeWrapper_ERF erf;
	private CyberShakeIMR imr;
	
	private List<AttenuationRelationship> gmpeComparisons;
	private double forceVs30 = Double.NaN;
	
	private HazardCurveCalculator gmpeCurveCalc;
	private DisaggregationCalculator disaggCalc;
	private ParameterList disaggParams;
	
	private File outputDir;
	
	// disagg plot settings
	private double minMag = 5;
	private int numMags = 10;
	private double deltaMag = 0.5;
	
	private int numSourcesForDisag = 100;
	
	private boolean showSourceDistances = true;
	
	private double maxZAxis = Double.NaN;
	
	private List<PlotType> plotTypes;
	
	private HashMap<Double, Double> imlToProbsMap;
	
	/**
	 * Creates a plotter that disaggregates the given CyberShake run, using a new (uncached-until-needed) peak amplitude
	 * accessor on the given database.
	 * 
	 * @param db database connection, or null to use {@link Cybershake_OpenSHA_DBApplication#getDB()}
	 * @param runID CyberShake run ID to disaggregate
	 * @param rawERF the (unwrapped) ERF; it is wrapped in a {@link CyberShakeWrapper_ERF} for the run's ERF ID
	 * @param ims intensity measures (periods/components) to disaggregate; must be non-empty
	 * @param gmpes optional GMPEs to also disaggregate for comparison, or null for none
	 * @param probLevels annual probabilities of exceedance to disaggregate at (converted to IMLs via the hazard curve), may be null
	 * @param imlLevels intensity measure levels to disaggregate at (linear units, e.g. g), may be null
	 * @param outputDir directory that plots/text are written to
	 * @param plotTypes output formats to write
	 */
	public DisaggregationPlotter(
			DBAccess db,
			int runID,
			AbstractERF rawERF,
			List<CybershakeIM> ims,
			List<AttenuationRelationship> gmpes,
			List<Double> probLevels,
			List<Double> imlLevels,
			File outputDir,
			List<PlotType> plotTypes) {
		this(db, null, runID, rawERF, ims, gmpes, probLevels, imlLevels, outputDir, plotTypes);
	}
	
	/**
	 * Same as {@link #DisaggregationPlotter(DBAccess, int, AbstractERF, List, List, List, List, File, List)}, but allows
	 * sharing an existing peak amplitude accessor (e.g. a {@link CachedPeakAmplitudesFromDB}) across many plotters.
	 * 
	 * @param db database connection, or null to use {@link Cybershake_OpenSHA_DBApplication#getDB()}
	 * @param amps2db peak amplitude accessor, or null to create one from {@code db}
	 * @param runID CyberShake run ID to disaggregate
	 * @param rawERF the (unwrapped) ERF
	 * @param ims intensity measures to disaggregate; must be non-empty
	 * @param gmpes optional GMPEs to also disaggregate for comparison, or null for none
	 * @param probLevels annual probabilities of exceedance to disaggregate at, may be null
	 * @param imlLevels intensity measure levels to disaggregate at, may be null
	 * @param outputDir directory that plots/text are written to
	 * @param plotTypes output formats to write
	 */
	public DisaggregationPlotter(
			DBAccess db,
			PeakAmplitudesFromDB amps2db,
			int runID,
			AbstractERF rawERF,
			List<CybershakeIM> ims,
			List<AttenuationRelationship> gmpes,
			List<Double> probLevels,
			List<Double> imlLevels,
			File outputDir,
			List<PlotType> plotTypes) {
		initDB(db, amps2db);
		init(runID, rawERF, ims, gmpes, probLevels, imlLevels, outputDir, plotTypes);
	}
	
	/**
	 * Creates a plotter from parsed command line arguments (see {@link #main(String[])}). Connects to the default
	 * CyberShake database, loads the ERF from {@code --erf-file}, and reads the run, IMs, probabilities/IMLs, output
	 * directory, plot types, GMPE comparison files and Vs30 override from the options.
	 * 
	 * @param cmd parsed command line
	 */
	public DisaggregationPlotter(CommandLine cmd) {
		initDB(null, null);
		init(cmd);
	}
	
	private void initDB(DBAccess db, PeakAmplitudesFromDB amps2db) {
		if (db == null)
			db = Cybershake_OpenSHA_DBApplication.getDB();
		this.db = db;
		runs2db = new Runs2DB(db);
		curve2db = new HazardCurve2DB(db);
		if (amps2db == null)
			amps2db = new PeakAmplitudesFromDB(db);
		this.amps2db = amps2db;
		site2db = new SiteInfo2DB(db);
	}
	
	/**
	 * Initializes this plotter from command line options. Resolves the run ID (directly or from site/ERF options), the IMs,
	 * the requested probabilities and IMLs, the output directory (created if missing), plot types (default
	 * {@link #TYPE_DEFAULT}), optional GMPE comparison files and Vs30 override, and loads the ERF, then delegates to
	 * {@link #init(int, AbstractERF, List, List, List, List, File, List)}.
	 * 
	 * @param cmd parsed command line
	 * @throws IllegalArgumentException if {@code --erf-file} is missing or does not exist
	 * @throws RuntimeException if the output directory cannot be created or an ERF/GMPE file cannot be loaded
	 */
	public void init(CommandLine cmd) {
		int runID = HazardCurvePlotter.getRunIDFromOptions(runs2db, curve2db, amps2db, site2db, cmd);
		this.run = runs2db.getRun(runID);
		List<CybershakeIM> ims = HazardCurvePlotter.getIMsFromOptions(cmd, run, curve2db, amps2db);
		List<Double> probLevels = null;
		if (cmd.hasOption("probs"))
			probLevels = DataUtils.commaDoubleSplit(cmd.getOptionValue("probs"));
		List<Double> imlLevels = null;
		if (cmd.hasOption("imls"))
			imlLevels = DataUtils.commaDoubleSplit(cmd.getOptionValue("imls"));
		
		File outputDir;
		if (cmd.hasOption("o")) {
			String outDirStr = cmd.getOptionValue("o");
			outputDir = new File(outDirStr);
			if (!outputDir.exists()) {
				boolean success = outputDir.mkdir();
				if (!success) {
					throw new RuntimeException("Directory doesn't exist and couldn't be created: " + outDirStr);
				}
			}
		} else {
			outputDir = new File("").getAbsoluteFile();
		}
		
		List<PlotType> plotTypes;
		if (cmd.hasOption("t")) {
			String typeStr = cmd.getOptionValue("t");
			
			plotTypes = PlotType.fromExtensions(DataUtils.commaSplit(typeStr));
		} else {
			plotTypes = new ArrayList<PlotType>();
			plotTypes.add(PlotType.PDF);
		}
		
		List<AttenuationRelationship> gmpeComparisons = null;
		if (cmd.hasOption("atten-rel-file")) {
			gmpeComparisons = Lists.newArrayList();
			
			String attenFiles = cmd.getOptionValue("af");
			
			for (String attenRelFile : DataUtils.commaSplit(attenFiles)) {
				AttenuationRelationship attenRel;
				try {
					attenRel = AttenRelSaver.LOAD_ATTEN_REL_FROM_FILE(attenRelFile);
				} catch (Exception e) {
					throw ExceptionUtils.asRuntimeException(
							new RuntimeException("Error loading IMR from "+attenRelFile, e));
				}
				gmpeComparisons.add(attenRel);
			}
		}
		
		if (cmd.hasOption("force-vs30"))
			forceVs30 = Double.parseDouble(cmd.getOptionValue("force-vs30"));
		
		Preconditions.checkArgument(cmd.hasOption("erf-file"), "Must supply ERF file argument!");
		File erfFile = new File(cmd.getOptionValue("erf-file"));
		Preconditions.checkArgument(erfFile.exists(), "ERF file doesn't exist: %s", erfFile.getAbsolutePath());
		AbstractERF rawERF;
		try {
			rawERF = ERFSaver.LOAD_ERF_FROM_FILE(erfFile.getAbsolutePath());
		} catch (Exception e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		init(runID, rawERF, ims, gmpeComparisons, probLevels, imlLevels, outputDir, plotTypes);
	}
	
	/**
	 * Initializes the run, site, CyberShake ERF and IMR, and the disaggregation calculator and its default parameters.
	 * The site is built with Wills 2015 Vs30 and the run's velocity model; the intensity measure type is fixed to SA
	 * (the period is set per IM in {@link #disaggregate()}).
	 * 
	 * @param runID CyberShake run ID
	 * @param rawERF the (unwrapped) ERF
	 * @param ims intensity measures to disaggregate; must be non-null and non-empty
	 * @param gmpeComparisons optional GMPEs to disaggregate for comparison, or null
	 * @param probLevels annual probabilities of exceedance, or null for none
	 * @param imlLevels intensity measure levels, or null for none
	 * @param outputDir directory that plots/text are written to
	 * @param plotTypes output formats to write
	 * @throws NullPointerException if the run cannot be found in the database or {@code ims} is null
	 */
	public void init(
			int runID,
			AbstractERF rawERF,
			List<CybershakeIM> ims,
			List<AttenuationRelationship> gmpeComparisons,
			List<Double> probLevels,
			List<Double> imlLevels,
			File outputDir,
			List<PlotType> plotTypes) {
		// get the full run description from the DB
		this.run = runs2db.getRun(runID);
		Preconditions.checkNotNull(run, "Error fetching runs from DB");
		
		this.plotTypes = plotTypes;
		
		this.ims = ims;
		Preconditions.checkNotNull(ims, "Error fetching IMs from DB");
		Preconditions.checkState(!ims.isEmpty(), "must have at least 1 IM");
		if (probLevels == null)
			probLevels = new ArrayList<Double>();
		this.probLevels = probLevels;
		if (imlLevels == null)
			imlLevels = new ArrayList<Double>();
		this.imlLevels = imlLevels;
		this.csSite = site2db.getSiteFromDB(run.getSiteID());
		CyberShakeSiteBuilder siteBuilder = new CyberShakeSiteBuilder(Vs30_Source.Wills2015, run.getVelModelID());
		if (!Double.isNaN(forceVs30))
			siteBuilder.setForceVs30(forceVs30);
		this.site = siteBuilder.buildSite(run, csSite);
		
		erf = new CyberShakeWrapper_ERF(run.getERFID(), rawERF);
		erf.updateForecast();
		if (!(amps2db instanceof CachedPeakAmplitudesFromDB))
			amps2db = new CachedPeakAmplitudesFromDB(db, null, rawERF);
		imr = new CyberShakeIMR(null, db, amps2db);
		imr.setParamDefaults();
		
		imr.setSite(site);
		
		imr.setRunID(runID);
		
		// now set the IMR params
		// hard code the IMT to SA. the period gets set later
		imr.setIntensityMeasure(SA_Param.NAME);
		
		this.gmpeComparisons = gmpeComparisons;
		
		disaggCalc = new DisaggregationCalculator();
		gmpeCurveCalc = new HazardCurveCalculator();
		
		disaggParams = DisaggregationCalculator.getDefaultParams();
		
		// absolute, as new File(new File(""), name) would resolve against the root directory rather than the working directory
		this.outputDir = outputDir.getAbsoluteFile();
	}
	
	/**
	 * Sets the magnitude binning used for disaggregation (defaults: 5.0, 10 bins, 0.5 width).
	 * 
	 * @param minMag lower edge of the first magnitude bin
	 * @param numMags number of magnitude bins
	 * @param deltaMag magnitude bin width
	 */
	public void setMagRange(double minMag, int numMags, double deltaMag) {
		this.minMag = minMag;
		this.numMags = numMags;
		this.deltaMag = deltaMag;
	}
	
	/**
	 * Runs the disaggregation for every configured IM and IML/probability and writes the requested outputs to the output
	 * directory. For each IM it fetches (or computes) the hazard curve, converts probabilities to IMLs, disaggregates with
	 * the CyberShake IMR, builds the plot with {@link PureJavaDisaggPlotter} (skipped if only TXT output was requested), and
	 * saves it. If GMPE comparisons were configured, each GMPE is then disaggregated at the same probability and written with
	 * the GMPE's short name appended to the file name.
	 * 
	 * @throws IOException if an output file cannot be written
	 * @throws RuntimeException if a disaggregation fails or the hazard curve cannot be obtained
	 */
	public void disaggregate() throws IOException {
		
		for (CybershakeIM im : ims) {
			imr.setIM(im);
			
			System.out.println("IMR Metadata: "+imr.getAllParamMetadata());
			
			int curveID = curve2db.getHazardCurveID(run.getRunID(), im.getID());
			System.out.println("Curve ID: "+curveID);
			DiscretizedFunc curve;
			if (curveID < 0) {
				// need to calculate it
				if (curveCalc == null)
					curveCalc = new HazardCurveComputation(db);
				
				ArbitrarilyDiscretizedFunc func = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
				ArrayList<Double> imVals = new ArrayList<Double>();
				for (int i=0; i<func.size(); i++)
					imVals.add(func.getX(i));
				
				System.out.println("Calculating Hazard Curve for "+im.getVal()+" s SA, "+im.getComponent());
				curve = curveCalc.computeHazardCurve(imVals, run, im);
			} else {
				// already in the database
				System.out.println("Fetching precalculated curve from the database");
				curve = curve2db.getHazardCurve(curveID);
			}
			
			imlToProbsMap = new HashMap<Double, Double>();
			List<Double> myIMLevels = Lists.newArrayList(imlLevels);
			// convert prob values to IMLs
			for (double probLevel : probLevels) {
				if (probLevel > curve.getY(0)
						|| probLevel < curve.getY(curve.size() - 1)) {
					System.err.println("Cannot produce plot at prob="+probLevel+" as it is outside of" +
							" the range of the hazard curve");
					continue;
				}
				double imLevel = curve.getFirstInterpolatedX_inLogXLogYDomain(probLevel);
				System.out.println("converted prob of: "+probLevel+" to IML of: "+imLevel);
				imlToProbsMap.put(imLevel, probLevel);
				myIMLevels.add(imLevel);
			}
			
			// set up GMPE comparisons
			if (gmpeComparisons != null && !gmpeComparisons.isEmpty()) {
				for (AttenuationRelationship attenRel : gmpeComparisons)
					for (Parameter<?> siteParam : attenRel.getSiteParams())
						if (!site.containsParameter(siteParam))
							site.addParameter((Parameter<?>)siteParam.clone());
				for (AttenuationRelationship attenRel : gmpeComparisons)
					HazardCurvePlotter.setAttenRelParams(attenRel, im);
			}
			
			for (double iml : myIMLevels) {
				System.out.println("Disaggregating");
				disaggCalc.setMagRange(minMag, numMags, deltaMag);
				disaggCalc.setNumSourcesToShow(numSourcesForDisag);
				disaggCalc.setShowDistances(showSourceDistances);
				boolean success = disaggCalc.disaggregate(Math.log(iml), site, imr, erf, gmpeCurveCalc.getSourceFilters(), disaggParams);
				if (!success)
					throw new RuntimeException("Disagg calc failed (see errors above, if any).");
				disaggCalc.setMaxZAxisForPlot(maxZAxis);
				System.out.println("Done Disaggregating");
				try {
					boolean textOnly = plotTypes.size() == 1 && plotTypes.get(0) == PlotType.TXT;
					
					Chart3DPanel chartPanel = null;

					if (!textOnly) {
						System.out.println("Building plot...");
						chartPanel = PureJavaDisaggPlotter.buildChartPanel(disaggCalc.getDisaggPlotData());
					}
					
					boolean isProb = imlToProbsMap.containsKey(iml);
					double prob;
					if (isProb)
						prob = imlToProbsMap.get(iml);
					else
						prob = curve.getInterpolatedY_inLogXLogYDomain(iml);
					
					Date date;
					if (curveID < 0)
						date = new Date(); // now
					else
						date = curve2db.getDateForCurve(curveID);
					String dateStr = HazardCurvePlotter.dateFormat.format(date);
					String periodStr = "SA_" + HazardCurvePlotter.getPeriodStr(im.getVal()) + "sec";
					String outFileName = csSite.short_name + "_ERF" + run.getERFID() + "_Run" + run.getRunID();
					if (isProb)
						outFileName += "_DisaggPOE_"+(float)prob;
					else
						outFileName += "_DisaggIML_"+(float)iml+"_G";
					outFileName += "_"+periodStr+"_"+im.getComponent().getShortName()+"_"+dateStr;
					
					String meanModeHeader = getMeanModeHeader(iml, isProb, prob);
					
					String meanModeText = meanModeHeader+disaggCalc.getMeanAndModeInfo();
					
					CybershakeVelocityModel velModel = runs2db.getVelocityModel(run.getVelModelID());
					String metadataText = HazardCurvePlotter.getCyberShakeCurveInfo(curveID, csSite, run, velModel, im,
							null, null, null, null);
					String binDataText = disaggCalc.getBinData();
					String sourceDataText = disaggCalc.getDisaggregationSourceInfo();
					
					for (PlotType type : plotTypes)
						savePlot(chartPanel, outFileName, meanModeText, metadataText, binDataText, sourceDataText, type);
					
					if (gmpeComparisons != null) {
						for (AttenuationRelationship attenRel : gmpeComparisons) {
							System.out.println("Calculating GMPE comparison: "+attenRel.getShortName());
							if (isProb) {
								// need to calculate the correct IML from the GMPE curve
								DiscretizedFunc gmpeCurve = curve.deepClone();
								gmpeCurve = HazardCurveSetCalculator.getLogFunction(gmpeCurve);
								gmpeCurveCalc.getHazardCurve(gmpeCurve, site, attenRel, erf);
								gmpeCurve = HazardCurveSetCalculator.unLogFunction(curve, gmpeCurve);
								iml = gmpeCurve.getFirstInterpolatedX_inLogXLogYDomain(prob);
							}
							success = disaggCalc.disaggregate(Math.log(iml), site, attenRel, erf, gmpeCurveCalc.getSourceFilters(), disaggParams);
							if (!success)
								throw new RuntimeException("Disagg calc failed (see errors above, if any).");
							disaggCalc.setMaxZAxisForPlot(maxZAxis);
							
							if (!textOnly) {
								System.out.println("Building plot...");
								chartPanel = PureJavaDisaggPlotter.buildChartPanel(disaggCalc.getDisaggPlotData());
							}
							
							String gmpeOutFileName = outFileName+"_"+attenRel.getShortName();
							String gmpeMeanModeHeader = getMeanModeHeader(iml, isProb, prob);
							String gmpeMeanModeText = gmpeMeanModeHeader+disaggCalc.getMeanAndModeInfo();
							String gmpeMetadataText = HazardCurvePlotter.getCurveParametersInfoAsString(
									attenRel, erf, site,
									gmpeCurveCalc.getSourceFilterManager().getMaxDistance());
							String gmpeBinDataText = disaggCalc.getBinData();
							String gmpeSourceDataText = disaggCalc.getDisaggregationSourceInfo();
							
							for (PlotType type : plotTypes)
								savePlot(chartPanel, gmpeOutFileName, gmpeMeanModeText, gmpeMetadataText,
										gmpeBinDataText, gmpeSourceDataText, type);
						}
					}
					
					System.out.println("DONE.");
				} catch (Exception e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			}
		}
	}

	private String getMeanModeHeader(double iml, boolean isProb, double prob) {
		String meanModeHeader;
		if (isProb)
			meanModeHeader = "Disaggregation Results for Prob = " + prob
			+ " (for IML = " + (float) iml + ")";
		else
			meanModeHeader = "Disaggregation Results for IML = " + iml
			+ " (for Prob = " + (float) prob + ")";
		return meanModeHeader;
	}

	private void savePlot(Chart3DPanel chartPanel, String outFileName, String meanModeText, String metadataText,
			String binDataText, String sourceDataText, PlotType type) throws IOException {
		if (type == PlotType.PDF) {
			String outputFileName = outputDir.getAbsolutePath()+File.separator+outFileName+".pdf";
			DisaggregationPlotViewerWindow.saveAsPDF(chartPanel,
					outputFileName, meanModeText, metadataText, binDataText, sourceDataText, null);
		} else if (type == PlotType.PNG) {
			System.out.println("Writing disagg png plot to: "+outputDir.getAbsolutePath()+File.separator+outFileName+".png");
			PureJavaDisaggPlotter.writeChartPlot(outputDir, outFileName, chartPanel, true, false);
		} else if (type == PlotType.JPG || type == PlotType.JPEG) {
			File outFile = new File(outputDir, outFileName+".jpg");
			System.out.println("Writing disagg jpg plot to: "+outFile.getAbsolutePath());
			ExportUtils.writeAsJPEG(chartPanel.getDrawable(), chartPanel.getWidth(), chartPanel.getHeight(), outFile);
		} else if (type == PlotType.TXT) {
			String outputFileName = outputDir.getAbsolutePath()+File.separator+outFileName+".txt";
			DisaggregationPlotViewerWindow.saveAsTXT(outputFileName, meanModeText, metadataText,
					binDataText, sourceDataText, null);
		} else {
			throw new IllegalArgumentException("Unknown plot type: "+type);
		}
	}

	/**
	 * This creates the apache cli options
	 * 
	 * @return
	 */
	private static Options createOptions() {
		Options ops = HazardCurvePlotter.createCommonOptions();
		
		for (Option op : ops.getOptions())
			if (op.getLongOpt().equals("erf-file"))
				op.setRequired(true);
		
		Option probs = new Option("pr", "probs", true, "Probabilities (1 year) to disaggregate at. " +
				"Multiple probabilities should be comma separated.");
		probs.setRequired(false);
		ops.addOption(probs);
		
		Option imls = new Option("i", "imls", true, "Intensity Measure Levels (IMLs) to disaggregate at. " +
		"Multiple IMLs should be comma separated.");
		imls.setRequired(false);
		ops.addOption(imls);
		
		Option type = new Option("t", "type", true, "Plot save type. Options are png, pdf, and txt. Multiple types can be " + 
				"comma separated (default is " + TYPE_DEFAULT.getExtension() + ")");
		type.setRequired(false);
		ops.addOption(type);
		
		Option forceVs30 = new Option("fvs", "force-vs30", true, "Option to force the given Vs30 value to be used"
				+ " in GMPE calculations.");
		ops.addOption(forceVs30);
		
		return ops;
	}

	/**
	 * Command line entry point. Run with {@code --help} for all options. Key options: {@code --run-id}, {@code --period},
	 * {@code --component}, {@code --probs}, {@code --imls}, {@code --erf-file} (required), {@code --atten-rel-file},
	 * {@code --type} (pdf, png, jpg, txt; comma separated), {@code --force-vs30} and {@code -o} (output directory).
	 * With the single argument {@code --hardcoded}, a built-in developer configuration is used.
	 * 
	 * @param args command line arguments
	 * @throws DocumentException if an ERF XML file cannot be parsed
	 * @throws InvocationTargetException if constructing an ERF from its XML metadata fails
	 */
	public static void main(String args[]) throws DocumentException, InvocationTargetException {
		if (args.length == 1 && args[0].equals("--hardcoded")) {
//			String[] newArgs = {"-R", "5760", "-p", "3,5,10", "-pr", "0.002,4.0e-4", "-o", "/tmp", "-t", "png",
//					"-ef", "/home/kevin/workspace/opensha-cybershake/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml",
//					"--force-vs30", "760"};
			//	String[] newArgs = {"-R", "3970", "-p", "5", "--component", CyberShakeComponent.RotD100.getShortName(), "-pr", "4.0e-4",
			//			"-o", "/tmp", "-t", "png", "-ef", "/home/kevin/workspace/opensha-cybershake/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml"};
			////	String[] newArgs = {"--help"};
			//	String[] newArgs = {"-R", "792", "-p", "3", "-pr", "4.0e-4", "-t", "pdf", "-o", "/tmp", "--skip-site-params"};
			String[] newArgs = {"--run-id", "4384", "--component", "RotD50", "--period", "4", "--probs", "1e-4",
					"--erf-file", "/home/kevin/workspace/opensha-cybershake/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml",
					"--type", "txt", "-t", "/tmp"};
			//	Cybershake_OpenSHA_DBApplication.HOST_NAME = Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME;
			args = newArgs;
		}
		
		try {
			Options options = createOptions();
			
			String appName = ClassUtils.getClassNameWithoutPackage(DisaggregationPlotter.class);
			
			CommandLineParser parser = new GnuParser();
			
			if (args.length == 0) {
				HazardCurvePlotter.printUsage(options, appName);
			}
			
			try {
				CommandLine cmd = parser.parse( options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					HazardCurvePlotter.printHelp(options, appName);
				}
				
				DisaggregationPlotter disagg = new DisaggregationPlotter(cmd);
				disagg.disaggregate();
			} catch (MissingOptionException e) {
				// TODO Auto-generated catch block
				Options helpOps = new Options();
				helpOps.addOption(new Option("h", "help", false, "Display this message"));
				try {
					CommandLine cmd = parser.parse( helpOps, args);
					
					if (cmd.hasOption("help")) {
						HazardCurvePlotter.printHelp(options, appName);
					}
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
//				e1.printStackTrace();
				}
				System.err.println(e.getMessage());
				HazardCurvePlotter.printUsage(options, appName);
//			e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				HazardCurvePlotter.printUsage(options, appName);
			}
			
			System.out.println("Done!");
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
}
