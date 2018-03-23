package org.opensha.sha.cybershake.plot;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
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
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.ListUtils;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.disaggregation.DisaggregationCalculator;
import org.opensha.sha.calc.hazardMap.HazardCurveSetCalculator;
import org.opensha.sha.calc.params.IncludeMagDistFilterParam;
import org.opensha.sha.calc.params.MagDistCutoffParam;
import org.opensha.sha.calc.params.MaxDistanceParam;
import org.opensha.sha.calc.params.NonSupportedTRT_OptionsParam;
import org.opensha.sha.calc.params.PtSrcDistanceCorrectionParam;
import org.opensha.sha.calc.params.SetTRTinIMR_FromSourceParam;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
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
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.util.SiteTranslator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
	
	public DisaggregationPlotter(
			int runID,
			AbstractERF rawERF,
			List<CybershakeIM> ims,
			List<AttenuationRelationship> gmpes,
			List<Double> probLevels,
			List<Double> imlLevels,
			File outputDir,
			List<PlotType> plotTypes) {
		initDB();
		init(runID, rawERF, ims, gmpes, probLevels, imlLevels, outputDir, plotTypes);
	}
	
	public DisaggregationPlotter(CommandLine cmd) {
		initDB();
		init(cmd);
	}
	
	private void initDB() {
		db = Cybershake_OpenSHA_DBApplication.getDB();
		runs2db = new Runs2DB(db);
		curve2db = new HazardCurve2DB(db);
		amps2db = new PeakAmplitudesFromDB(db);
		site2db = new SiteInfo2DB(db);
	}
	
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
			outputDir = new File("");
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
			
			if (cmd.hasOption("force-vs30"))
				forceVs30 = Double.parseDouble(cmd.getOptionValue("force-vs30"));
		}
		
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
		this.site = new Site(csSite.createLocation());
		
		erf = new CyberShakeWrapper_ERF(run.getERFID(), rawERF);
		erf.updateForecast();
		imr = new CyberShakeIMR(null, db, new CachedPeakAmplitudesFromDB(db, null, rawERF));
		imr.setParamDefaults();
		
		imr.setSite(site);
		
		imr.setRunID(runID);
		
		// now set the IMR params
		// hard code the IMT to SA. the period gets set later
		imr.setIntensityMeasure(SA_Param.NAME);
		
		this.gmpeComparisons = gmpeComparisons;
		
		disaggCalc = new DisaggregationCalculator();
		gmpeCurveCalc = new HazardCurveCalculator();
		
		disaggParams = new ParameterList();
		disaggParams.addParameter(new MaxDistanceParam());
		disaggParams.addParameter(new IncludeMagDistFilterParam());
		disaggParams.addParameter(new MagDistCutoffParam());
		disaggParams.addParameter(new SetTRTinIMR_FromSourceParam());
		disaggParams.addParameter(new NonSupportedTRT_OptionsParam());
		disaggParams.addParameter(new PtSrcDistanceCorrectionParam());
		
		this.outputDir = outputDir;
	}
	
	public void disaggregate() throws IOException {
		
		List<SiteDataValue<?>> datas = null; // for comparisons if needed
		
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
				if (datas == null) {
					OrderedSiteDataProviderList provs = HazardCurvePlotter.createProviders(run.getVelModelID());
					datas = provs.getBestAvailableData(csSite.createLocation());
					if (!Double.isNaN(forceVs30)) {
						for (int i=datas.size(); --i>=0;)
							if (datas.get(i).getDataType() == SiteData.TYPE_VS30)
								datas.remove(i);
						datas.add(0, new SiteDataValue<Double>(SiteData.TYPE_VS30,
								SiteData.TYPE_FLAG_INFERRED, forceVs30));
					}
					for (AttenuationRelationship attenRel : gmpeComparisons)
						for (Parameter<?> siteParam : attenRel.getSiteParams())
							if (!site.containsParameter(siteParam))
								site.addParameter((Parameter<?>)siteParam.clone());
					SiteTranslator trans = new SiteTranslator();
					for (Parameter<?> siteParam : site)
						trans.setParameterValue(siteParam, datas);
				}
				for (AttenuationRelationship attenRel : gmpeComparisons)
					HazardCurvePlotter.setAttenRelParams(attenRel, im, run, csSite, datas);
			}
			
			for (double iml : myIMLevels) {
				System.out.println("Disaggregating");
				disaggCalc.setMagRange(minMag, numMags, deltaMag);
				disaggCalc.setNumSourcestoShow(numSourcesForDisag);
				disaggCalc.setShowDistances(showSourceDistances);
				boolean success = disaggCalc.disaggregate(Math.log(iml), site, imr, erf, disaggParams);
				if (!success)
					throw new RuntimeException("Disagg calc failed (see errors above, if any).");
				disaggCalc.setMaxZAxisForPlot(maxZAxis);
				System.out.println("Done Disaggregating");
				String metadata = "temp metadata";
				try {
					boolean textOnly = plotTypes.size() == 1 && plotTypes.get(0) == PlotType.TXT;
					
					String address = null;
					
					if (!textOnly) {
						System.out.println("Fetching plot...");
						address = disaggCalc.getDisaggregationPlotUsingServlet(metadata);
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
							null, null, null);
					String binDataText = disaggCalc.getBinData();
					String sourceDataText = disaggCalc.getDisaggregationSourceInfo();
					
					for (PlotType type : plotTypes)
						savePlot(address, outFileName, meanModeText, metadataText, binDataText, sourceDataText, type);
					
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
							success = disaggCalc.disaggregate(Math.log(iml), site, attenRel, erf, disaggParams);
							if (!success)
								throw new RuntimeException("Disagg calc failed (see errors above, if any).");
							disaggCalc.setMaxZAxisForPlot(maxZAxis);
							
							if (!textOnly) {
								System.out.println("Fetching plot...");
								address = disaggCalc.getDisaggregationPlotUsingServlet(metadata);
							}
							
							String gmpeOutFileName = outFileName+"_"+attenRel.getShortName();
							String gmpeMeanModeHeader = getMeanModeHeader(iml, isProb, prob);
							String gmpeMeanModeText = gmpeMeanModeHeader+disaggCalc.getMeanAndModeInfo();
							String gmpeMetadataText = HazardCurvePlotter.getCurveParametersInfoAsString(
									attenRel, erf, site,
									disaggParams.getParameter(Double.class, MaxDistanceParam.NAME).getValue());
							String gmpeBinDataText = disaggCalc.getBinData();
							String gmpeSourceDataText = disaggCalc.getDisaggregationSourceInfo();
							
							for (PlotType type : plotTypes)
								savePlot(address, gmpeOutFileName, gmpeMeanModeText, gmpeMetadataText,
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

	private void savePlot(String address, String outFileName, String meanModeText, String metadataText,
			String binDataText, String sourceDataText, PlotType type) throws IOException {
		if (type == PlotType.PDF) {
			String outputFileName = outputDir.getAbsolutePath()+File.separator+outFileName+".pdf";
			DisaggregationPlotViewerWindow.saveAsPDF(
					address+DisaggregationCalculator.DISAGGREGATION_PLOT_PDF_NAME,
					outputFileName, meanModeText, metadataText, binDataText, sourceDataText);
		} else if (type == PlotType.PNG) {
			downloadPlot(address+ DisaggregationCalculator.DISAGGREGATION_PLOT_PNG_NAME, outFileName, "png");
		} else if (type == PlotType.JPG || type == PlotType.JPEG) {
			downloadPlot(address+ DisaggregationCalculator.DISAGGREGATION_PLOT_JPG_NAME, outFileName, "jpg");
		} else if (type == PlotType.TXT) {
			String outputFileName = outputDir.getAbsolutePath()+File.separator+outFileName+".txt";
			DisaggregationPlotViewerWindow.saveAsTXT(outputFileName, meanModeText, metadataText,
					binDataText, sourceDataText);
		} else {
			throw new IllegalArgumentException("Unknown plot type: "+type);
		}
	}
	
	private void downloadPlot(String address, String outFileName, String ext) throws IOException {
		File outFile = new File(outputDir.getAbsolutePath()+File.separator+outFileName+"."+ext);
		System.out.println("Downloading disagg "+ext+" plot to: "+outFile.getAbsolutePath());
		FileUtils.downloadURL(address, outFile);
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

	public static void main(String args[]) throws DocumentException, InvocationTargetException {
//		String[] newArgs = {"-R", "5760", "-p", "3,5,10", "-pr", "0.002,4.0e-4", "-o", "/tmp", "-t", "png",
//				"-ef", "/home/kevin/workspace/opensha-cybershake/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml"};
////		String[] newArgs = {"--help"};
////		String[] newArgs = {"-R", "792", "-p", "3", "-pr", "4.0e-4", "-t", "pdf", "-o", "D:\\Documents\\temp"};
//		Cybershake_OpenSHA_DBApplication.HOST_NAME = Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME;
//		args = newArgs;
		
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
