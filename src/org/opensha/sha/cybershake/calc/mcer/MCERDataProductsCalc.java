package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import oracle.net.aso.a;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.dom4j.DocumentException;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.mcer.ASCEDetLowerLimitCalc;
import org.opensha.sha.calc.mcer.AbstractMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.AbstractMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedCurveBasedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.CombinedMultiMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.CombinedMultiMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CurveBasedMCErProbabilisitCalc;
import org.opensha.sha.calc.mcer.DeterministicResult;
import org.opensha.sha.calc.mcer.GMPE_MCErDeterministicCalc;
import org.opensha.sha.calc.mcer.GMPE_MCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.calc.mcer.WeightProvider;
import org.opensha.sha.calc.mcer.WeightedAverageMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.WeightedAverageMCErProbabilisticCalc;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.HazardDecompositionPlotter;
import org.opensha.sha.cybershake.calc.RupProbModERF;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.gui.util.AttenRelSaver;
import org.opensha.sha.cybershake.gui.util.ERFSaver;
import org.opensha.sha.cybershake.plot.DisaggregationPlotter;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.cybershake.plot.PlotType;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.attenRelImpl.MultiIMR_Averaged_AttenRel;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;
import org.opensha.sha.util.SiteTranslator;

import scratch.UCERF3.erf.FaultSystemSolutionERF;
import scratch.peter.ucerf3.NSHMP13_DeterminisiticERF;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MCERDataProductsCalc {
	
	public static File cacheDir;
	static {
		File dir = new File("/home/kevin/CyberShake/MCER/.amps_cache");
		if (dir.exists())
			cacheDir = dir;
	}
	
	private CyberShakeComponent comp;
	private List<Double> periods;
	
	private List<AttenuationRelationship> gmpes;
	
	private File outputDir;
	
	private DBAccess db;
	private Runs2DB runs2db;
	private CybershakeSiteInfo2DB sites2db;
	private CachedPeakAmplitudesFromDB amps2db;
	
//	private HSSFSheet asceSheet;
//	private FormulaEvaluator evaluator;
	
	private CyberShakeMCErDeterministicCalc csDetCalc;
	private CyberShakeMCErProbabilisticCalc csProbCalc;
	
	private List<AbstractMCErDeterministicCalc> gmpeDetCalcs;
	private List<CurveBasedMCErProbabilisitCalc> gmpeProbCalcs;
	
	private WeightedAverageMCErDeterministicCalc avgDetCalc;
	private WeightedAverageMCErProbabilisticCalc avgProbCalc;
	private static boolean directAverage = false;
	private WeightProvider avgWeightProv;
	private static boolean doFinalMCER = true;
	
	private boolean includeNEHRP = false;
	
	static final String default_periods = "1,1.5,2,3,4,5,7.5,10";
	static final String all_periods = "0.01,0.02,0.03,0.05,0.075,0.1,0.15,0.2,0.25,0.3,0.4,"
				+"0.5,0.75,1.0,1.5,2.0,3.0,4.0,5.0,7.5,10.0";
//	static final String all_periods = "0.01,0.02,0.03,0.05,0.075,0.1,0.15,0.2,0.25,0.3,0.4,"
//			+"0.5,0.75,1.0,1.5,2.0,3.0,4.0,5.0,5.5,6,6.5,7.5,8.5,10.0";
	
	static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
	
	private boolean disaggregate = false;
	private Map<String, List<Integer>> combinedSources = null;
	
	private ERF erf;
	
	public MCERDataProductsCalc(ERF erf, List<AttenuationRelationship> gmpes,
			CyberShakeComponent comp, List<Double> periods, File outputDir) throws IOException {
		init(db = Cybershake_OpenSHA_DBApplication.getDB(), erf, gmpes, null, comp, periods, outputDir, false, null, false);
	}
	
	public MCERDataProductsCalc(CommandLine cmd) throws IOException, DocumentException, InvocationTargetException {
		ERF erf = ERFSaver.LOAD_ERF_FROM_FILE(cmd.getOptionValue("erf-file"));
		List<AttenuationRelationship> attenRels = Lists.newArrayList();
		
		for (String attenRelFile : DataUtils.commaSplit(cmd.getOptionValue("atten-rel-file"))) {
			AttenuationRelationship attenRel = AttenRelSaver.LOAD_ATTEN_REL_FROM_FILE(attenRelFile);
			attenRels.add(attenRel);
		}
		
		erf.updateForecast();
		
		ERF gmpeERF = null;
		if (cmd.hasOption("gmpe-erf-file")) {
			gmpeERF = ERFSaver.LOAD_ERF_FROM_FILE(cmd.getOptionValue("gmpe-erf-file"));
			gmpeERF.updateForecast();
		}
		
		CyberShakeComponent comp = CybershakeIM.fromShortName(cmd.getOptionValue("component"), CyberShakeComponent.class);
		
		String periodStrs;
		if (cmd.hasOption("period"))
			periodStrs = cmd.getOptionValue("period");
		else
			periodStrs = default_periods;
		List<Double> periods = DataUtils.commaDoubleSplit(periodStrs);
		
		File outputDir = new File(cmd.getOptionValue("output-dir"));
		Preconditions.checkArgument((outputDir.exists() && outputDir.isDirectory()) || outputDir.mkdir(),
				"Output dir does not exist and could not be created");
		
		File gmpeCacheDir = null;
		if (cmd.hasOption("gmpe-cache-dir")) {
			gmpeCacheDir = new File(cmd.getOptionValue("gmpe-cache-dir"));
			Preconditions.checkArgument(gmpeCacheDir.exists() && gmpeCacheDir.isDirectory() || gmpeCacheDir.mkdir(),
					"GMPE cache dir does not exist and could not be created");
		}
		
		boolean includeNEHRP = cmd.hasOption("nehrp-mcer");
		
		disaggregate = cmd.hasOption("disaggregate");
		
		init(Cybershake_OpenSHA_DBApplication.getDB(), erf, attenRels, gmpeERF, comp, periods, outputDir,
				cmd.hasOption("weight-average"), gmpeCacheDir, includeNEHRP);
	}
	
	private void init(DBAccess db, ERF erf, List<AttenuationRelationship> gmpes, ERF gmpeERF,
			CyberShakeComponent comp, List<Double> periods, File outputDir, boolean weightAverage,
			File gmpeCacheDir, boolean includeNEHRP) throws IOException {
		this.db = db;
		this.comp = comp;
		this.periods = periods;
		this.gmpes = gmpes;
		this.outputDir = outputDir;
		this.includeNEHRP = includeNEHRP;
		this.erf = erf;
		
		runs2db = new Runs2DB(db);
		sites2db = new CybershakeSiteInfo2DB(db);
		amps2db = new CachedPeakAmplitudesFromDB(db, cacheDir, erf);
		
		csDetCalc = new CyberShakeMCErDeterministicCalc(amps2db, erf, comp);
		csProbCalc = new CyberShakeMCErProbabilisticCalc(db, comp);
		
		if (gmpes != null && !gmpes.isEmpty()) {
			gmpeDetCalcs = Lists.newArrayList();
			gmpeProbCalcs = Lists.newArrayList();
			
			if (gmpeERF == null)
				gmpeERF = erf;
			ERF gmpeDetERF = getGMPEDetERF(gmpeERF, csDetCalc.getRupProbMod());
			System.out.println("GMPE Det ERF: "+ClassUtils.getClassNameWithoutPackage(gmpeDetERF.getClass()));
			
			Component gmpeComp = MCErCalcUtils.getSupportedTranslationComponent(
					gmpes.get(0), comp.getGMPESupportedComponents());
			
			DiscretizedFunc xVals = IMT_Info.getUSGS_SA_Function();
			
			for (AttenuationRelationship gmpe : gmpes) {
				gmpeDetCalcs.add(new GMPE_MCErDeterministicCalc(gmpeDetERF, gmpe, gmpeComp));
				gmpeProbCalcs.add(new GMPE_MCErProbabilisticCalc(gmpeERF, gmpe, gmpeComp, xVals));
			}
			
			if (gmpeCacheDir != null) {
				for (int i=0; i<gmpes.size(); i++) {
					AttenuationRelationship gmpe = gmpes.get(i);
					String cachePrefix = CyberShakeMCErMapGenerator.getCachePrefix(
							-1, gmpeERF, gmpeComp, Lists.newArrayList(gmpe));
					
					gmpeDetCalcs.set(i, new CachedMCErDeterministicCalc(gmpeDetCalcs.get(i),
							new File(gmpeCacheDir, cachePrefix+"_deterministic.xml")));
					gmpeProbCalcs.set(i, new CachedCurveBasedMCErProbabilisticCalc(gmpeProbCalcs.get(i),
							new File(gmpeCacheDir, cachePrefix+"_probabilistic_curve.xml")));
				}
			}
			
			if (weightAverage) {
				CombinedMultiMCErDeterministicCalc gmpeDetCalc = new CombinedMultiMCErDeterministicCalc(gmpeDetCalcs);
				avgWeightProv = new CyberShakeWeightProvider(csProbCalc, gmpeProbCalcs, csDetCalc, gmpeDetCalc);
				
				avgDetCalc = new WeightedAverageMCErDeterministicCalc(avgWeightProv, csDetCalc, gmpeDetCalc);
				List<CurveBasedMCErProbabilisitCalc> allProbCalcs = Lists.newArrayList();
				allProbCalcs.add(csProbCalc);
				allProbCalcs.addAll(gmpeProbCalcs);
				avgProbCalc = new WeightedAverageMCErProbabilisticCalc(avgWeightProv, allProbCalcs);
			}
		} else {
			Preconditions.checkState(!weightAverage, "Can't weight average without GMPEs!");
		}
	}
	
	static ERF getGMPEDetERF(ERF erf, RuptureProbabilityModifier detProbMod) {
		if (erf instanceof FaultSystemSolutionERF) {
			System.out.println("Assuming UCERF3 and using UCERF3 Deterministic ERF for det calcs");
			NSHMP13_DeterminisiticERF detERF = NSHMP13_DeterminisiticERF.create(false);
			detERF.updateForecast();
			return detERF;
		} else if (detProbMod != null && erf instanceof MeanUCERF2) {
			System.out.println("Creating UCERF2 deterministic ERF");
			RupProbModERF detERF = new RupProbModERF(erf, detProbMod);
			detERF.updateForecast();
			return detERF;
		}
		return erf;
	}
	
	public void calc(int runID) throws IOException {
		calc(Lists.newArrayList(runID));
	}
	
	public void calc(List<Integer> runIDs) throws IOException {
		for (int runID : runIDs) {
			CybershakeRun run = runs2db.getRun(runID);
			Preconditions.checkNotNull(run, "Run not found: %s", runID);
			CybershakeSite site = sites2db.getSiteFromDB(run.getSiteID());
			
			doCalc(run, site);
		}
	}
	
	static ParameterList getSiteParams(List<? extends ScalarIMR> gmpes) {
		ParameterList siteParams = new ParameterList();
		if (gmpes == null || gmpes.isEmpty()) {
			// need Vs30 for det lower limit even if no GMPEs
			siteParams.addParameter(new Vs30_Param());
		} else {
			for (ScalarIMR gmpe : gmpes)
				for (Parameter<?> param : gmpe.getSiteParams())
					if (!siteParams.containsParameter(param.getName()))
						siteParams.addParameter(param);
		}
		return siteParams;
	}
	
	private void doCalc(CybershakeRun run, CybershakeSite csSite) throws IOException {
		System.out.println("Calculating for "+csSite.short_name+", runID="+run.getRunID());
		File runOutputDir = new File(outputDir, csSite.short_name+"_run"+run.getRunID());
		Preconditions.checkState(runOutputDir.exists() && runOutputDir.isDirectory() || runOutputDir.mkdir());
		
		CyberShakeSiteRun site = new CyberShakeSiteRun(csSite, run);
		
		// load in site data
		OrderedSiteDataProviderList provs = HazardCurvePlotter.createProviders(run.getVelModelID());
		ParameterList siteParams = getSiteParams(gmpes);
		SiteTranslator siteTrans = new SiteTranslator();
		List<SiteDataValue<?>> datas = provs.getBestAvailableData(site.getLocation());
		for (Parameter<?> param : siteParams) {
			siteTrans.setParameterValue(param, datas);
			site.addParameter(param);
		}
		
		// calc CyberShake deterministic
		System.out.println("Calculating CyberShake Values");
		List<DeterministicResult> csDeterms = Lists.newArrayList();
		DiscretizedFunc csDetSpectrum = new ArbitrarilyDiscretizedFunc();
		csDetSpectrum.setName("CyberShake Deterministic");
		DiscretizedFunc csProbSpectrum = new ArbitrarilyDiscretizedFunc();
		csProbSpectrum.setName("CyberShake Probabilistic");
		
		// to use consistent x values between CyberShake and GMPEs for prob calcs
		Map<Double, DiscretizedFunc> xValsMap = Maps.newHashMap();
		
		// calc CyberShake
		for (double period : periods) {
			try {
				DeterministicResult csDet = csDetCalc.calc(site, period);
				Preconditions.checkNotNull(csDet); // will kick down to catch and skip this period if null
				csDeterms.add(csDet);
				DiscretizedFunc curve = csProbCalc.calcHazardCurves(site, Lists.newArrayList(period)).get(period);
				xValsMap.put(period, curve);
				double csProb = CurveBasedMCErProbabilisitCalc.calcRTGM(curve);
//				double csProb = csProbCalc.calc(site, period);
				csProbSpectrum.set(period, csProb);
			} catch (RuntimeException e) {
				if (e.getMessage() != null && e.getMessage().startsWith("No CyberShake IM match")
						|| e instanceof NullPointerException) {
					System.out.println("Skipping period "+period+", no matching CyberShake IM");
					csDeterms.add(null);
					continue;
				}
				throw e;
			}
		}
		Preconditions.checkState(csDeterms.size() == periods.size());
		for (int i=0; i<periods.size(); i++)
			if (csDeterms.get(i) != null)
				csDetSpectrum.set(periods.get(i), csDeterms.get(i).getVal());
		
		System.out.println("Calculating GMPE Values");
		List<DiscretizedFunc> gmpeDetSpectrums = null;
		List<DiscretizedFunc> gmpeProbSpectrums = null;
		DiscretizedFunc gmpeCombinedDetSpectrum = null;
		DiscretizedFunc gmpeCombinedProbSpectrum = null;
		List<List<DeterministicResult>> gmpeDeterms = null;
		if (gmpeDetCalcs != null) {
			gmpeDetSpectrums = Lists.newArrayList();
			gmpeProbSpectrums = Lists.newArrayList();
			gmpeDeterms = Lists.newArrayList();
			
			CombinedMultiMCErProbabilisticCalc gmpeCombinedCalc = new CombinedMultiMCErProbabilisticCalc(gmpeProbCalcs);
			for (int i=0; i<gmpes.size(); i++) {
//				ArbitrarilyDiscretizedFunc detSpectrum = new ArbitrarilyDiscretizedFunc(
//						gmpes.get(i).getShortName()+" Deterministic");
//				ArbitrarilyDiscretizedFunc probSpectrum = new ArbitrarilyDiscretizedFunc(
//						gmpes.get(i).getShortName()+" Probabilistic");
				System.out.println("Calculating GMPE Det "+gmpes.get(i).getShortName());
				Map<Double, DeterministicResult> detResults = gmpeDetCalcs.get(i).calc(site, periods);
				List<DeterministicResult> gmpeDeterm = Lists.newArrayList();
				for (double period : periods)
					gmpeDeterm.add(detResults.get(period));
				gmpeDeterms.add(gmpeDeterm);
				DiscretizedFunc detSpectrum = AbstractMCErDeterministicCalc.toSpectrumFunc(detResults);
				detSpectrum.setName(gmpes.get(i).getShortName()+" Deterministic");
//				DiscretizedFunc probSpectrum = gmpeProbCalcs.get(i).calc(site, periods);
				DiscretizedFunc probSpectrum = new ArbitrarilyDiscretizedFunc();
				for (double period : periods) {
					System.out.println("Calculating GMPE Prob "+gmpes.get(i).getShortName()+" "+period+"s");
					if (xValsMap.containsKey(period))
						gmpeProbCalcs.get(i).setXVals(xValsMap.get(period));
					probSpectrum.set(period, gmpeProbCalcs.get(i).calc(site, period));
				}
				probSpectrum.setName(gmpes.get(i).getShortName()+" Probabilistic");
				
				gmpeDetSpectrums.add(detSpectrum);
				gmpeProbSpectrums.add(probSpectrum);
			}
			gmpeCombinedDetSpectrum = maximum(gmpeDetSpectrums);
//			gmpeCombinedProbSpectrum = average(gmpeProbSpectrums);
			// do it right, average hazard curves not spectrum
			gmpeCombinedProbSpectrum = gmpeCombinedCalc.calc(site, periods);
		}
		
		DiscretizedFunc avgProbVals = null;
		if (avgProbCalc != null) {
			avgProbVals = avgProbCalc.calc(site, periods);
		}
		
		Map<Double, DeterministicResult> avgDetVals = null;
		List<DeterministicResult> avgDetValsList = null;
		if (avgDetCalc != null) {
			avgDetVals = avgDetCalc.calc(site, periods);
			avgDetValsList = Lists.newArrayList();
			for (double period : periods)
				avgDetValsList.add(avgDetVals.get(period));
		}
		
		// plot deterministic
		for (boolean velPlot : new boolean[] {true, false})
			DeterministicResultPlotter.plot(comp, buildDetermMapForSite(csSite.short_name, csDetSpectrum),
					buildDetermMapForSite(csSite.short_name, gmpeDetSpectrums),
					Lists.newArrayList(PlotType.PNG, PlotType.PDF), velPlot, runOutputDir);
		
		// write deterministic CSV
		String perStr;
		double percentile = AbstractMCErDeterministicCalc.percentile;
		if ((float)percentile == (float)((int)percentile))
			perStr = (int)percentile+"";
		else
			perStr = (float)percentile+"";
		
		String name = site.getCS_Site().short_name+"_run"+run.getRunID()+"_Deterministic_";
		name += comp.getShortName()+"_"+perStr+"per_"+dateFormat.format(new Date())+".csv";
		
		File outputFile = new File(runOutputDir, name);
		DeterministicResultPlotter.writeCSV(periods, csDeterms, avgDetValsList, gmpes, gmpeDeterms, outputFile, false);
		
		// now PSV
		name = site.getCS_Site().short_name+"_run"+run.getRunID()+"_Deterministic_";
		name += comp.getShortName()+"_vel_"+perStr+"per_"+dateFormat.format(new Date())+".csv";
		
		outputFile = new File(runOutputDir, name);
		DeterministicResultPlotter.writeCSV(periods, csDeterms, avgDetValsList, gmpes, gmpeDeterms, outputFile, true);
		
		// plot probabilistic
		for (boolean velPlot : new boolean[] {true, false})
			ProbabilisticResultPlotter.plotProbMCEr(site, comp, csProbSpectrum, avgProbVals, gmpes, gmpeProbSpectrums, periods,
					Lists.newArrayList(PlotType.PNG, PlotType.PDF, PlotType.CSV), velPlot, runOutputDir);
		
//		// calc probabalistic
//		System.out.println("Calculating Probabilistic");
//		RTGMCalc probCalc = new RTGMCalc(run.getRunID(), comp, runOutputDir, db);
//		probCalc.setForceAddIMs(forceAddIMs);
//		probCalc.setGMPEs(erf, gmpes);
//		probCalc.setVelPlot(true);
//		probCalc.setPlotTypes(Lists.newArrayList(PlotType.CSV, PlotType.PNG, PlotType.PDF));
//		Preconditions.checkState(probCalc.calc());
//		// now do again for regular plot
//		probCalc.setVelPlot(false);
//		probCalc.setPlotTypes(Lists.newArrayList(PlotType.PNG, PlotType.PDF));
//		Preconditions.checkState(probCalc.calc());
//		DiscretizedFunc csProb = probCalc.getCSSpectrumMap().get(comp);
//		
//		// get mean GMPE RTGM
//		Map<Double, List<DiscretizedFunc>> gmpeHazCurvesMap = probCalc.getGMPEHazardCurves().rowMap().get(comp);
//		ArbitrarilyDiscretizedFunc gmpeProb = new ArbitrarilyDiscretizedFunc("GMPE Probabilistic");
//		for (int i=0; i<periods.size(); i++) {
//			Double period = periods.get(i);
//			List<DiscretizedFunc> gmpeHazCurves = gmpeHazCurvesMap.get(csRoundedPeriods.get(i));
//			Preconditions.checkNotNull(gmpeHazCurves, "No GMPE haz curves for period="+period
//					+". Avail: "+Joiner.on(",").join(gmpeHazCurvesMap.keySet()));
//			DiscretizedFunc meanCurve = average(gmpeHazCurves);
//			double rtgm = RTGMCalc.calcRTGM(meanCurve);
//			gmpeProb.set(period, rtgm);
//		}
		
//		// load in ASCE values if available
//		HSSFRow row = null;
//		for (int r=0; r<=asceSheet.getLastRowNum(); r++) {
//			HSSFRow testRow = asceSheet.getRow(r);
//			HSSFCell nameCell = testRow.getCell(0);
//			if (nameCell != null && nameCell.getStringCellValue().trim().equals(site.short_name)) {
//				row = testRow;
//				break;
//			}
//		}
//		DiscretizedFunc asceDeterm, asceProb;
//		if (row == null) {
//			System.out.println("WARNING: Couldn't find site "+site.short_name+" in ASCE spreadsheet");
//			asceDeterm = null;
//			asceProb = null;
//		} else {
//			DiscretizedFunc xVals = gmpeProb;
//			double tl = loadASCEValue(row.getCell(4), evaluator);
//			double prob = loadASCEValue(row.getCell(5), evaluator);
//			double det = loadASCEValue(row.getCell(7), evaluator);
//			asceDeterm = calcASCE(xVals, det, tl);
//			asceProb = calcASCE(xVals, prob, tl);
//			asceProb = null;
//		}
		// get vs30 from GMPE calc
		Preconditions.checkState(site.containsParameter(Vs30_Param.NAME));
		double vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
		// gmpeProb just used for x values here
		DiscretizedFunc asceDeterm = ASCEDetLowerLimitCalc.calc(gmpeCombinedProbSpectrum, vs30, csSite.createLocation());
		DiscretizedFunc asceProb = null;
		if (includeNEHRP) {
			try {
				asceProb = MCEr_USGS_ComparisonFetcher.getMCEr(site, gmpeCombinedProbSpectrum);
			} catch (Exception e) {
				System.err.println("Error fetching USGS NEHRP MCEr:");
				e.printStackTrace();
			}
		}
		
		DiscretizedFunc weightAverageProb = null;
		DiscretizedFunc weightAverageDet = null;
		
		if (avgProbCalc != null)
			weightAverageProb = MCErCalcUtils.saToPsuedoVel(avgProbVals);
		if (avgDetCalc != null)
			weightAverageDet = MCErCalcUtils.saToPsuedoVel(
					AbstractMCErDeterministicCalc.toSpectrumFunc(avgDetVals));
		
		// now generate combined plots
		System.out.println("Generating plots");
		makePlots(runOutputDir, csSite, run, comp, MCErCalcUtils.saToPsuedoVel(csDetSpectrum),
				MCErCalcUtils.saToPsuedoVel(gmpeCombinedDetSpectrum), MCErCalcUtils.saToPsuedoVel(asceDeterm),
				MCErCalcUtils.saToPsuedoVel(csProbSpectrum), MCErCalcUtils.saToPsuedoVel(gmpeCombinedProbSpectrum),
				asceProb, weightAverageDet, weightAverageProb);
		
		if (disaggregate) {
			List<CybershakeIM> ims = amps2db.getSupportedIMs(run.getRunID());
			CybershakeIM imForOrder = null;
			for (int i=ims.size(); --i>=0;) {
				CybershakeIM im = ims.get(i);
				if (im.getComponent() != comp)
					ims.remove(i);
				else if ((float)im.getVal() == 5f)
					imForOrder = im;
			}
			if (imForOrder == null)
				imForOrder = ims.get(0);
			DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
			int numToInclude = 5;
			if (combinedSources == null)
				combinedSources = HazardDecompositionPlotter.getCombinedSources(erf);
//			numToInclude = combinedSources.size()-1;
			System.out.println("Sources: "+combinedSources.size());
			
			String prefix = csSite.short_name+"_run"+run.getRunID()+"_"+comp.getShortName();
			File disaggDir = new File(runOutputDir, "disaggregations");
			Preconditions.checkState(disaggDir.exists() || disaggDir.mkdir());
			HazardCurveComputation calc = new HazardCurveComputation(db);
			calc.setPeakAmpsAccessor(amps2db);
			double uhsVal = Double.NaN; // RTGM if NaN
			SiteInfo2DB site2db = new SiteInfo2DB(db);
			HazardDecompositionPlotter.writeBarChart(calc, site2db, run, csSite,
					ims, imForOrder, xVals, disaggDir, numToInclude, prefix+"_hazard_decomp_bar",
					erf, combinedSources, uhsVal);
			
			// now hazard decomposition
			List<CybershakeIM> imsForDisagg = Lists.newArrayList();
			for (CybershakeIM im : ims) {
				float period = (float)PeakAmplitudesFromDB.getCleanedCS_Period(im.getVal());
				if (period == 5f || period == 7.5f)
					imsForDisagg.add(im);
			}
			
			uhsVal = 4e-4;
			AttenuationRelationship gmpe = null;
			if (gmpes != null) {
				if (gmpes.size() > 1) {
					gmpe = new MultiIMR_Averaged_AttenRel(gmpes) {
						@Override
						public String getShortName() {
							return "GMPE";
						}
					};
					gmpe.setParamDefaults();
				} else {
					gmpe = gmpes.get(0);
				}
			}
			HazardCurveCalculator gmpeCalc = new HazardCurveCalculator();
			for (CybershakeIM im : imsForDisagg) {
				float period = (float)PeakAmplitudesFromDB.getCleanedCS_Period(im.getVal());
				String xAxisLabel = period+"s SA";
				HazardDecompositionPlotter.doCyberShake(calc, site2db, run, csSite, im, xVals, disaggDir,
						numToInclude, xAxisLabel, prefix+"_hazard_decomp_"+period+"s", erf, combinedSources, uhsVal);
				if (gmpes != null)
					HazardDecompositionPlotter.doGMPE(gmpeCalc, site, gmpe, erf, xVals, disaggDir, numToInclude,
						xAxisLabel, prefix+"_hazard_decomp_gmpe_"+period+"s", im, combinedSources, uhsVal);
			}
			
			// now disagg
			DisaggregationPlotter disagg = new DisaggregationPlotter(run.getRunID(), imsForDisagg,
					Lists.newArrayList(gmpe), Lists.newArrayList(uhsVal), null, disaggDir,
					Lists.newArrayList(PlotType.PDF, PlotType.PNG, PlotType.TXT));
			disagg.disaggregate();
		}
	}
	
	private static DiscretizedFunc average(List<DiscretizedFunc> funcs) {
		ArbitrarilyDiscretizedFunc mean = new ArbitrarilyDiscretizedFunc();
		double scalar = 1d/funcs.size(); // evenly weight
		for (DiscretizedFunc func : funcs) {
			if (mean.size() == 0) {
				// initialize with zeros
				for (Point2D pt : func)
					mean.set(pt.getX(), 0d);
			}
			Preconditions.checkState(mean.size() == func.size());
			for (int index=0; index<mean.size(); index++) {
				double y = mean.getY(index);
				y += scalar * func.getY(index);
				mean.set(index, y);
			}
		}
		return mean;
	}
	
	private static Map<String, DiscretizedFunc> buildDetermMapForSite(
			String siteName, DiscretizedFunc spectrum) {
		Map<String, DiscretizedFunc> map = Maps.newHashMap();
		map.put(siteName, spectrum);
		return map;
	}
	
	private static Map<String, List<DiscretizedFunc>> buildDetermMapForSite(
			String siteName, List<DiscretizedFunc> spectrums) {
		Map<String, List<DiscretizedFunc>> map = Maps.newHashMap();
		map.put(siteName, spectrums);
		return map;
	}
	
	public static double loadASCEValue(HSSFCell cell, FormulaEvaluator evaluator) {
		if (cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
			return cell.getNumericCellValue();
		} else if (cell.getCellType() == HSSFCell.CELL_TYPE_FORMULA) {
			return evaluator.evaluate(cell).getNumberValue();
		} else {
			try {
				return Double.parseDouble(cell.getStringCellValue());
			} catch (Exception e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
	}
	
//	public static DiscretizedFunc calcASCE(DiscretizedFunc xValsFunc, double val, double tl) {
//		ArbitrarilyDiscretizedFunc ret = new ArbitrarilyDiscretizedFunc();
//		
//		List<Double> xVals = Lists.newArrayList();
//		for (Point2D pt : xValsFunc)
//			xVals.add(pt.getX());
//		xVals.add(tl); // make sure that TL is in there
//		
//		for (double x : xVals) {
//			if (x <= tl)
//				ret.set(x, val);
//			else
//				ret.set(x, val*(tl/x));
//		}
//		
//		return ret;
//	}
	
	private static DiscretizedFunc maximum(List<DiscretizedFunc> funcs) {
		ArbitrarilyDiscretizedFunc ret = new ArbitrarilyDiscretizedFunc();
		
		DiscretizedFunc xVals = funcs.get(0);
		
		for (int i=0; i<xVals.size(); i++) {
			double x = xVals.getX(i);
			
			double y = 0;
			for (DiscretizedFunc func : funcs) {
				Preconditions.checkState((float)x == (float)func.getX(i));
				y = Math.max(y, func.getY(i));
			}
			
			ret.set(x, y);
		}
		
		return ret;
	}
	
	public void makePlots(File outputDir, CybershakeSite site, CybershakeRun run, CyberShakeComponent comp,
			DiscretizedFunc csDeterm, DiscretizedFunc gmpeDeterm, DiscretizedFunc asceDeterm,
			DiscretizedFunc csProb, DiscretizedFunc gmpeProb, DiscretizedFunc asceProb,
			DiscretizedFunc weightAverageDet, DiscretizedFunc weightAverageProb) throws IOException {
		boolean xLog = true;
		boolean yLog = true;
		Range xRange = new Range(1d, 10d);
		Range yRange = new Range(2e1, 2e3);
		
		String siteName = site.short_name;
		int runID = run.getRunID();

		String prefix = siteName+"_run"+runID+"_"+comp.getShortName();

		csDeterm.setName("CyberShake Det");
		gmpeDeterm.setName("GMPE Det");
		if (asceDeterm != null)
			asceDeterm.setName("ASCE 7-10 Det Lower Limit");

		csProb.setName("CyberShake Prob");
		gmpeProb.setName("GMPE Prob");
		if (asceProb != null)
//			asceProb.setName("ASCE 7-10 Ch 11.4");
			asceProb.setName("ASCE 7-16 MCEr");

		List<DiscretizedFunc> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();

		if (asceDeterm != null) {
			funcs.add(asceDeterm);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.DARK_GRAY));
		}

		if (asceProb != null) {
			funcs.add(asceProb);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLACK));
		}

		funcs.add(gmpeProb);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.RED));

		funcs.add(gmpeDeterm);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, PlotSymbol.CIRCLE, 4f, Color.RED));

		funcs.add(csProb);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLUE));

		funcs.add(csDeterm);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, PlotSymbol.CIRCLE, 4f, Color.BLUE));
		
		Color avgColor = Color.GREEN.darker().darker();
		DiscretizedFunc directAvgProb = null;
		if (weightAverageProb != null) {
			weightAverageProb.setName("Avg. Prob");
			funcs.add(weightAverageProb);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, avgColor));
			
			if (directAverage) {
				// also add direct average
				directAvgProb = directAverage(gmpeProb, csProb);
				directAvgProb.setName("Direct Avg. Prob");
				funcs.add(directAvgProb);
				chars.add(new PlotCurveCharacterstics(PlotLineType.DOTTED, 2f, PlotSymbol.CIRCLE, 4f, Color.GREEN));
			}
		}
		
		DiscretizedFunc directAvgDet = null;
		if (weightAverageDet != null) {
			weightAverageDet.setName("Avg. Det");
			funcs.add(weightAverageDet);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, PlotSymbol.CIRCLE, 4f, avgColor));
			
			if (directAverage) {
				// also add direct average
				directAvgDet = directAverage(gmpeDeterm, csDeterm);
				directAvgDet.setName("Direct Avg. Det");
				funcs.add(directAvgDet);
				chars.add(new PlotCurveCharacterstics(PlotLineType.DOTTED, 2f, PlotSymbol.CIRCLE, 4f, Color.GREEN));
			}
		}

		PlotSpec spec = new PlotSpec(funcs, chars, siteName, "Period (s)", "PSV (cm/s)");
		spec.setLegendVisible(true);

		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
		//			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);

		gp.drawGraphPanel(spec, xLog, yLog, xRange, yRange);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);

		gp.validate();
		gp.repaint();

		File file = new File(outputDir, prefix+"_all_curves");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsTXT(file.getAbsolutePath()+".txt");

		DiscretizedFunc csMCER = calcMCER(csDeterm, csProb, asceDeterm);
		csMCER.setName("CyberShake MCEr");
		DiscretizedFunc gmpeMCER = calcMCER(gmpeDeterm, gmpeProb, asceDeterm);
		gmpeMCER.setName("GMPE MCEr");
		DiscretizedFunc avgMCER = null;
		if (weightAverageDet != null && weightAverageProb != null) {
			avgMCER = calcMCER(weightAverageDet, weightAverageProb, asceDeterm);
			avgMCER.setName("Avg. MCEr");
		}

		funcs = Lists.newArrayList();
		chars = Lists.newArrayList();

//		if (asceDeterm != null) {
//			funcs.add(asceDeterm);
//			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.DARK_GRAY));
//		}

		if (asceProb != null) {
			funcs.add(asceProb);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLACK));
		}

		funcs.add(gmpeMCER);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.RED));

		funcs.add(csMCER);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLUE));
		
		if (avgMCER != null) {
			funcs.add(avgMCER);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, avgColor));
			
			if (directAverage) {
				// also add direct average
				DiscretizedFunc directAvgMCER = calcMCER(directAvgDet, directAvgProb, asceDeterm);
				directAvgMCER.setName("Direct Avg. MCEr");
				funcs.add(directAvgMCER);
				chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, PlotSymbol.CIRCLE, 4f, Color.GREEN));
			}
		}

		spec = new PlotSpec(funcs, chars, siteName+" MCER", "Period (s)", "PSV (cm/s)");
		spec.setLegendVisible(true);

		gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
		//			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);

		gp.drawGraphPanel(spec, xLog, yLog, xRange, yRange);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);

		gp.validate();
		gp.repaint();

		file = new File(outputDir, prefix+"_MCER");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsTXT(file.getAbsolutePath()+".txt");
		
		// now final MCEr
		DiscretizedFunc finalMCER = null;
		if (doFinalMCER) {
			finalMCER = calcFinalMCER(csMCER, gmpeMCER);
			
			funcs = Lists.newArrayList();
			chars = Lists.newArrayList();

			if (asceProb != null) {
				funcs.add(asceProb);
				chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLACK));
			}

//			funcs.add(gmpeMCER);
//			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.RED));
//
//			funcs.add(csMCER);
//			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLUE));
			
			funcs.add(finalMCER);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, avgColor));

			spec = new PlotSpec(funcs, chars, siteName+" MCER", "Period (s)", "PSV (cm/s)");
			spec.setLegendVisible(true);

			gp = new HeadlessGraphPanel();
			gp.setBackgroundColor(Color.WHITE);
			//			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
			gp.setTickLabelFontSize(18);
			gp.setAxisLabelFontSize(20);
			gp.setPlotLabelFontSize(21);

			gp.drawGraphPanel(spec, xLog, yLog, xRange, yRange);
			gp.getChartPanel().setSize(1000, 800);
			gp.setVisible(true);

			gp.validate();
			gp.repaint();

			file = new File(outputDir, prefix+"_final_MCER");
			gp.saveAsPDF(file.getAbsolutePath()+".pdf");
			gp.saveAsPNG(file.getAbsolutePath()+".png");
			gp.saveAsTXT(file.getAbsolutePath()+".txt");
		}
		
		// now write CSV
		
		CSVFile<String> csv = new CSVFile<String>(true);
		
		List<String> header = Lists.newArrayList("Site Short Name", "Run ID", "Component", "Period");
		if (finalMCER != null)
			header.add("Final MCEr");
		if (avgMCER != null)
			header.add("Curve Weight Averaged MCEr");
		header.add("CyberShake MCEr");
		header.add("GMPE MCEr");
		if (avgMCER != null)
			header.add("Curve Weight Averaged Probabilistic");
		header.add("CyberShake Probabilistic");
		header.add("GMPE Probabilistic");
		if (avgMCER != null)
			header.add("Curve Weight Averaged Deterministic");
		header.add("CyberShake Deterministic");
		header.add("GMPE Deterministic");
		header.add("Deterministic Lower Limit");
		csv.addLine(header);
		
		for (double period : periods) {
			List<String> line = Lists.newArrayList(site.short_name, run.getRunID()+"", comp.getShortName(), (float)period+"");
			if (finalMCER != null)
				line.add(getValIfPresent(finalMCER, period));
			if (avgMCER != null)
				line.add(getValIfPresent(avgMCER, period));
			line.add(getValIfPresent(csMCER, period));
			line.add(getValIfPresent(gmpeMCER, period));
			if (avgMCER != null)
				line.add(getValIfPresent(weightAverageProb, period));
			line.add(getValIfPresent(csProb, period));
			line.add(getValIfPresent(gmpeProb, period));
			if (avgMCER != null)
				line.add(getValIfPresent(weightAverageDet, period));
			line.add(getValIfPresent(csDeterm, period));
			line.add(getValIfPresent(gmpeDeterm, period));
			line.add(getValIfPresent(asceDeterm, period));
			
			csv.addLine(line);
		}
		
		csv.writeToFile(new File(outputDir, prefix+"_MCER.csv"));
	}
	
	public static DiscretizedFunc calcFinalMCER(DiscretizedFunc csMCER, DiscretizedFunc gmpeMCER) {
		ArbitrarilyDiscretizedFunc finalMCER = new ArbitrarilyDiscretizedFunc();
		finalMCER.setName("Final MCEr");
		
		for (int i=0; i<gmpeMCER.size(); i++) {
			double period = gmpeMCER.getX(i);
			double gmpeVal = gmpeMCER.getY(period);
			double csVal;
			if (csMCER.hasX(period))
				csVal = csMCER.getY(period);
			else
				csVal = gmpeVal;
			
			finalMCER.set(period, calcFinalMCER(csVal, gmpeVal, period));
		}
		
		return finalMCER;
	}
	
	public static double calcFinalMCER(double csVal, double gmpeVal, double period) {
		if (gmpeVal >= csVal) {
			// CS can only increase, not decrease
			return gmpeVal;
		} else {
			double csWeight = CyberShakeWeightProvider.calcCyberShakeWeight(period);
			double gmpeWeight = CyberShakeWeightProvider.calcGMPEWeight(period);
			
			// weighted average in log space
			return Math.exp(Math.log(csVal)*csWeight + Math.log(gmpeVal)*gmpeWeight);
		}
	}
	
	static String getValIfPresent(DiscretizedFunc func, double period) {
		return getValIfPresent(func, period, null);
	}
	
	static String getValIfPresent(DiscretizedFunc func, double period, DecimalFormat df) {
		if (func.hasX(period)) {
			double val = func.getY(period);
			if (df == null)
				return val+"";
			else
				return df.format(val);
		}
		return "";
	}
	
	private static DiscretizedFunc directAverage(DiscretizedFunc gmpeFunc, DiscretizedFunc csFunc) {
		ArbitrarilyDiscretizedFunc ret = new ArbitrarilyDiscretizedFunc();
		
		for (int i=0; i<gmpeFunc.size(); i++) {
			double period = gmpeFunc.getX(i);
			
			double gmpeVal = gmpeFunc.getY(i);
			
			double csVal;
			if (!csFunc.hasX(period)) {
				ret.set(period, gmpeVal);
				continue;
			} else {
				csVal = csFunc.getY(period);
			}
			
			double gmpeWeight = CyberShakeWeightProvider.calcGMPEWeight(period);
			double csWeight = CyberShakeWeightProvider.calcCyberShakeWeight(period);
			
			double avg = gmpeWeight*gmpeVal + csWeight*csVal;
//			double avg = MCErCalcUtils.saToPsuedoVel(gmpeWeight*MCErCalcUtils.psuedoVelToSA(gmpeVal, period)
//					+ csWeight*MCErCalcUtils.psuedoVelToSA(csVal, period), period);
			
			ret.set(period, avg);
		}
		
		return ret;
	}
	
	public static DiscretizedFunc calcMCER(DiscretizedFunc determ, DiscretizedFunc prob,
			DiscretizedFunc determLowerLimit) {
		ArbitrarilyDiscretizedFunc ret = new ArbitrarilyDiscretizedFunc();
		
		// from CB Crouse via e-mail 10/17/2014, subject "RE: New plots and updates"
		// The rules are: take the higher of the deterministic curve and the deterministic lower limit
		// curve at each period; the result is the deterministic MCER. Then take the lower of the
		// probabilistic MCER and the deterministic MCER; this curve is the MCER.
		
		for (Point2D pt : prob) {
			double x = pt.getX();
			
			double dVal;
			if (determ == null)
				dVal = Double.POSITIVE_INFINITY;
			else
				dVal = determ.getY(x);
			Preconditions.checkState(dVal >= 0, "Deterministic value must be >= 0");
			double pVal = prob.getY(x);
			Preconditions.checkState(pVal >= 0, "Probabilistic value must be >= 0");
			double dLowVal;
			if (determLowerLimit != null)
				dLowVal = determLowerLimit.getY(x);
			else
				dLowVal = 0d;
			
			double val = MCErCalcUtils.calcMCER(dVal, pVal, dLowVal);
			
			ret.set(x, val);
		}
		
		return ret;
	}
	
	private static Options createOptions() {
		Options ops = new Options();
		
		Option run = new Option("R", "run-id", true, "Run ID. Multiple can be comma separated");
		run.setRequired(true);
		ops.addOption(run);
		
		Option component = new Option("cmp", "component", true, "Intensity measure component. "
				+ "Options: "+Joiner.on(",").join(CybershakeIM.getShortNames(CyberShakeComponent.class)));
		component.setRequired(true);
		ops.addOption(component);
		
		Option output = new Option("o", "output-dir", true, "Output directory");
		output.setRequired(true);
		ops.addOption(output);
		
		Option period = new Option("p", "period", true, "SA periods. Default: "+default_periods);
		period.setRequired(false);
		ops.addOption(period);
		
		Option erfFile = new Option("ef", "erf-file", true, "XML ERF description file for comparison");
		erfFile.setRequired(true);
		ops.addOption(erfFile);
		
		Option gmpeERFFile = new Option("gmpeef", "gmpe-erf-file", true, "XML ERF description file for comparison if different"
				+ "than CyberShake ERF");
		gmpeERFFile.setRequired(false);
		ops.addOption(gmpeERFFile);
		
		Option weightingOption = new Option("weight", "weight-average", false,
				"Apply weighting scheme betweeen GMPE and CyberShake results and add curve to plots.");
		weightingOption.setRequired(false);
		ops.addOption(weightingOption);
		
		Option usgsOption = new Option("nehrp", "nehrp-mcer", false,
				"Fetch and add 2015 NEHRP MCEr spectrum to plots");
		usgsOption.setRequired(false);
		ops.addOption(usgsOption);
		
		Option attenRelFiles = new Option("af", "atten-rel-file", true,
				"XML Attenuation Relationship description file(s) for " + 
				"comparison. Multiple files should be comma separated");
		attenRelFiles.setRequired(true);
		ops.addOption(attenRelFiles);
		
		Option gmpeCacheDir = new Option("gcache", "gmpe-cache-dir", true, "GMPE cache directory");
		gmpeCacheDir.setRequired(false);
		ops.addOption(gmpeCacheDir);
		
		Option disagg = new Option("disagg", "disaggregate", false, "Flag to do disaggregation");
		disagg.setRequired(false);
		ops.addOption(disagg);
		
		Option help = new Option("?", "help", false, "Display this message");
		help.setRequired(false);
		ops.addOption(help);
		
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

	public static void main(String[] args) throws DocumentException, InvocationTargetException, IOException {
		if (args.length == 1 && args[0].equals("--hardcoded")) {
//			String argStr = "--run-id 2657,3037,2722,3022,3030,3027,2636,2638,2660,2703,3504,2988,2965,3007";
//			String argStr = "--run-id 2657";
//			String argStr = "--run-id 3030"; // STNI orig
//			String argStr = "--run-id 3873"; // STNI 1 hz
//			String argStr = "--run-id 3873,3880"; // STNI,SBSM 1 hz
//			String argStr = "--run-id 3883"; // s603 1 hz
//			String argStr = "--run-id 3875,3878,3877"; // LAPD,PAS,COO 1 hz
//			String argStr = "--run-id 3876,3877,3868,3875,3879,3878,3882,3883,3884,3885,3880,3869,3873,3861";
//			String argStr = "--run-id 4282,4283,4284,4285,4286,4287,4289,4290,4291,4292,4293,4294,4295,4296";
			String argStr = "--component RotD100";
//			argStr += " --run-id 4282,4283,4284,4285,4286,4287,4289,4290,4291,4292,4293,4294,4295,4296";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_12_main";
//			argStr += " --run-id 4516,4517,4518,4519,4520,4521,4522,4523";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_12_la_west";
//			argStr += " --run-id 3876,3877,3870,3875,3879,3878,3882,3883,3884,3885,3880,3869,3873,3861";
//			argStr += " --run-id 3878";
			argStr += " --run-id 3877,3870";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_main";
			argStr += " --output-dir /tmp/mcer";
//			argStr += " --run-id 4067,4068,4069,4217,4218,4219,4220,4221";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_la_basin_west";
//			argStr += " --run-id 4083,4084,4085,4086,4087,4088,4237,4090";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_la_basin_central";
//			argStr += " --run-id 4111,4112,4113,4114,4115,4116,4129,4130";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_la_basin_east";
//			argStr += " --run-id 4058,4047,3938,4037,3932,3934";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_sf_valley";
//			argStr += " --run-id 4125,4126,4127,4128,4139,4140,4141,4142,4151,4152,4153";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_orange_county";
//			argStr += " --run-id 4155,4156,3883,4157,4167,4168,4169,4181,4182";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_inland_empire";
//			argStr += " --run-id 4067,4068,4069,4217,4218,4219,4220,4221,4083,4084,4085,4086,4087,4088,4237,"
//					+ "4090,4111,4112,4113,4114,4115,4116,4129,4130,4058,4047,3938,4037,3932,3934";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products/study_15_4_extra30";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products_stochastic";
//			argStr += " --output-dir /home/kevin/CyberShake/MCER/mcer_data_products_gmpeUCERF3";
//			argStr += " --output-dir /tmp/mcer_data_products";
			argStr += " --erf-file src/org/opensha/sha/cybershake/conf/MeanUCERF.xml";
			argStr += " --atten-rel-file src/org/opensha/sha/cybershake/conf/ask2014.xml,"
					+ "src/org/opensha/sha/cybershake/conf/bssa2014.xml,"
					+ "src/org/opensha/sha/cybershake/conf/cb2014.xml,"
					+ "src/org/opensha/sha/cybershake/conf/cy2014.xml";
//			argStr += " --atten-rel-file /tmp/gmpe_no_trunk/ask2014.xml,"
//					+ "/tmp/gmpe_no_trunk/bssa2014.xml,"
//					+ "/tmp/gmpe_no_trunk/cb2014.xml,"
//					+ "/tmp/gmpe_no_trunk/cy2014.xml";
			argStr += " --period "+all_periods;
//			argStr += " --disaggregate";
//			argStr += " --gmpe-erf-file src/org/opensha/sha/cybershake/conf/MeanUCERF3_downsampled.xml";
			// for UCERF3 GMPE
//			argStr += " --gmpe-erf-file src/org/opensha/sha/cybershake/conf/MeanUCERF3_full.xml";
//			argStr += " --gmpe-cache-dir /home/kevin/CyberShake/MCER/gmpe_cache_gen/2015_09_29-ucerf3_full_ngaw2/";
			
			argStr += " --weight-average";
			argStr += " --nehrp-mcer";
			args = Splitter.on(" ").splitToList(argStr).toArray(new String[0]);
			
			Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		}
		
		DBAccess db = null;
		
		try {
			Options options = createOptions();
			
			String appName = ClassUtils.getClassNameWithoutPackage(MCERDataProductsCalc.class);
			
			CommandLineParser parser = new GnuParser();
			
			if (args.length == 0) {
				printUsage(options, appName);
			}
			
			try {
				CommandLine cmd = parser.parse( options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					printHelp(options, appName);
				}
				
				MCERDataProductsCalc calc = new MCERDataProductsCalc(cmd);
				db = calc.db;
				
				List<Integer> runIDs = Lists.newArrayList();
				for (String runID : DataUtils.commaSplit(cmd.getOptionValue("run-id")))
					runIDs.add(Integer.parseInt(runID));
				
				calc.calc(runIDs);
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
