package org.opensha.sha.cybershake.calc;

import java.awt.Color;
import java.awt.Font;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.UncertainArbDiscDataset;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.exceptions.IMRException;
import org.opensha.commons.exceptions.ParameterException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.util.ComparablePairing;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.hazardMap.HazardCurveSetCalculator;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ERFTestSubset;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.attenRelImpl.NGAWest_2014_Averaged_AttenRel;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncLevelParam;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncTypeParam;
import org.opensha.sha.util.SiteTranslator;
import org.opensha.sra.rtgm.RTGM;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;

public class HazardDecompositionPlotter {
	
	public static void main(String[] args) throws IOException {
//		List<Integer> runIDs = Lists.newArrayList(2657, 3037, 2722, 3022, 3030, 3027, 2636, 2638,
//				2660, 2703, 3504, 2988, 2965, 3007);
		List<Integer> runIDs = Lists.newArrayList(3877);
		File baseDir = new File("/home/kevin/CyberShake/MCER");
		File outputDir = new File(baseDir, "hazard_decomposition_2");
		if (!outputDir.exists())
			outputDir.mkdir();
		File cacheDir = new File(baseDir, ".amps_cache");
		if (!cacheDir.exists())
			cacheDir.mkdir();
		
		double uhsVal = Double.NaN; // can be set to 4e-4 for 2% in 50 instead of RTGM
		
		// NGA2 without Idriss
		CachedGMPE gmpe = new CachedGMPE();
//		ScalarIMR gmpe = new NGAWest_2014_Averaged_AttenRel(null, false);
		gmpe.setParamDefaults();
		gmpe.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_1SIDED);
		gmpe.getParameter(SigmaTruncLevelParam.NAME).setValue(3d);
		gmpe.setIntensityMeasure(SA_Param.NAME);
		String periodFileStr = "3sec";
		SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), 3d);
		String xAxisLabel = "3sec SA";
		CybershakeIM imType = new CybershakeIM(146, IMType.SA, 3d, null, CyberShakeComponent.RotD100);
//		String periodFileStr = "7.5sec";
//		SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), 7.5d);
//		String xAxisLabel = "7.5sec SA";
//		CybershakeIM imType = new CybershakeIM(138, IMType.SA, 7.5d, null, CyberShakeComponent.RotD100);
		
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		Map<String, List<Integer>> combSourceMap = getCombinedSources(erf);
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		Runs2DB run2db = new Runs2DB(db); 
		CybershakeSiteInfo2DB csSite2db = new CybershakeSiteInfo2DB(db);
		SiteInfo2DB site2db = new SiteInfo2DB(db);
		HazardCurveComputation csCalc = new HazardCurveComputation(db);
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, cacheDir, erf);
		csCalc.setPeakAmpsAccessor(amps2db);
		
		HazardCurveCalculator calc = new HazardCurveCalculator();
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(gmpe.getIntensityMeasure());
		
		SiteTranslator trans = new SiteTranslator();
		
		int numToInclude = 10;
		
		HazardCurvePlotter.SCALE_PRINT_SUCCESS = false;
		
		for (int runID : runIDs) {
			if (gmpe instanceof CachedGMPE)
				((CachedGMPE)gmpe).clearCache();
			
			CybershakeRun run = run2db.getRun(runID);
			int siteID = run.getSiteID();
			CybershakeSite csSite = csSite2db.getSiteFromDB(siteID);
			Location loc = csSite.createLocation();
			
			System.out.println("Doing "+csSite.short_name);
			
			Site site = new Site(loc);
			
			OrderedSiteDataProviderList providers =
					HazardCurvePlotter.createProviders(run.getVelModelID());
			
			trans.setAllSiteParams(gmpe, providers.getBestAvailableData(loc));
			site.addParameterList(gmpe.getSiteParams());
			site.setName(csSite.short_name);
			
			String prefix = csSite.short_name+"_decomposition_"+periodFileStr;
			
			System.out.println("Doing CyberShake for "+site.getName());
			doCyberShake(csCalc, site2db, run, csSite, imType, xVals, outputDir, numToInclude,
					xAxisLabel, prefix+"_CyberShake", erf, combSourceMap, uhsVal);
			
			System.out.println("Doing GMPE for "+site.getName());
			doGMPE(calc, site, gmpe, erf, xVals, outputDir, numToInclude,
					xAxisLabel, prefix+"_NGA2", imType, combSourceMap, uhsVal);
		}
		
		db.destroy();
		System.exit(0);
	}
	
	public static void doGMPE(HazardCurveCalculator calc, Site site, ScalarIMR imr,
			ERF erf, DiscretizedFunc xVals, File outputDir, int numToInclude,
			String xAxisLabel, String prefix, CybershakeIM imType,
			Map<String, List<Integer>> combSourceMap, double uhsVal) throws IOException {
		DiscretizedFunc logXVals = HazardCurveSetCalculator.getLogFunction(xVals.deepClone());
		DiscretizedFunc totalHazard = calc.getAnnualizedRates(
				HazardCurveSetCalculator.unLogFunction(xVals,
						calc.getHazardCurve(logXVals.deepClone(), site, imr, erf)), 1d);
		totalHazard = HazardCurvePlotter.getScaledCurveForComponent(imr, imType, totalHazard);
		totalHazard.setName("Total Hazard");
		
		List<DiscretizedFunc> sourceFuncs = Lists.newArrayList();
		List<Double> sourceRTGMContributions = Lists.newArrayList();
		
		double totRTGM = calcVal(totalHazard, uhsVal);
		
		double sumContributions = 0d;
		
		boolean origScalePrint = HazardCurvePlotter.SCALE_PRINT_SUCCESS;
		HazardCurvePlotter.SCALE_PRINT_SUCCESS = false;
		
		for (String sourceName : combSourceMap.keySet()) {
			List<Integer> ids = combSourceMap.get(sourceName);
			HashSet<Integer> withinIDs = new HashSet<Integer>();
			for (int sourceID : ids) {
				ProbEqkSource source = erf.getSource(sourceID);
				if (source.getMinDistance(site) > 200d)
					continue;
				withinIDs.add(sourceID);
			}
			if (withinIDs.isEmpty())
				continue;
			
//			System.out.println("Source "+sourceID);
			ERFTestSubset subset = new ERFTestSubset(erf);
			for (int sourceID : withinIDs)
				subset.includeSource(sourceID);
			subset.updateForecast();
			Preconditions.checkState(subset.getNumSources() == withinIDs.size());
			DiscretizedFunc srcHazard = calc.getAnnualizedRates(
					HazardCurveSetCalculator.unLogFunction(xVals,
							calc.getHazardCurve(logXVals.deepClone(), site, imr, subset)), 1d);
			srcHazard = HazardCurvePlotter.getScaledCurveForComponent(imr, imType, srcHazard);
			sourceFuncs.add(srcHazard);
			
			// now all sources except this one for disagg
			subset = new ERFTestSubset(erf);
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++)
				if (!withinIDs.contains(sourceID))
					subset.includeSource(sourceID);
			subset.updateForecast();
			Preconditions.checkState(subset.getNumSources() == erf.getNumSources()-withinIDs.size());
			DiscretizedFunc srcWithoutHazard = calc.getAnnualizedRates(
					HazardCurveSetCalculator.unLogFunction(xVals,
							calc.getHazardCurve(logXVals.deepClone(), site, imr, subset)), 1d);
			srcWithoutHazard = HazardCurvePlotter.getScaledCurveForComponent(imr, imType, srcWithoutHazard);
			
			double withoutRTGM = calcVal(srcWithoutHazard, uhsVal);
			double deltaRTGM = totRTGM - withoutRTGM;
//			Preconditions.checkState(deltaRTGM >= 0);
			sourceRTGMContributions.add(deltaRTGM);
			
			sumContributions += deltaRTGM/totRTGM;
			
			srcHazard.setName(sourceName);
			srcHazard.setInfo("RTGM Contribution: "+(float)totRTGM+" - "+(float)withoutRTGM+" = "+(float)deltaRTGM);
//			System.out.println(source.getName()+" "+srcHazard.getInfo());
		}
		HazardCurvePlotter.SCALE_PRINT_SUCCESS = origScalePrint;
		
		System.out.println("Total fractional GMPE contribution: "+sumContributions);
		
//		System.out.println("Num above zero: "+numAboveZero);
		
		List<ComparablePairing<Double, DiscretizedFunc>> pairings =
				ComparablePairing.build(sourceRTGMContributions, sourceFuncs);
		Collections.sort(pairings);
		Collections.reverse(pairings);
		
		List<XY_DataSet> funcs = Lists.newArrayList();
		List<Color> colors = GraphWindow.generateDefaultColors();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		funcs.add(totalHazard);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, colors.get((funcs.size()-1) % colors.size())));
		
		for (int i=0; i<numToInclude; i++) {
			funcs.add(pairings.get(i).getData());
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, colors.get((funcs.size()-1) % colors.size())));
		}
		
		Range xRange = new Range(3e-3, 3e0);
		Range yRange = new Range(5e-7, 5e-1);
		
		if (uhsVal > 0) {
			double x = totalHazard.getFirstInterpolatedX_inLogXLogYDomain(uhsVal);
			DefaultXY_DataSet xy = new DefaultXY_DataSet();
			xy.set(x, yRange.getLowerBound());
			xy.set(x, yRange.getUpperBound());
			funcs.add(xy);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
		}
		
		PlotSpec spec = new PlotSpec(funcs, chars, site.getName()+" NGA2 Hazard Decomposition",
				xAxisLabel, "Annual Rate Of Exceedance");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
//		gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		
		gp.drawGraphPanel(spec, true, true, xRange, yRange);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);
		
		gp.validate();
		gp.repaint();
		
		File file = new File(outputDir, prefix);
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
	}
	
	public static void doCyberShake(HazardCurveComputation calc, SiteInfo2DB site2db,
			CybershakeRun run, CybershakeSite site, CybershakeIM imType, DiscretizedFunc xVals,
			File outputDir, int numToInclude, String xAxisLabel, String prefix, ERF erf,
			Map<String, List<Integer>> combSourceMap, double uhsVal) throws IOException {
		
		List<DecompositionResult> results = calcFaultContributions(
				calc, site2db, run, site, imType, xVals, erf, combSourceMap, uhsVal);
		
		List<XY_DataSet> funcs = Lists.newArrayList();
		List<Color> colors = GraphWindow.generateDefaultColors();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		funcs.add(results.get(0).curveAllFaults);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, colors.get((funcs.size()-1) % colors.size())));
		
		for (int i=0; i<numToInclude && i<results.size(); i++) {
			funcs.add(results.get(i).curveFaultOnly);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, colors.get((funcs.size()-1) % colors.size())));
		}
		
		Range xRange = new Range(3e-3, 3e0);
		Range yRange = new Range(5e-7, 5e-1);
		
		if (uhsVal > 0) {
			double x = results.get(0).curveAllFaults.getFirstInterpolatedX_inLogXLogYDomain(uhsVal);
			DefaultXY_DataSet xy = new DefaultXY_DataSet();
			xy.set(x, yRange.getLowerBound());
			xy.set(x, yRange.getUpperBound());
			funcs.add(xy);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
		}
		
		PlotSpec spec = new PlotSpec(funcs, chars, site.short_name+" CyberShake Hazard Decomposition",
				xAxisLabel, "Annual Rate Of Exceedance");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
//		gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		
		gp.drawGraphPanel(spec, true, true, xRange, yRange);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);
		
		gp.validate();
		gp.repaint();
		
		File file = new File(outputDir, prefix);
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
	}
	
	public static class DecompositionResult implements Comparable<DecompositionResult> {
		final String faultName;
		final double hazardDelta;
		final double hazardWith;
		final double hazardWithout;
		final DiscretizedFunc curveFaultOnly;
		final DiscretizedFunc curveAllFaults;
		
		public DecompositionResult(String faultName, double hazardDelta, double hazardWith, double hazardWithout,
				DiscretizedFunc curveFaultOnly, DiscretizedFunc curveAllFaults) {
			super();
			this.faultName = faultName;
			this.hazardDelta = hazardDelta;
			this.hazardWith = hazardWith;
			this.hazardWithout = hazardWithout;
			this.curveFaultOnly = curveFaultOnly;
			this.curveAllFaults = curveAllFaults;
		}

		@Override
		public int compareTo(DecompositionResult o) {
			return Double.compare(o.hazardDelta, hazardDelta);
		}
		
		
	}
	
	static List<DecompositionResult> calcFaultContributions(HazardCurveComputation calc, SiteInfo2DB site2db,
			CybershakeRun run, CybershakeSite site, CybershakeIM imType, DiscretizedFunc xVals, ERF erf,
			Map<String, List<Integer>> combSourceMap, double uhsVal) {
		List<DecompositionResult> results = Lists.newArrayList();
		
		List<Double> xValsList = Lists.newArrayList();
		for (Point2D pt : xVals)
			xValsList.add(pt.getX());
		
		HazardCurveCalculator gmpeCalc = new HazardCurveCalculator();
		
		List<Integer> allSoucreIDs = site2db.getSrcIdsForSite(run.getSiteID(), run.getERFID());
		
		DiscretizedFunc totalHazard = gmpeCalc.getAnnualizedRates(
				calc.computeHazardCurve(xValsList, run, imType, allSoucreIDs), 1d);
		totalHazard.setName("Total Hazard");
		
		List<DiscretizedFunc> sourceFuncs = Lists.newArrayList();
		List<Double> sourceRTGMContributions = Lists.newArrayList();
		
		double totRTGM = calcVal(totalHazard, uhsVal);
		Preconditions.checkState(Doubles.isFinite(totRTGM));
		
		HashSet<Integer> sourceSet = new HashSet<Integer>(
				allSoucreIDs);
		
		HashSet<Integer> sourcesConsidered = new HashSet<Integer>();
		
		for (String sourceName : combSourceMap.keySet()) {
			List<Integer> ids = combSourceMap.get(sourceName);
			HashSet<Integer> withinIDs = new HashSet<Integer>();
			for (int sourceID : ids) {
				if (!sourceSet.contains(sourceID))
					continue;
				withinIDs.add(sourceID);
				Preconditions.checkState(!sourcesConsidered.contains(sourceID));
				sourcesConsidered.add(sourceID);
			}
			if (withinIDs.isEmpty())
				continue;
			
//			System.out.println("CS Source "+sourceID);
			DiscretizedFunc srcHazard = gmpeCalc.getAnnualizedRates(
					calc.computeHazardCurve(xValsList, run, imType, Lists.newArrayList(withinIDs)), 1d);
			sourceFuncs.add(srcHazard);
			
			// now all sources except this one for disagg
			List<Integer> sourcesWithout = Lists.newArrayList();
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++)
				if (sourceSet.contains(sourceID) && !withinIDs.contains(sourceID))
					sourcesWithout.add(sourceID);
			Preconditions.checkState(sourcesWithout.size() == sourceSet.size()-withinIDs.size());
			DiscretizedFunc srcWithoutHazard = gmpeCalc.getAnnualizedRates(
					calc.computeHazardCurve(xValsList, run, imType, sourcesWithout), 1d);
			
			double withoutRTGM = calcVal(srcWithoutHazard, uhsVal);
			double deltaRTGM = totRTGM - withoutRTGM;
//			double deltaRTGM = calcRTGM(srcHazard);
//			if (Double.isNaN(deltaRTGM))
//				deltaRTGM = 0d;
//			double withoutRTGM = totRTGM - deltaRTGM;
			Preconditions.checkState(Doubles.isFinite(deltaRTGM));
			Preconditions.checkState(Doubles.isFinite(withoutRTGM));
			Preconditions.checkState(deltaRTGM >= 0);
			sourceRTGMContributions.add(deltaRTGM);
			
			srcHazard.setName(sourceName);
			srcHazard.setInfo("CS RTGM Contribution: "+(float)totRTGM+" - "+(float)withoutRTGM+" = "+(float)deltaRTGM);
//			System.out.println(source.getName()+" "+srcHazard.getInfo());
			
			results.add(new DecompositionResult(sourceName, deltaRTGM, totRTGM, withoutRTGM, srcHazard, totalHazard));
		}
		
		System.out.println(sourcesConsidered.size()+"/"+sourceSet.size()+" considered");
		
		Collections.sort(results);
		
		return results;
	}
	
	private static double calcVal(DiscretizedFunc curve, double uhsVal) {
		if (uhsVal > 0)
			return HazardDataSetLoader.getCurveVal(curve, false, uhsVal);
		RTGM calc = RTGM.create(curve, null, null);
		try {
			calc.call();
		} catch (RuntimeException e) {
			System.err.println("RTGM Calc failed for Hazard Curve:\n"+curve);
			System.err.flush();
			throw e;
		}
		double rtgm = calc.get();
		Preconditions.checkState(rtgm > 0, "RTGM is not positive");
		return rtgm;
	}
	
	public static class CachedGMPE extends NGAWest_2014_Averaged_AttenRel {
		
		int cacheHits = 0;
		int cacheMisses = 0;
		
		private Table<EqkRupture, Site, DiscretizedFunc> exceedProbsCache;
		
		public CachedGMPE() {
			super(null, false);
			
			exceedProbsCache = HashBasedTable.create();
		}

		@Override
		public synchronized DiscretizedFunc getExceedProbabilities(
				DiscretizedFunc intensityMeasureLevels)
				throws ParameterException {
			EqkRupture rup = getEqkRupture();
			Preconditions.checkNotNull(rup);
			Site site = getSite();
			Preconditions.checkNotNull(site);
			DiscretizedFunc probs = exceedProbsCache.get(rup, site);
			if (probs == null) {
				cacheMisses++;
				super.setEqkRupture(rup);
				probs = super.getExceedProbabilities(intensityMeasureLevels).deepClone();
				exceedProbsCache.put(rup, site, probs);
			} else {
				cacheHits++;
			}
			// now fill in passed func
			for (int i=0; i<intensityMeasureLevels.size(); i++)
				intensityMeasureLevels.set(i, probs.getY(i));
			return intensityMeasureLevels;
		}
		
		@Override
		public double getMean() {
			throw new UnsupportedOperationException();
		}

		@Override
		public double getStdDev() {
			throw new UnsupportedOperationException();
		}

		@Override
		public double getExceedProbability() throws ParameterException,
				IMRException {
			throw new UnsupportedOperationException();
		}

		@Override
		protected double getExceedProbability(double mean, double stdDev,
				double iml) throws ParameterException, IMRException {
			throw new UnsupportedOperationException();
		}

		@Override
		public double getExceedProbability(double iml)
				throws ParameterException, IMRException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void setEqkRupture(EqkRupture eqkRupture) {
			this.eqkRupture = eqkRupture;
		}

		public void clearCache() {
			exceedProbsCache.clear();
			cacheHits = 0;
			cacheMisses = 0;
		}
		
	}
	
	public static Map<String, List<Integer>> getCombinedSources(ERF erf) {
		int commonPrefix = 8; // will pass if common prefix this long 
		int distance = 3; // or if lev distance this little and prefix as long as below
		int distPrefix = 6;
		
		Map<String, List<Integer>> map = Maps.newHashMap();
		
		HashSet<Integer> processedSources = new HashSet<Integer>();
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			if (processedSources.contains(sourceID))
				continue;
			String name = getStrippedName(erf.getSource(sourceID).getName());
			List<Integer> matches = Lists.newArrayList();
			matches.add(sourceID);
			String commonAllPrefix = null;
			List<String> matchNames = Lists.newArrayList();
			matchNames.add("'"+erf.getSource(sourceID).getName()+"'");
			for (int s=0; s<erf.getNumSources(); s++) {
				if (s == sourceID || processedSources.contains(s))
					continue;
				String name2 = getStrippedName(erf.getSource(s).getName());
				String common = StringUtils.getCommonPrefix(name, name2);
				if (common.length() >= commonPrefix
						|| (common.length() >= distPrefix
							&& StringUtils.getLevenshteinDistance(name, name2) <= distance)) {
					matches.add(s);
					matchNames.add("'"+erf.getSource(s).getName()+"'");
					if (commonAllPrefix == null || common.length() < commonAllPrefix.length())
						commonAllPrefix = common;
				}
			}
			if (commonAllPrefix == null) {
				Preconditions.checkState(matches.size() == 1);
				commonAllPrefix = name;
			}
			String origPrefix = commonAllPrefix;
			commonAllPrefix = commonAllPrefix.trim();
			if (commonAllPrefix.contains("alt"))
				commonAllPrefix = commonAllPrefix.substring(0, commonAllPrefix.indexOf("alt")).trim();
			while (commonAllPrefix.endsWith(",") || commonAllPrefix.endsWith(";")
					|| commonAllPrefix.endsWith("("))
				commonAllPrefix = commonAllPrefix.substring(0, commonAllPrefix.length()-1);
			
			Preconditions.checkState(commonAllPrefix.length() > 2,
					"Common prefix too short: '"+commonAllPrefix+"', orig: '"+origPrefix+"'");
			map.put(commonAllPrefix, matches);
			processedSources.addAll(matches);
			
			System.out.println(commonAllPrefix+": "+Joiner.on(", ").join(matchNames));
		}
		
		return map;
	}
	
	private static String getStrippedName(String name) {
		name = name.trim();
		if (StringUtils.isNumeric(name.substring(0,1)) || name.startsWith("NV"))
			name = name.substring(name.indexOf(" ")+1);
		name = name.replaceAll("-", " ");
		if (name.toLowerCase().startsWith("eastern"))
			name = name.substring("eastern".length()).trim();
		if (name.toLowerCase().startsWith("western"))
			name = name.substring("western".length()).trim();
		return name;
	}
	
	public static void writeBarChart(HazardCurveComputation calc, SiteInfo2DB site2db,
			CybershakeRun run, CybershakeSite site, List<CybershakeIM> imTypes, CybershakeIM imForOrder,
			DiscretizedFunc xVals, File outputDir, int numToInclude, String prefix,
			ERF erf, Map<String, List<Integer>> combSourceMap, double uhsVal) throws IOException {
		boolean normalize = true;
		Preconditions.checkState(imTypes.contains(imForOrder));
		
		// first do it for im for ordering
		List<DecompositionResult> resultsForOrder = calcFaultContributions(
				calc, site2db, run, site, imForOrder, xVals, erf, combSourceMap, uhsVal);
		if (numToInclude > resultsForOrder.size())
			numToInclude = resultsForOrder.size();
		resultsForOrder = resultsForOrder.subList(0, numToInclude);
		
		double sumContrib = 0;
		for (DecompositionResult result : resultsForOrder) {
			double val = result.hazardDelta/result.hazardWith;
			System.out.println(result.faultName+": delta="+result.hazardDelta+", with="+result.hazardWith
					+", without="+result.hazardWithout+", contrib="+val);
			sumContrib += val;
		}
		System.out.println("Sum of ref contributions: "+sumContrib);
//		System.exit(0);
		
		Map<String, List<Integer>> myFaultsMap = Maps.newHashMap();
		HashSet<Integer> sourcesIncluded = new HashSet<Integer>();
		for (DecompositionResult result : resultsForOrder) {
			myFaultsMap.put(result.faultName, combSourceMap.get(result.faultName));
			sourcesIncluded.addAll(combSourceMap.get(result.faultName));
		}
		List<Integer> otherSources = Lists.newArrayList();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++)
			if (!sourcesIncluded.contains(sourceID))
				otherSources.add(sourceID);
		myFaultsMap.put("Other Faults", otherSources);
		Map<String, List<Integer>> additional = Maps.newHashMap();
		additional.put("Other Faults", otherSources);
		resultsForOrder.add(calcFaultContributions(
				calc, site2db, run, site, imForOrder, xVals, erf, additional, uhsVal).get(0));
		
		Map<String, DiscretizedFunc> faultContribFuncs = Maps.newHashMap();
		for (DecompositionResult result : resultsForOrder) {
			DiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
			func.setName(result.faultName);
			faultContribFuncs.put(result.faultName, func);
		}
		
		for (int i=0; i<imTypes.size(); i++) {
			CybershakeIM im = imTypes.get(i);
			List<DecompositionResult> results;
//			if (im.equals(imForOrder))
//				results = resultsForOrder;
//			else
				results = calcFaultContributions(calc, site2db, run, site, imTypes.get(i), xVals, erf, myFaultsMap, uhsVal);
			
			for (DecompositionResult result : results)
				faultContribFuncs.get(result.faultName).set(im.getVal(), result.hazardDelta/result.hazardWith);
//				faultContribFuncs.get(result.faultName).set(im.getVal(), result.hazardDelta);
		}
		
		// now combine
		List<DiscretizedFunc> relativeFuncs = Lists.newArrayList();
		List<DiscretizedFunc> absoluteFuncs = Lists.newArrayList();
		
		for (int j=0; j<resultsForOrder.size(); j++) {
			ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
			func.setName(resultsForOrder.get(j).faultName);
			relativeFuncs.add(func);
			func = new ArbitrarilyDiscretizedFunc();
			func.setName(resultsForOrder.get(j).faultName);
			absoluteFuncs.add(func);
		}
//		ArbitrarilyDiscretizedFunc otherRelative = new ArbitrarilyDiscretizedFunc();
//		otherRelative.setName("Other Faults");
//		relativeFuncs.add(otherRelative); // for all others
		
		for (int i=0; i<imTypes.size(); i++) {
			CybershakeIM im = imTypes.get(i);
			
			double val = 0d;
			for (int j=0; j<resultsForOrder.size(); j++) {
				String faultName = resultsForOrder.get(j).faultName;
				
				double myRelative = faultContribFuncs.get(faultName).getY(im.getVal());
				val += myRelative;
			}
			
			double maxVal = val;
			
			for (int j=0; j<resultsForOrder.size(); j++) {
				String faultName = resultsForOrder.get(j).faultName;
				
				double myRelative = faultContribFuncs.get(faultName).getY(im.getVal());
				if (normalize)
					relativeFuncs.get(j).set(im.getVal(), val/maxVal);
				else
					relativeFuncs.get(j).set(im.getVal(), val);
				absoluteFuncs.get(j).set(im.getVal(), myRelative);
				val -= myRelative;
			}
//			Preconditions.checkState(val >= 0, val);
//			relativeFuncs.get(relativeFuncs.size()-1).set(im.getVal(), val);
		}
		
		List<DiscretizedFunc> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		List<Color> colors = GraphWindow.generateDefaultColors();
		
		Range xRange = null;
		
		for (int i=0; i<relativeFuncs.size(); i++) {
			DiscretizedFunc func = relativeFuncs.get(i);
			DiscretizedFunc funcAfter;
			if (i == relativeFuncs.size()-1) {
				funcAfter = new ArbitrarilyDiscretizedFunc();
				for (CybershakeIM im : imTypes)
					funcAfter.set(im.getVal(), 0d);
			} else {
				funcAfter = relativeFuncs.get(i+1);
			}
			
			if (xRange == null)
				xRange = new Range(func.getMinX(), func.getMaxX());
			
			UncertainArbDiscDataset rangeFunc = new UncertainArbDiscDataset(func, funcAfter, func);
			rangeFunc.setName(func.getName());
			
			funcs.add(rangeFunc);
			Color c = colors.get((funcs.size()-1) % colors.size());
			chars.add(new PlotCurveCharacterstics(PlotLineType.SHADED_UNCERTAIN, 1f, c));
			
			func.setName(null);
			funcs.add(func);
			chars.add(new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 4f, c));
		}
		
		PlotSpec spec = new PlotSpec(funcs, chars, site.short_name+" CyberShake MCEr Fault Impact",
				"Period (s)", "RTGM Fault Exclusion Impact");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
//		gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		gp.setLegendFontSize(18);
		
		gp.drawGraphPanel(spec, true, false, xRange, null);
		if (normalize)
			gp.getYAxis().setTickLabelsVisible(false);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);
		
		gp.validate();
		gp.repaint();
		
		File file = new File(outputDir, prefix);
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		
		if (normalize)
			gp.getYAxis().setTickLabelsVisible(true);
		
		// write line chart
		funcs = absoluteFuncs;
		chars = Lists.newArrayList();
		for (int i=0; i<funcs.size(); i++) {
			Color c = colors.get(i % colors.size());
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, c));
		}
		
		spec = new PlotSpec(funcs, chars, site.short_name+" CyberShake MCEr Fault Impact",
				"Period (s)", "RTGM Fault Exclusion Impact");
		spec.setLegendVisible(true);
		
		gp.drawGraphPanel(spec, true, false, xRange, null);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);
		
		gp.validate();
		gp.repaint();
		
		if (prefix.contains("bar"))
			prefix = prefix.replace("bar", "line");
		else
			prefix += "_line";
		file = new File(outputDir, prefix);
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		gp.saveAsPNG(file.getAbsolutePath()+".png");
	}

}
