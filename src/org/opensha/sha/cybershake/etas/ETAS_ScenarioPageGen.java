package org.opensha.sha.cybershake.etas;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.etas.ETASModProbConfig.ETAS_Cybershake_TimeSpans;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.mod.ModAttenRelRef;
import org.opensha.sha.imr.mod.ModAttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import scratch.UCERF3.erf.ETAS.ETAS_CatalogIO;
import scratch.UCERF3.erf.ETAS.ETAS_EqkRupture;
import scratch.UCERF3.erf.ETAS.analysis.SimulationMarkdownGenerator;
import scratch.UCERF3.erf.ETAS.launcher.ETAS_Config;
import scratch.UCERF3.erf.ETAS.launcher.ETAS_Config.BinaryFilteredOutputConfig;
import scratch.kevin.cybershake.simCompare.CSRupture;
import scratch.kevin.cybershake.simCompare.StudyModifiedProbRotDProvider;
import scratch.kevin.cybershake.simCompare.StudyRotDProvider;
import scratch.kevin.simCompare.SimulationHazardCurveCalc;
import scratch.kevin.simCompare.SiteHazardCurveComarePageGen;

public class ETAS_ScenarioPageGen {
	
	private CyberShakeStudy study;
	
	private File catalogsFile;
	private List<List<ETAS_EqkRupture>> catalogs;
	
	private ETAS_Config etasConfig;
	private ETAS_Cybershake_TimeSpans[] timeSpans;
	private ETASModProbConfig[] modProbConfigs;
	
	private List<CybershakeRun> runs;
	private List<Site> sites;
	private List<Site> curveSites;
	
	private CachedPeakAmplitudesFromDB amps2db;

	private LoadingCache<CS_CurveKey, DiscretizedFunc> csCurveCache;
	private LoadingCache<GMPE_CurveKey, DiscretizedFunc> gmpeCurveCache;
	
	private AttenRelRef gmpeRef;
	private ModAttenRelRef directivityRef;
	private Deque<ScalarIMR> gmpeDeque;

	private DiscretizedFunc hiResXVals;
	private DiscretizedFunc standardResXVals;
	private DiscretizedFunc lnStandardResXVals;
	
	public ETAS_ScenarioPageGen(CyberShakeStudy study, Vs30_Source vs30Source, ETAS_Config etasConfig, File mappingsCSVFile,
			ETAS_Cybershake_TimeSpans[] timeSpans, String[] highlightSiteNames, File ampsCacheDir, AttenRelRef gmpeRef) throws IOException {
		this.study = study;
		this.timeSpans = timeSpans;
		this.etasConfig = etasConfig;
		this.gmpeRef = gmpeRef;
		
		// first load catalogs
		List<BinaryFilteredOutputConfig> binaryFilters = etasConfig.getBinaryOutputFilters();
		if (binaryFilters != null) {
			binaryFilters = new ArrayList<>(binaryFilters);
			// sort so that the one with the lowest magnitude is used preferentially
			binaryFilters.sort(SimulationMarkdownGenerator.binaryOutputComparator);
			for (BinaryFilteredOutputConfig bin : binaryFilters) {
				File binFile = new File(etasConfig.getOutputDir(), bin.getPrefix()+".bin");
				if (binFile.exists()) {
					catalogsFile = binFile;
					break;
				}
			}
		}
		
		Preconditions.checkNotNull(catalogsFile);
		System.out.println("Loading catalogs...");
		catalogs = ETAS_CatalogIO.loadCatalogsBinary(catalogsFile, 5d);
		System.out.println("Loaded "+catalogs.size()+" catalogs");
		
		System.out.println("Fetching CyberShake runs/sites...");
		runs = study.runFetcher().fetch();
		sites = CyberShakeSiteBuilder.buildSites(study, vs30Source, runs);
		if (highlightSiteNames != null && highlightSiteNames.length > 0) {
			curveSites = new ArrayList<>();
			for (String name : highlightSiteNames)
				for (Site site : sites)
					if (site.getName().equals(name))
						curveSites.add(site);
		}
		System.out.println("Loaded "+runs.size()+" sites");
		
		int erfID = runs.get(0).getERFID();
		int rupVarScenID = runs.get(0).getRupVarScenID();
		
		modProbConfigs = new ETASModProbConfig[timeSpans.length];
		
		for (int i=0; i<timeSpans.length; i++)
			modProbConfigs[i] = new ETASModProbConfig(etasConfig, -1, timeSpans[i], mappingsCSVFile, erfID, rupVarScenID);
		
		amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, study.getERF());
		
		hiResXVals = SimulationHazardCurveCalc.getDefaultHazardCurve(4);
		standardResXVals = SimulationHazardCurveCalc.getDefaultHazardCurve(1);
		lnStandardResXVals = new ArbitrarilyDiscretizedFunc();
		for (Point2D pt : standardResXVals)
			lnStandardResXVals.set(Math.log(pt.getX()), 1d);
		
		csCurveCache = CacheBuilder.newBuilder().build(new CacheLoader<CS_CurveKey, DiscretizedFunc>() {

			@Override
			public DiscretizedFunc load(CS_CurveKey key) throws Exception {
				return key.calc.calc(key.site, key.period, 1d);
			}
			
		});
		
		HazardCurveCalculator gmpeCalc = new HazardCurveCalculator();
		gmpeDeque = new ArrayDeque<>();
		directivityRef = ModAttenRelRef.BAYLESS_SOMERVILLE_2013_DIRECTIVITY;
		gmpeCurveCache = CacheBuilder.newBuilder().build(new CacheLoader<GMPE_CurveKey, DiscretizedFunc>() {

			@Override
			public DiscretizedFunc load(GMPE_CurveKey key) throws Exception {
				DiscretizedFunc curve = lnStandardResXVals.deepClone();
				ScalarIMR gmpe = checkOutGMPE();
				SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), key.period);
				gmpeCalc.getHazardCurve(curve, key.site, gmpe, key.erf);
				DiscretizedFunc linearCurve = new ArbitrarilyDiscretizedFunc();
				for (int i=0; i<curve.size(); i++)
					linearCurve.set(standardResXVals.getX(i), curve.getY(i));
				return linearCurve;
			}
			
		});
	}
	
	private synchronized ScalarIMR checkOutGMPE() {
		if (gmpeDeque.isEmpty()) {
			ModAttenuationRelationship modGMPE = new ModAttenuationRelationship(gmpeRef, directivityRef);
			modGMPE.setParamDefaults();
			modGMPE.setIntensityMeasure(SA_Param.NAME);
			return modGMPE;
		}
		return gmpeDeque.pop();
	}
	
	private synchronized void checkInGMPE(ScalarIMR gmpe) {
		gmpeDeque.push(gmpe);
	}
	
	public void generatePage(File outputDir, double... periods) throws IOException {
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		StudyRotDProvider rawProv = new StudyRotDProvider(study, amps2db, periods, "CyberShake-TI");

		List<SimulationHazardCurveCalc<CSRupture>> tiCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		List<SimulationHazardCurveCalc<CSRupture>> tdCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		List<SimulationHazardCurveCalc<CSRupture>> etasCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		List<SimulationHazardCurveCalc<CSRupture>> etasUniformCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		
		List<AbstractERF> tiERFs = new ArrayList<>();
		List<AbstractERF> tdERFs = new ArrayList<>();
		List<AbstractERF> etasERFs = new ArrayList<>();
		List<AbstractERF> etasUniformERFs = new ArrayList<>();
		
		AbstractERF tiERF = study.buildNewERF();
		AbstractERF tdERF = study.buildNewERF();
		tdERF.setParameter(UCERF2.PROB_MODEL_PARAM_NAME, MeanUCERF2.PROB_MODEL_WGCEP_PREF_BLEND);
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTimeInMillis(etasConfig.getSimulationStartTimeMillis());
		tdERF.getTimeSpan().setStartTime(cal.get(GregorianCalendar.YEAR));
		tdERF.getTimeSpan().setDuration(1d);
		tdERF.updateForecast();
		
		for (int i=0; i<timeSpans.length; i++) {
			double durationYears = timeSpans[i].getTimeYears();
			tiCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					new NoEtasRupProbMod(study.getERF(), false, durationYears), "CyberShake-TI"), standardResXVals));
			tdCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					new NoEtasRupProbMod(study.getERF(), true, durationYears), "CyberShake-TD"), standardResXVals));
			etasCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					modProbConfigs[i].getRupProbModifier(), modProbConfigs[i].getRupVarProbModifier(), "CyberShake-ETAS"), hiResXVals));
			etasUniformCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					modProbConfigs[i].getRupProbModifier(), new UniformRVsRupVarProbMod(modProbConfigs[i].getRupVarProbModifier()),
					"CyberShake-ETAS Uniform"), hiResXVals));
			
			tiERFs.add(new ModProbERF(tiERF, durationYears));
			tdERFs.add(new ModProbERF(tdERF, durationYears));
			etasERFs.add(modProbConfigs[i].getModERFforGMPE(true));
			etasUniformERFs.add(modProbConfigs[i].getModERFforGMPE(false));
		}
		
		List<String> lines = new ArrayList<>();
		
		lines.add("# CyberShake-ETAS Simulations, "+etasConfig.getSimulationName());
		lines.add("");
		
		File etasPlotsDir = new File(outputDir, "etas_plots");
		if (etasPlotsDir.exists() && new File(etasPlotsDir, "README.md").exists()) {
			lines.add("[View ETAS Simulation Information and Plots]("+etasPlotsDir.getName()+")");
			lines.add("");
		}
		
		lines.add("CyberShake Study: "+study.getName()+" with "+sites.size()+" sites");
		lines.add("");
		
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		if (curveSites != null) {
			lines.add("## Hazard Curves");
			lines.add(topLink); lines.add("");
			for (Site site : curveSites) {
				lines.add("### "+site.getName()+" Hazard Curves");
				lines.add(topLink); lines.add("");
				
				File mapFile = new File(resourcesDir, site.getName()+"_location_map.png");
				if (!mapFile.exists())
					FileUtils.downloadURL(new URL(SiteHazardCurveComarePageGen.getMiniMap(site.getLocation())), mapFile);
				
				TableBuilder table = MarkdownUtils.tableBuilder();
				table.addLine("Site Location Map");
				table.addLine("![site map](resources/"+mapFile.getName()+")");
				lines.addAll(table.build());
				lines.add("");
				
				lines.add("#### CyberShake "+site.getName()+" Hazard Curves");
				lines.add(topLink); lines.add("");
				
				table = MarkdownUtils.tableBuilder();
				
				table.initNewLine();
				table.addColumn("Time Span");
				for (double period : periods)
					table.addColumn(optionalDigitDF.format(period)+"s");
				table.finalizeLine();
				
				for (int i=0; i<timeSpans.length; i++) {
					table.initNewLine();
					table.addColumn(timeSpans[i]);
					for (double period : periods) {
						File plot = plotHazardCurves(resourcesDir, site, period, timeSpans[i], tiCalcs.get(i),
								tdCalcs.get(i), etasCalcs.get(i), etasUniformCalcs.get(i));
						table.addColumn("![plot](resources/"+plot.getName()+")");
					}
					table.finalizeLine();
				}
				lines.addAll(table.build());
				lines.add("");
				
				lines.add("#### GMPE "+site.getName()+" Hazard Curves");
				lines.add(topLink); lines.add("");
				
				table = MarkdownUtils.tableBuilder();
				
				table.initNewLine();
				table.addColumn("Time Span");
				for (double period : periods)
					table.addColumn(optionalDigitDF.format(period)+"s");
				table.finalizeLine();
				
				for (int i=0; i<timeSpans.length; i++) {
					table.initNewLine();
					table.addColumn(timeSpans[i]);
					for (double period : periods) {
						File plot = plotGMPEHazardCurves(resourcesDir, site, period, timeSpans[i], tiERFs.get(i),
								tdERFs.get(i), etasERFs.get(i), etasUniformERFs.get(i));
						table.addColumn("![plot](resources/"+plot.getName()+")");
					}
					table.finalizeLine();
				}
				lines.addAll(table.build());
				lines.add("");
			}
		}
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
		lines.add(tocIndex, "## Table Of Contents");

		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
	}
	
	private class CS_CurveKey {
		private final Site site;
		private final Location loc;
		private final double period;
		private final SimulationHazardCurveCalc<CSRupture> calc;
		
		public CS_CurveKey(Site site, double period, SimulationHazardCurveCalc<CSRupture> calc) {
			this.site = site;
			this.loc = site.getLocation();
			this.period = period;
			this.calc = calc;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((calc == null) ? 0 : calc.hashCode());
			result = prime * result + ((loc == null) ? 0 : loc.hashCode());
			long temp;
			temp = Double.doubleToLongBits(period);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CS_CurveKey other = (CS_CurveKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (calc == null) {
				if (other.calc != null)
					return false;
			} else if (!calc.equals(other.calc))
				return false;
			if (loc == null) {
				if (other.loc != null)
					return false;
			} else if (!loc.equals(other.loc))
				return false;
			if (Double.doubleToLongBits(period) != Double.doubleToLongBits(other.period))
				return false;
			return true;
		}

		private ETAS_ScenarioPageGen getOuterType() {
			return ETAS_ScenarioPageGen.this;
		}
	}
	
	private class GMPE_CurveKey {
		private final Site site;
		private final Location loc;
		private final double period;
		private final AbstractERF erf;
		
		public GMPE_CurveKey(Site site, double period, AbstractERF erf) {
			this.site = site;
			this.loc = site.getLocation();
			this.period = period;
			this.erf = erf;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((erf == null) ? 0 : erf.hashCode());
			result = prime * result + ((loc == null) ? 0 : loc.hashCode());
			long temp;
			temp = Double.doubleToLongBits(period);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GMPE_CurveKey other = (GMPE_CurveKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (erf == null) {
				if (other.erf != null)
					return false;
			} else if (!erf.equals(other.erf))
				return false;
			if (loc == null) {
				if (other.loc != null)
					return false;
			} else if (!loc.equals(other.loc))
				return false;
			if (Double.doubleToLongBits(period) != Double.doubleToLongBits(other.period))
				return false;
			return true;
		}

		private ETAS_ScenarioPageGen getOuterType() {
			return ETAS_ScenarioPageGen.this;
		}
	}
	
	private File plotHazardCurves(File resourcesDir, Site site, double period, ETAS_Cybershake_TimeSpans timeSpan,
			SimulationHazardCurveCalc<CSRupture> tiCalc, SimulationHazardCurveCalc<CSRupture> tdCalc,
			SimulationHazardCurveCalc<CSRupture> etasCalc, SimulationHazardCurveCalc<CSRupture> uniformEtasCalc)
					throws IOException {
		
		// actual duration handled by mod probs
		DiscretizedFunc tiCurve;
		DiscretizedFunc tdCurve;
		DiscretizedFunc etasCurve;
		DiscretizedFunc uniformCurve;
		try {
			tiCurve = csCurveCache.get(new CS_CurveKey(site, period, tiCalc));
			tdCurve = csCurveCache.get(new CS_CurveKey(site, period, tdCalc));
			etasCurve = csCurveCache.get(new CS_CurveKey(site, period, etasCalc));
			uniformCurve = csCurveCache.get(new CS_CurveKey(site, period, uniformEtasCalc));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		String prefix = "hazard_curves_cs_"+site.getName()+"_"+optionalDigitDF.format(period)+"s_"+timeSpan.name().toLowerCase();
		return plotHazardCurves(resourcesDir, site, period, site.getName()+" CyberShake Hazard Curves", prefix, timeSpan,
				tiCurve, tdCurve, etasCurve, uniformCurve);
	}
	
	private File plotGMPEHazardCurves(File resourcesDir, Site site, double period, ETAS_Cybershake_TimeSpans timeSpan,
			AbstractERF tiERF, AbstractERF tdERF, AbstractERF etasERF, AbstractERF uniformETAS_ERF) throws IOException {
		
		// actual duration handled by mod probs
		DiscretizedFunc tiCurve;
		DiscretizedFunc tdCurve;
		DiscretizedFunc etasCurve;
		DiscretizedFunc uniformCurve;
		try {
			tiCurve = gmpeCurveCache.get(new GMPE_CurveKey(site, period, tiERF));
			tdCurve = gmpeCurveCache.get(new GMPE_CurveKey(site, period, tdERF));
			etasCurve = gmpeCurveCache.get(new GMPE_CurveKey(site, period, etasERF));
			uniformCurve = gmpeCurveCache.get(new GMPE_CurveKey(site, period, uniformETAS_ERF));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		String prefix = "hazard_curves_gmpe_"+site.getName()+"_"+optionalDigitDF.format(period)+"s_"+timeSpan.name().toLowerCase();
		return plotHazardCurves(resourcesDir, site, period, site.getName()+" GMPE Hazard Curves", prefix, timeSpan,
				tiCurve, tdCurve, etasCurve, uniformCurve);
	}

	private File plotHazardCurves(File resourcesDir, Site site, double period, String title, String prefix, ETAS_Cybershake_TimeSpans timeSpan,
			DiscretizedFunc tiCurve, DiscretizedFunc tdCurve, DiscretizedFunc etasCurve, DiscretizedFunc uniformCurve)
			throws IOException {
		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		funcs.add(tiCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE));
		
		funcs.add(tdCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.GREEN.darker()));
		
		funcs.add(etasCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED));
		
		funcs.add(uniformCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 1.5f, Color.RED));
		
		PlotSpec spec = new PlotSpec(funcs, chars, title,
				optionalDigitDF.format(period)+"s SA", timeSpan+" Probability of Exceedance");
		spec.setLegendVisible(true);
		
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(18);
		plotPrefs.setAxisLabelFontSize(20);
		plotPrefs.setPlotLabelFontSize(21);
		plotPrefs.setBackgroundColor(Color.WHITE);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
		
		Range curveXRange = new Range(1e-2, 1e1);
		Range curveYRange;
		switch (timeSpan) {
		case ONE_DAY:
			curveYRange = new Range(1e-8, 1e-1);
			break;
		case ONE_WEEK:
			curveYRange = new Range(1e-8, 1e-1);
			break;
		case ONE_YEAR:
			curveYRange = new Range(1e-7, 1e-0);
			break;

		default:
			throw new IllegalStateException();
		}
		
		gp.drawGraphPanel(spec, true, true, curveXRange, curveYRange);
		gp.getChartPanel().setSize(800, 600);
		
		File pngFile = new File(resourcesDir, prefix+".png");
		gp.saveAsPNG(pngFile.getAbsolutePath());
		File pdfFile = new File(resourcesDir, prefix+".pdf");
		gp.saveAsPDF(pdfFile.getAbsolutePath());
		return pngFile;
	}
	
	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.##");
	
	private class NoEtasRupProbMod implements RuptureProbabilityModifier {
		
		private double[][] probs;
		
		public NoEtasRupProbMod(AbstractERF erf, boolean td, double durationYears) {
			if (td) {
				erf.setParameter(UCERF2.PROB_MODEL_PARAM_NAME, MeanUCERF2.PROB_MODEL_WGCEP_PREF_BLEND);
				GregorianCalendar cal = new GregorianCalendar();
				cal.setTimeInMillis(etasConfig.getSimulationStartTimeMillis());
				erf.getTimeSpan().setStartTime(cal.get(GregorianCalendar.YEAR));
			} else {
				erf.setParameter(UCERF2.PROB_MODEL_PARAM_NAME, UCERF2.PROB_MODEL_POISSON);
			}
			erf.getTimeSpan().setDuration(1d);
			erf.updateForecast();
			probs = new double[erf.getNumSources()][];
			for (int sourceID=0; sourceID<probs.length; sourceID++) {
				ProbEqkSource source = erf.getSource(sourceID);
				probs[sourceID] = new double[source.getNumRuptures()];
				for (int rupID=0; rupID<probs[sourceID].length; rupID++) {
					double origRate = source.getRupture(rupID).getMeanAnnualRate(1d);
					probs[sourceID][rupID] = 1d - Math.exp(-origRate*durationYears);
				}
			}
		}

		@Override
		public double getModifiedProb(int sourceID, int rupID, double origProb) {
			return probs[sourceID][rupID];
		}
		
	}
	
	private class UniformRVsRupVarProbMod implements RuptureVariationProbabilityModifier {
		
		private RuptureVariationProbabilityModifier origMod;

		public UniformRVsRupVarProbMod(RuptureVariationProbabilityModifier origMod) {
			this.origMod = origMod;
		}

		@Override
		public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run,
				CybershakeIM im) {
			List<Double> origProbs = origMod.getVariationProbs(sourceID, rupID, originalProb, run, im);
			if (origProbs == null)
				return null;
			double sum = 0d;
			for (Double prob : origProbs)
				sum += prob;
			double rate = sum/origProbs.size();
			List<Double> modProbs = new ArrayList<>();
			for (int i=0; i<origProbs.size(); i++)
				modProbs.add(rate);
			return modProbs;
		}
		
	}
	
	public class ModProbERF extends AbstractERF {

		private AbstractERF erf;
		private double timeYears;
		private double origDuration;

		public ModProbERF(AbstractERF erf, double timeYears) {
			this.erf = erf;
			this.timeYears = timeYears;
			origDuration = erf.getTimeSpan().getDuration();
		}

		@Override
		public int getNumSources() {
			return erf.getNumSources();
		}

		@Override
		public ProbEqkSource getSource(int idx) {
			return new ModProbSource(erf.getSource(idx), origDuration, timeYears);
		}

		@Override
		public void updateForecast() {
			erf.updateForecast();
		}

		@Override
		public String getName() {
			return erf.getName();
		}
		
	}
	
	private class ModProbSource extends ProbEqkSource {
		
		private ProbEqkSource source;
		private double origDuration;
		private double newDuration;

		public ModProbSource(ProbEqkSource source, double origDuration, double newDuration) {
			this.source = source;
			this.origDuration = origDuration;
			this.newDuration = newDuration;
		}

		@Override
		public LocationList getAllSourceLocs() {
			return source.getAllSourceLocs();
		}

		@Override
		public RuptureSurface getSourceSurface() {
			return source.getSourceSurface();
		}

		@Override
		public double getMinDistance(Site site) {
			return source.getMinDistance(site);
		}

		@Override
		public int getNumRuptures() {
			return source.getNumRuptures();
		}

		@Override
		public ProbEqkRupture getRupture(int nRupture) {
			ProbEqkRupture origRup = source.getRupture(nRupture);
			double origRate = origRup.getMeanAnnualRate(origDuration);
			double newProb = 1d - Math.exp(-origRate*newDuration);
			return new ProbEqkRupture(origRup.getMag(), origRup.getAveRake(), newProb,
					origRup.getRuptureSurface(), origRup.getHypocenterLocation());
		}
		
	}

	public static void main(String[] args) throws IOException {
		File simsDir = new File("/home/kevin/OpenSHA/UCERF3/etas/simulations");
		File gitDir = new File("/home/kevin/git/cybershake-etas");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		File mappingsCSVFile = new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/u2_mapped_mappings.csv");
		
//		File configFile = new File(simsDir, "2019_01_11-2009BombayBeachM48-u2mapped-noSpont-10yr-8threads/config.json");
//		File configFile = new File(simsDir, "2019_01_11-2009BombayBeachM6-u2mapped-noSpont-10yr-8threads/config.json");
//		File configFile = new File(simsDir, "2019_01_11-MojavePointM6-u2mapped-noSpont-10yr-8threads/config.json");
		File configFile = new File(simsDir, "2019_01_11-ParkfieldM6-u2mapped-noSpont-10yr-8threads/config.json");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		Vs30_Source vs30Source = Vs30_Source.Wills2015;
		ETAS_Config config = ETAS_Config.readJSON(configFile);
		
		AttenRelRef gmpeRef = AttenRelRef.ASK_2014;
		
		boolean replotETAS = false;
		
		String[] highlightSiteNames = { "SBSM", "MRSD", "STNI" };
		double[] periods = { 3d, 5d, 10d };
		
		String dirPrefix = config.getSimulationName().replaceAll("\\W+", "_");
		File outputDir = new File(gitDir, dirPrefix);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		ETAS_Cybershake_TimeSpans[] timeSpans = ETAS_Cybershake_TimeSpans.values();
		
		ETAS_ScenarioPageGen pageGen = new ETAS_ScenarioPageGen(study, vs30Source, config, mappingsCSVFile, timeSpans,
				highlightSiteNames, ampsCacheDir, gmpeRef);
		
		File etasPlotsDir = new File(outputDir, "etas_plots");
		if (!etasPlotsDir.exists() || !new File(etasPlotsDir, "README.md").exists() || replotETAS) {
			System.out.println("Writing standard ETAS plots...");
			SimulationMarkdownGenerator.generateMarkdown(configFile, pageGen.catalogsFile, config, etasPlotsDir, false);
		}
		
		try {
			pageGen.generatePage(outputDir, periods);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			study.getDB().destroy();
		}
		System.exit(0);
	}

}
