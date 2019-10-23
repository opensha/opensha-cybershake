package org.opensha.sha.cybershake.etas;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.stat.StatUtils;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.HistogramFunction;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSetMath;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.ComparablePairing;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.IDPairing;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.refFaultParamDb.vo.FaultSectionPrefData;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.etas.ETASModProbConfig.ETAS_Cybershake_TimeSpans;
import org.opensha.sha.cybershake.maps.GMT_InterpolationSettings;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
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
import com.google.common.primitives.Doubles;

import scratch.UCERF3.FaultSystemRupSet;
import scratch.UCERF3.analysis.FaultBasedMapGen;
import scratch.UCERF3.erf.ETAS.ETAS_CatalogIO;
import scratch.UCERF3.erf.ETAS.ETAS_EqkRupture;
import scratch.UCERF3.erf.ETAS.ETAS_Params.ETAS_ParameterList;
import scratch.UCERF3.erf.ETAS.analysis.SimulationMarkdownGenerator;
import scratch.UCERF3.erf.ETAS.launcher.ETAS_Config;
import scratch.UCERF3.erf.ETAS.launcher.TriggerRupture;
import scratch.UCERF3.erf.ETAS.launcher.ETAS_Config.BinaryFilteredOutputConfig;
import scratch.kevin.cybershake.simCompare.CSRupture;
import scratch.kevin.cybershake.simCompare.StudyModifiedProbRotDProvider;
import scratch.kevin.cybershake.simCompare.StudyRotDProvider;
import scratch.kevin.simCompare.SimulationHazardCurveCalc;
import scratch.kevin.simCompare.SiteHazardCurveComarePageGen;

public class ETAS_ScenarioPageGen {
	
	private CyberShakeStudy study;
	
	private File catalogsFile;
	private List<? extends List<ETAS_EqkRupture>> catalogs;
	private Location scenarioLoc;
	
	private ETAS_Config etasConfig;
	private ETAS_Cybershake_TimeSpans[] timeSpans;
	private ETASModProbConfig[] modProbConfigs;
	
	private List<CybershakeRun> runs;
	private List<Site> sites;
	private List<Site> curveSites;
	
	private CachedPeakAmplitudesFromDB amps2db;

	private LoadingCache<CS_CurveKey, DiscretizedFunc> csCurveCache;
	private LoadingCache<GMPE_CurveKey, DiscretizedFunc> gmpeCurveCache;
	
	private Map<SimulationHazardCurveCalc<CSRupture>, File> csCurveCacheFiles;
	private Map<AbstractERF, File> gmpeCurveCacheFiles;
	
	private Map<SimulationHazardCurveCalc<CSRupture>, CSVFile<String>> csCurveCacheCSVs;
	private Map<AbstractERF, CSVFile<String>> gmpeCurveCacheCSVs;
	
	private HashSet<CSVFile<String>> unflushedCSVs;
	
	private AttenRelRef gmpeRef;
	private ModAttenRelRef directivityRef;
	private Deque<ScalarIMR> gmpeDeque;

	private DiscretizedFunc hiResXVals;
	private DiscretizedFunc standardResXVals;
	private DiscretizedFunc lnStandardResXVals;
	
	private ExecutorService exec;
	
	public ETAS_ScenarioPageGen(CyberShakeStudy study, Vs30_Source vs30Source, ETAS_Config etasConfig, File mappingsCSVFile,
			ETAS_Cybershake_TimeSpans[] timeSpans, String[] highlightSiteNames, File ampsCacheDir, AttenRelRef gmpeRef) throws IOException {
		this.study = study;
		this.timeSpans = timeSpans;
		this.etasConfig = etasConfig;
		this.gmpeRef = gmpeRef;
		
		if (etasConfig.getTriggerRuptures().size() == 1 && etasConfig.getTriggerRuptures().get(0) instanceof TriggerRupture.Point) {
			// get hypocenter
			scenarioLoc = etasConfig.getTriggerRuptures().get(0).buildRupture(null, Long.MIN_VALUE, new ETAS_ParameterList()).getHypocenterLocation();
		}
		
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
		
		csCurveCacheFiles = new HashMap<>();
		csCurveCacheCSVs = new HashMap<>();
		gmpeCurveCacheFiles = new HashMap<>();
		gmpeCurveCacheCSVs = new HashMap<>();
		unflushedCSVs = new HashSet<>();
		
		csCurveCache = CacheBuilder.newBuilder().build(new CacheLoader<CS_CurveKey, DiscretizedFunc>() {

			@Override
			public DiscretizedFunc load(CS_CurveKey key) throws Exception {
				DiscretizedFunc curve = key.calc.calc(key.site, key.period, 1d);
				CSVFile<String> csv = csCurveCacheCSVs.get(key.calc);
				addCurveToCSV(key.site, key.period, curve, csv);
				return curve;
			}
			
		});
		
		gmpeDeque = new ArrayDeque<>();
		directivityRef = ModAttenRelRef.BAYLESS_SOMERVILLE_2013_DIRECTIVITY;
		gmpeCurveCache = CacheBuilder.newBuilder().build(new CacheLoader<GMPE_CurveKey, DiscretizedFunc>() {

			@Override
			public DiscretizedFunc load(GMPE_CurveKey key) throws Exception {
				HazardCurveCalculator gmpeCalc = new HazardCurveCalculator();
				DiscretizedFunc curve = lnStandardResXVals.deepClone();
				ScalarIMR gmpe = checkOutGMPE();
				SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), key.period);
				gmpeCalc.getHazardCurve(curve, key.site, gmpe, key.erf);
				checkInGMPE(gmpe);
				DiscretizedFunc linearCurve = new ArbitrarilyDiscretizedFunc();
				for (int i=0; i<curve.size(); i++)
					linearCurve.set(standardResXVals.getX(i), curve.getY(i));
				linearCurve.setName(key.erf.getName());
				CSVFile<String> csv = gmpeCurveCacheCSVs.get(key.erf);
				addCurveToCSV(key.site, key.period, curve, csv);
				return linearCurve;
			}
			
		});
		
		exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}
	
	private int unflushed_curves = 0;
	
	private synchronized void addCurveToCSV(Site site, double period, DiscretizedFunc curve, CSVFile<String> csv) {
//		System.out.println("Adding curve for "+site.getName()+" to CSV");
		List<String> line = new ArrayList<>();
		Preconditions.checkNotNull(site.getName());
		line.add(site.getName());
		line.add(site.getLocation().getLatitude()+"");
		line.add(site.getLocation().getLongitude()+"");
		line.add(period+"");
		for (Point2D pt : curve)
			line.add(pt.getY()+"");
		// we truncate the curves beyond the minimum possible prob, so  there might be extra x-values missing
		int expectedXVals = csv.getNumCols()-4;
		int extraXVals = expectedXVals - curve.size();
		Preconditions.checkState(extraXVals >= 0);
		for (int i=0; i<extraXVals; i++)
			line.add("");
		csv.addLine(line);
		unflushedCSVs.add(csv);
		unflushed_curves++;
	}
	
	private void buildLoadCSV(AbstractERF erf, DiscretizedFunc xVals, File csvFile) throws IOException {
		CSVFile<String> csv;
		if (csvFile.exists()) {
			csv = CSVFile.readFile(csvFile, true);
			System.out.println("Loading "+(csv.getNumRows()-1)+" curves from "+csvFile.getName());
			List<String> header = csv.getLine(0);
			Preconditions.checkState(header.size() == xVals.size()+4, "X values inconsistent");
			for (int row=1; row<csv.getNumRows(); row++) {
				List<String> line = csv.getLine(row);
				double lat = Double.parseDouble(line.get(1));
				double lon = Double.parseDouble(line.get(2));
				double period = Double.parseDouble(line.get(3));
				Preconditions.checkState(line.size() == xVals.size()+4);
				DiscretizedFunc curve = new ArbitrarilyDiscretizedFunc();
				for (int i=0; i<xVals.size(); i++)
					curve.set(xVals.getX(i), Double.parseDouble(line.get(i+4)));
				curve.setName(erf.getName());
				GMPE_CurveKey key = new GMPE_CurveKey(new Location(lat, lon), period, erf);
				gmpeCurveCache.put(key, curve);
			}
		} else {
			csv = new CSVFile<>(true);
			List<String> header = new ArrayList<>();
			header.add("Site Name");
			header.add("Latitude");
			header.add("Longitude");
			header.add("Period (s)");
			for (Point2D pt : xVals)
				header.add(pt.getX()+"");
			csv.addLine(header);
		}
		gmpeCurveCacheCSVs.put(erf, csv);
		gmpeCurveCacheFiles.put(erf, csvFile);
	}
	
	private void buildLoadCSV(SimulationHazardCurveCalc<CSRupture> calc, File csvFile) throws IOException {
		DiscretizedFunc xVals = calc.getXVals();
		CSVFile<String> csv;
		if (csvFile.exists()) {
			csv = CSVFile.readFile(csvFile, true);
			System.out.println("Loading "+(csv.getNumRows()-1)+" curves from "+csvFile.getName());
			List<String> header = csv.getLine(0);
			Preconditions.checkState(header.size() == xVals.size()+4, "X values inconsistent");
			for (int row=1; row<csv.getNumRows(); row++) {
				List<String> line = csv.getLine(row);
				double lat = Double.parseDouble(line.get(1));
				double lon = Double.parseDouble(line.get(2));
				double period = Double.parseDouble(line.get(3));
				Preconditions.checkState(line.size() == xVals.size()+4);
				DiscretizedFunc curve = new ArbitrarilyDiscretizedFunc();
				for (int i=0; i<xVals.size(); i++) {
					String str = line.get(i+4);
					if (!str.isEmpty())
						// can be empty if past truncation
						curve.set(xVals.getX(i), Double.parseDouble(str));
				}
				curve.setName(calc.getSimProv().getName());
				CS_CurveKey key = new CS_CurveKey(new Location(lat, lon), period, calc);
				csCurveCache.put(key, curve);
			}
		} else {
			csv = new CSVFile<>(true);
			List<String> header = new ArrayList<>();
			header.add("Site Name");
			header.add("Latitude");
			header.add("Longitude");
			header.add("Period (s)");
			for (Point2D pt : xVals)
				header.add(pt.getX()+"");
			csv.addLine(header);
		}
		csCurveCacheCSVs.put(calc, csv);
		csCurveCacheFiles.put(calc, csvFile);
	}
	
	private synchronized void flushCurveCaches() throws IOException {
//		System.out.println("Flushing "+unflushedCSVs.size()+" files ("+unflushed_curves+" unflushed curves)");
		for (SimulationHazardCurveCalc<CSRupture> calc : csCurveCacheCSVs.keySet()) {
			CSVFile<String> csv = csCurveCacheCSVs.get(calc);
			File file = csCurveCacheFiles.get(calc);
//			System.out.println("CSV has "+(csv.getNumRows()-1)+" curves. Flush ? "+unflushedCSVs.contains(csv));
			if (unflushedCSVs.contains(csv)) {
				System.out.println("Flushing "+(csv.getNumRows()-1)+" curves to "+file.getName());
				csv.writeToFile(file);
				unflushedCSVs.remove(csv);
			}
		}
		for (AbstractERF erf : gmpeCurveCacheCSVs.keySet()) {
			CSVFile<String> csv = gmpeCurveCacheCSVs.get(erf);
			File file = gmpeCurveCacheFiles.get(erf);
//			System.out.println("CSV has "+(csv.getNumRows()-1)+" curves. Flush ? "+unflushedCSVs.contains(csv));
			if (unflushedCSVs.contains(csv)) {
				System.out.println("Flushing "+(csv.getNumRows()-1)+" curves to "+file.getName());
				csv.writeToFile(file);
				unflushedCSVs.remove(csv);
			}
		}
		unflushed_curves = 0;
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
	
	public void generatePage(File outputDir, double[] periods, double[] mapPeriods, boolean replotMaps) throws IOException, GMT_MapException {
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		File curvesDir = new File(outputDir, "curves");
		Preconditions.checkState(curvesDir.exists() || curvesDir.mkdir());
		
		StudyRotDProvider rawProv = new StudyRotDProvider(study, amps2db, periods, "CyberShake-TI");
		rawProv.setSpectraCacheDir(amps2db.getCacheDir());

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
			SimulationHazardCurveCalc<CSRupture> tiCalc = new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					new NoEtasRupProbMod(study.getERF(), false, durationYears), "CyberShake, TI"), standardResXVals);
			SimulationHazardCurveCalc<CSRupture> tdCalc = new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					new NoEtasRupProbMod(study.getERF(), true, durationYears), "CyberShake, TD"), standardResXVals);
			SimulationHazardCurveCalc<CSRupture> etasCalc = new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					modProbConfigs[i].getRupProbModifier(), modProbConfigs[i].getRupVarProbModifier(), "CyberShake, ETAS"), hiResXVals);
			SimulationHazardCurveCalc<CSRupture> etasUniformCalc = new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					modProbConfigs[i].getRupProbModifier(), new UniformRVsRupVarProbMod(modProbConfigs[i].getRupVarProbModifier()),
					"CyberShake, Uniform ETAS"), hiResXVals);
			
			buildLoadCSV(tiCalc, new File(curvesDir, "cs_ti_"+timeSpans[i].name()+".csv"));
			tiCalcs.add(tiCalc);
			buildLoadCSV(tdCalc, new File(curvesDir, "cs_td_"+timeSpans[i].name()+".csv"));
			tdCalcs.add(tdCalc);
			buildLoadCSV(etasCalc, new File(curvesDir, "cs_etas_"+timeSpans[i].name()+".csv"));
			etasCalcs.add(etasCalc);
			buildLoadCSV(etasUniformCalc, new File(curvesDir, "cs_etas_uniform_"+timeSpans[i].name()+".csv"));
			etasUniformCalcs.add(etasUniformCalc);
			
			AbstractERF myTI_ERF = new ModProbERF(tiERF, durationYears, gmpeRef.getShortName()+", TI");
			AbstractERF myTD_ERF = new ModProbERF(tdERF, durationYears, gmpeRef.getShortName()+", TD");
			AbstractERF myETAS_ERF = new ModNameERF(modProbConfigs[i].getModERFforGMPE(true),
					gmpeRef.getShortName()+"+"+directivityRef.getShortName().replaceAll("_", " ")+", ETAS");
			AbstractERF myETAS_UniformERF = new ModNameERF(modProbConfigs[i].getModERFforGMPE(false), gmpeRef.getShortName()+", ETAS");
			
			buildLoadCSV(myTI_ERF, standardResXVals, new File(curvesDir, gmpeRef.getShortName()+"_ti_"+timeSpans[i].name()+".csv"));
			tiERFs.add(myTI_ERF);
			buildLoadCSV(myTD_ERF, standardResXVals, new File(curvesDir, gmpeRef.getShortName()+"_td_"+timeSpans[i].name()+".csv"));
			tdERFs.add(myTD_ERF);
			buildLoadCSV(myETAS_ERF, standardResXVals, new File(curvesDir, gmpeRef.getShortName()+"_etas_"+timeSpans[i].name()+".csv"));
			etasERFs.add(myETAS_ERF);
			buildLoadCSV(myETAS_UniformERF, standardResXVals, new File(curvesDir, gmpeRef.getShortName()+"_etas_uniform_"+timeSpans[i].name()+".csv"));
			etasUniformERFs.add(myETAS_UniformERF);
		}
		
		List<SimulationHazardCurveCalc<CSRupture>> allCalcs = new ArrayList<>();
		allCalcs.addAll(tiCalcs);
		allCalcs.addAll(tdCalcs);
		allCalcs.addAll(etasUniformCalcs);
		allCalcs.addAll(etasCalcs);
		List<AbstractERF> allERFs = new ArrayList<>();
		allERFs.addAll(tiERFs);
		allERFs.addAll(tdERFs);
		allERFs.addAll(etasUniformERFs);
		allERFs.addAll(etasERFs);
		
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
		
		lines.add("## Mapping Information");
		lines.add("");
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Num Catalogs", etasConfig.getNumSimulations());
		List<Double> rvCounts = null;
		double countDuration = 0d;
		for (int i=0; i<timeSpans.length; i++) {
			if (timeSpans[i].getTimeYears() > countDuration) {
				countDuration = timeSpans[i].getTimeYears();
				rvCounts = modProbConfigs[i].getRVCounts();
			}
		}
		table.addLine("Mapped Ruptures", rvCounts.size());
		double[] countArray = Doubles.toArray(rvCounts);
		table.addLine("Mean variations per ruptures", (float)StatUtils.mean(countArray));
		table.addLine("Median variations per ruptures", (float)DataUtils.median(countArray));
		table.addLine("Min variations per ruptures", (float)StatUtils.min(countArray));
		table.addLine("Max variations per ruptures", (float)StatUtils.max(countArray));
		lines.addAll(table.build());
		lines.add("");
		
		lines.add("## Conditional Hypocenter Distributions");
		lines.add(topLink); lines.add("");
		
		FaultSystemRupSet rupSet = modProbConfigs[0].getSol().getRupSet();
		Map<Integer, String> parentSectNames = new HashMap<>();
		Map<Integer, Integer> triggeredParentCounts = new HashMap<>();
		int minRupsForParent = 100;
		for (List<ETAS_EqkRupture> catalog : catalogs) {
			for (ETAS_EqkRupture rup : catalog) {
				if (rup.getFSSIndex() < 0)
					continue;
				for (Integer parentID : rupSet.getParentSectionsForRup(rup.getFSSIndex())) {
					Integer count = triggeredParentCounts.get(parentID);
					if (count == null)
						count = 0;
					triggeredParentCounts.put(parentID, count+1);
				}
			}
		}
		List<Integer> sortedParentIDs = ComparablePairing.getSortedData(triggeredParentCounts);
		Collections.reverse(sortedParentIDs);
		if (sortedParentIDs.size() > 5)
			sortedParentIDs = sortedParentIDs.subList(0, 5);
		for (Integer parentID : sortedParentIDs) {
			Integer count = triggeredParentCounts.get(parentID);
			if (count < minRupsForParent)
				break;
			String parentSectionName = null;
			for (FaultSectionPrefData sect : rupSet.getFaultSectionDataList()) {
				if (sect.getParentSectionId() == parentID) {
					parentSectionName = sect.getParentSectionName();
					break;
				}
			}
			
			lines.add("### "+parentSectionName+" CHD");
			lines.add(topLink); lines.add("");
			
			System.out.println("Doing CHD for "+parentSectionName);
			
			File[] plots = plotCHDs(resourcesDir, rupSet, parentID, true);
			table = MarkdownUtils.tableBuilder();
			table.initNewLine();
			for (int i=0; i<timeSpans.length; i++)
				table.addColumn(timeSpans[i]);
			table.finalizeLine();
			table.initNewLine();
			for (int i=0; i<timeSpans.length; i++)
				table.addColumn("![CHD](resources/"+plots[i].getName()+")");
			table.finalizeLine();
			lines.addAll(table.build());
			lines.add("");
			
			plotCHDs(resourcesDir, rupSet, parentID, false); // build plots without CS as well
		}
		
		if (curveSites != null) {
			lines.add("## Hazard Curves");
			lines.add(topLink); lines.add("");
			for (Site site : curveSites) {
				try {
					rawProv.checkLoadCacheForSite(site, CyberShakeComponent.RotD50);
				} catch (ExecutionException e) {}
				lines.add("### "+site.getName()+" Hazard Curves");
				lines.add(topLink); lines.add("");
				
				File mapFile = new File(resourcesDir, site.getName()+"_location_map.png");
				if (!mapFile.exists())
					FileUtils.downloadURL(new URL(SiteHazardCurveComarePageGen.getMiniMap(site.getLocation())), mapFile);
				
				table = MarkdownUtils.tableBuilder();
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
				
				for (double period : mapPeriods) {
					lines.add("#### CyberShake "+site.getName()+" "+optionalDigitDF.format(period)+"s Hazard Gain Table");
					lines.add(topLink); lines.add("");
					
					DiscretizedFunc tiCurves[] = new DiscretizedFunc[timeSpans.length];
					DiscretizedFunc tdCurves[] = new DiscretizedFunc[timeSpans.length];
					DiscretizedFunc etasUniformCurves[] = new DiscretizedFunc[timeSpans.length];
					DiscretizedFunc etasCurves[] = new DiscretizedFunc[timeSpans.length];
					try {
						for (int i=0; i<timeSpans.length; i++) {
							tiCurves[i] = csCurveCache.get(new CS_CurveKey(site, period, tiCalcs.get(i)));
							tdCurves[i] = csCurveCache.get(new CS_CurveKey(site, period, tdCalcs.get(i)));
							etasUniformCurves[i] = csCurveCache.get(new CS_CurveKey(site, period, etasUniformCalcs.get(i)));
							etasCurves[i] = csCurveCache.get(new CS_CurveKey(site, period, etasCalcs.get(i)));
						}
					} catch (ExecutionException e) {
						throw ExceptionUtils.asRuntimeException(e);
					}
					
					lines.addAll(buildGainsTable(site, timeSpans, tiCurves, tdCurves, etasUniformCurves, etasCurves).build());
					lines.add("");
				}
				
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
				
				for (double period : mapPeriods) {
					lines.add("#### GMPE "+site.getName()+" "+optionalDigitDF.format(period)+"s Hazard Gain Table");
					lines.add(topLink); lines.add("");
					
					DiscretizedFunc tiCurves[] = new DiscretizedFunc[timeSpans.length];
					DiscretizedFunc tdCurves[] = new DiscretizedFunc[timeSpans.length];
					DiscretizedFunc etasUniformCurves[] = new DiscretizedFunc[timeSpans.length];
					DiscretizedFunc etasCurves[] = new DiscretizedFunc[timeSpans.length];
					try {
						for (int i=0; i<timeSpans.length; i++) {
							tiCurves[i] = gmpeCurveCache.get(new GMPE_CurveKey(site, period, tiERFs.get(i)));
							tdCurves[i] = gmpeCurveCache.get(new GMPE_CurveKey(site, period, tdERFs.get(i)));
							etasUniformCurves[i] = gmpeCurveCache.get(new GMPE_CurveKey(site, period, etasUniformERFs.get(i)));
							etasCurves[i] = gmpeCurveCache.get(new GMPE_CurveKey(site, period, etasERFs.get(i)));
						}
					} catch (ExecutionException e) {
						throw ExceptionUtils.asRuntimeException(e);
					}
					
					lines.addAll(buildGainsTable(site, timeSpans, tiCurves, tdCurves, etasUniformCurves, etasCurves).build());
					lines.add("");
				}
			}
			
			flushCurveCaches();
		}
		
		System.out.println("Caching spectra and curves for map generation...");
		
		for (Site site : sites) {
			try {
				List<Callable<DiscretizedFunc>> callables = new ArrayList<>();
				for (SimulationHazardCurveCalc<CSRupture> calc : allCalcs) {
					for (double period : mapPeriods) {
						CS_CurveKey key = new CS_CurveKey(site, period, calc);
						if (csCurveCache.getIfPresent(key) == null)
							callables.add(key);
					}
				}
				boolean needsCS = !callables.isEmpty();
				for (AbstractERF erf : allERFs) {
					for (double period : mapPeriods) {
						GMPE_CurveKey key = new GMPE_CurveKey(site, period, erf);
						if (gmpeCurveCache.getIfPresent(key) == null)
							callables.add(key);
					}
				}
				if (!callables.isEmpty()) {
					System.out.println("Caching/calculating "+callables.size()+" curves for "+site.getName());
					if (needsCS) {
						rawProv.checkWriteCacheForSite(site, CyberShakeComponent.RotD50);
						rawProv.checkLoadCacheForSite(site, CyberShakeComponent.RotD50);
					}
					List<Future<DiscretizedFunc>> futures = new ArrayList<>();
					for (Callable<DiscretizedFunc> callable : callables)
						futures.add(exec.submit(callable));
					for (Future<DiscretizedFunc> future : futures)
						future.get();
				}
				if (unflushed_curves > 500)
					flushCurveCaches();
			} catch (ExecutionException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		flushCurveCaches();
		
		System.out.println("Done caching spectra");
		
		FaultBasedMapGen.LOCAL_MAPGEN = true;
		
		lines.add("## Hazard Maps");
		lines.add(topLink); lines.add("");
		
		boolean[] isProbAtIMLs = { 	false,	false,	false,	true,	true,	true,	true };
		double[] vals =			 {	0.0001,	0.001,	0.01,	0.01,	0.1,	0.2,	0.5 };
		for (double period : mapPeriods) {
			String periodStr = optionalDigitDF.format(period)+"s";
			
			String heading = "###";
			if (mapPeriods.length > 1) {
				lines.add("### "+periodStr+" Hazard Maps");
				lines.add(topLink); lines.add("");
				heading += "#";
			}
			
			for (int t=0; t<timeSpans.length; t++) {
				String tsLabel = timeSpans[t].toString();
				String tsPrefix = timeSpans[t].name().toLowerCase();
				lines.add(heading+" "+periodStr+" "+tsLabel+" Hazard Maps");
				lines.add(topLink); lines.add("");
				
				TableBuilder gainsTable = MarkdownUtils.tableBuilder();
				gainsTable.addLine("Map Value", "CyberShake ETAS/TD Gain", "CyberShake ETAS/Uniform Gain",
						"GMPE ETAS/TD Gain", "GMPE ETAS/Uniform Gain");
				
				boolean logPlot = true;
				CPT poeCPT, imlCPT;
				CPT rawHazardCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
						"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
				rawHazardCPT.setBelowMinColor(rawHazardCPT.getMinColor());
				rawHazardCPT.setAboveMaxColor(rawHazardCPT.getMaxColor());
				if (logPlot) {
					poeCPT = rawHazardCPT.rescale(-8, -1);
					imlCPT = rawHazardCPT.rescale(-4, 0);
				} else {
					poeCPT = rawHazardCPT.rescale(-8, -1);
					imlCPT = rawHazardCPT.rescale(-4, 0);
				}
				
				for (int i=0; i<isProbAtIMLs.length; i++) {
					
					String imtLabel, imtPrefix;
					boolean isProbAtIML = isProbAtIMLs[i];
					double val = vals[i];
					CPT hazardCPT;
					if (isProbAtIML) {
						imtLabel = tsLabel+" POE "+(float)val+" (g) "+periodStr+" SA";
						imtPrefix = tsPrefix+"_"+periodStr+"_poe_"+(float)val+"g";
						hazardCPT = poeCPT;
					} else {
						imtLabel = tsLabel+" "+periodStr+" Sa (g) with POE="+(float)val;
						imtPrefix = tsPrefix+"_"+periodStr+"_iml_with_poe_"+(float)val;
						hazardCPT = imlCPT;
					}
					imtPrefix = "map_"+imtPrefix;
					
					lines.add(heading+"# "+imtLabel+", Maps");
					lines.add(topLink); lines.add("");
					
					lines.add(heading+"## "+imtLabel+", CyberShake Maps");
					lines.add(topLink); lines.add("");
					
					System.out.println("Calculating "+imtLabel+" CyberShake Maps");
					GeoDataSet csTI = calcCyberShakeHazardMap(tiCalcs.get(t), period, isProbAtIML, val);
					GeoDataSet csTD = calcCyberShakeHazardMap(tdCalcs.get(t), period, isProbAtIML, val);
					GeoDataSet csUniformETAS = calcCyberShakeHazardMap(etasUniformCalcs.get(t), period, isProbAtIML, val);
					GeoDataSet csETAS = calcCyberShakeHazardMap(etasCalcs.get(t), period, isProbAtIML, val);
					
					System.out.println("Plotting "+imtLabel+" CyberShake Maps");
					lines.addAll(buildMapLines(resourcesDir, imtLabel, imtPrefix, false, hazardCPT, logPlot,
							csTI, csTD, csUniformETAS, csETAS, replotMaps).build());
					lines.add("");
					
					lines.add(heading+"## "+imtLabel+", GMPE Maps");
					lines.add(topLink); lines.add("");
					
					System.out.println("Calculating "+imtLabel+" GMPE Maps");
					GeoDataSet gmpeTI = calcGMPEHazardMap(tiERFs.get(t), period, isProbAtIML, val);
					GeoDataSet gmpeTD = calcGMPEHazardMap(tdERFs.get(t), period, isProbAtIML, val);
					GeoDataSet gmpeUniformETAS = calcGMPEHazardMap(etasUniformERFs.get(t), period, isProbAtIML, val);
					GeoDataSet gmpeETAS = calcGMPEHazardMap(etasERFs.get(t), period, isProbAtIML, val);
					
					System.out.println("Plotting "+imtLabel+" GMPE Maps");
					lines.addAll(buildMapLines(resourcesDir, imtLabel, imtPrefix, true, hazardCPT, logPlot,
							gmpeTI, gmpeTD, gmpeUniformETAS, gmpeETAS, replotMaps).build());
					lines.add("");
					
					waitOnMaps();
					
					gainsTable.addLine("**"+imtLabel+"**",
							imageIfExists(resourcesDir, imtPrefix+"_cs_etas_td_gain.png"),
							imageIfExists(resourcesDir, imtPrefix+"_cs_etas_uni_gain.png"),
							imageIfExists(resourcesDir, imtPrefix+"_gmpe_etas_td_gain.png"),
							imageIfExists(resourcesDir, imtPrefix+"_gmpe_etas_uni_gain.png"));
				}
				
				lines.add(heading+" "+periodStr+" "+tsLabel+" EATS Gains Table");
				lines.add(topLink); lines.add("");
				lines.addAll(gainsTable.build());
				lines.add("");
			}
		}
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 5));
		lines.add(tocIndex, "## Table Of Contents");

		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
		
		exec.shutdown();
		try {
			exec.awaitTermination(120, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private class CS_CurveKey implements Callable<DiscretizedFunc> {
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
		
		public CS_CurveKey(Location loc, double period, SimulationHazardCurveCalc<CSRupture> calc) {
			this.site = null;
			this.loc = loc;
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

		@Override
		public DiscretizedFunc call() throws Exception {
			return csCurveCache.get(this);
		}
	}
	
	private class GMPE_CurveKey implements Callable<DiscretizedFunc> {
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
		
		public GMPE_CurveKey(Location loc, double period, AbstractERF erf) {
			this.site = null;
			this.loc = loc;
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

		@Override
		public DiscretizedFunc call() throws Exception {
			return gmpeCurveCache.get(this);
		}
	}
	
	private TableBuilder buildGainsTable(Site site, ETAS_Cybershake_TimeSpans[] timeSpans,
			DiscretizedFunc[] tiCurves, DiscretizedFunc[] tdCurves, DiscretizedFunc[] etasUniformCurves,
			DiscretizedFunc[] etasCurves) {
		TableBuilder table = MarkdownUtils.tableBuilder();
		
		table.initNewLine();
		table.addColumn("Dividend");
		table.addColumn("Divisor");
		for (ETAS_Cybershake_TimeSpans timeSpan : timeSpans) {
			table.addColumn(timeSpan.toString()+" Min");
			table.addColumn(timeSpan.toString()+" Max");
		}
		table.finalizeLine();

		addGainTableLine(table, tdCurves, tiCurves);
		addGainTableLine(table, etasUniformCurves, tdCurves);
		addGainTableLine(table, etasCurves, tdCurves);
		addGainTableLine(table, etasCurves, etasUniformCurves);
		
		return table;
	}
	
	private void addGainTableLine(TableBuilder table, DiscretizedFunc[] dividend, DiscretizedFunc[] divisor) {
		table.initNewLine();
		table.addColumn(dividend[0].getName());
		table.addColumn(divisor[0].getName());
		for (int i=0; i<dividend.length; i++) {
			double minMaxX = Double.min(dividend[i].getMaxX(), divisor[i].getMaxX());
			HashSet<Double> possibleXs = new HashSet<>();
			for (Point2D pt : dividend[i])
				if (pt.getX() <= minMaxX)
					possibleXs.add(pt.getX());
			for (Point2D pt : divisor[i])
				if (pt.getX() <= minMaxX)
					possibleXs.add(pt.getX());
			
			double min = Double.POSITIVE_INFINITY;
			double imlAtMin = Double.NaN;
			double max = Double.NEGATIVE_INFINITY;
			double imlAtMax = Double.NaN;
			for (double x : possibleXs) {
				if (x < 1e-2)
					continue;
				double gain = dividend[i].getInterpolatedY_inLogXLogYDomain(x) /
						divisor[i].getInterpolatedY_inLogXLogYDomain(x);
				if (!Double.isFinite(gain))
					continue;
				if (gain > max) {
					max = gain;
					imlAtMax = x;
				}
				if (gain < min) {
					min = gain;
					imlAtMin = x;
				}
			}
			
			table.addColumn(gainDF.format(min)+" at "+gainDF.format(imlAtMin)+" g");
			table.addColumn(gainDF.format(max)+" at "+gainDF.format(imlAtMax)+" g");
		}
		table.finalizeLine();
	}
	
	private static final DecimalFormat gainDF = new DecimalFormat("0.000");
	
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
		return plotHazardCurves(resourcesDir, site, period, site.getName()+" Empirical Hazard Curves", prefix, timeSpan,
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
		
		funcs.add(uniformCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 1.5f, Color.RED));
		
		funcs.add(etasCurve);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED));
		
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
	
	private class CSCurveCallable implements Callable<DiscretizedFunc> {
		
		private final CS_CurveKey key;

		public CSCurveCallable(CS_CurveKey key) {
			this.key = key;
		}

		@Override
		public DiscretizedFunc call() throws Exception {
			return csCurveCache.get(key);
		}
		
	}
	
	private class GMPECurveCallable implements Callable<DiscretizedFunc> {
		
		private final GMPE_CurveKey key;

		public GMPECurveCallable(GMPE_CurveKey key) {
			this.key = key;
		}

		@Override
		public DiscretizedFunc call() throws Exception {
			return gmpeCurveCache.get(key);
		}
		
	}
	
	private GeoDataSet calcCyberShakeHazardMap(SimulationHazardCurveCalc<CSRupture> calc, double period, boolean isProbAtIML, double value) {
		GeoDataSet ret = new ArbDiscrGeoDataSet(false);
		
//		Map<Site, Future<DiscretizedFunc>> futresMap = new HashMap<>();
//		
//		for (Site site : sites)
//			futresMap.put(site, exec.submit(new CSCurveCallable(new CS_CurveKey(site, period, calc))));
//		
//		for (Site site : sites) {
//			DiscretizedFunc curve;
//			try {
//				curve = futresMap.get(site).get();
//			} catch (ExecutionException | InterruptedException e) {
//				throw ExceptionUtils.asRuntimeException(e);
//			}
		for (Site site : sites) {
			DiscretizedFunc curve;
			try {
				curve = new CSCurveCallable(new CS_CurveKey(site, period, calc)).call();
			} catch (Exception e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			double z = getCurveVal(curve, isProbAtIML, value);
			ret.set(site.getLocation(), z);
		}
		
		return ret;
	}
	
	private GeoDataSet calcGMPEHazardMap(AbstractERF erf, double period, boolean isProbAtIML, double value) {
		GeoDataSet ret = new ArbDiscrGeoDataSet(false);
		
		Map<Site, Future<DiscretizedFunc>> futresMap = new HashMap<>();
		
		for (Site site : sites)
			futresMap.put(site, exec.submit(new GMPECurveCallable(new GMPE_CurveKey(site, period, erf))));
		
		for (Site site : sites) {
			DiscretizedFunc curve;
			try {
				curve = futresMap.get(site).get();
			} catch (ExecutionException | InterruptedException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			double z = getCurveVal(curve, isProbAtIML, value);
			ret.set(site.getLocation(), z);
		}
		
		return ret;
	}
	
	private double getCurveVal(DiscretizedFunc curve, boolean isProbAtIML, double value) {
		if (isProbAtIML) {
			if (value > curve.getMaxX())
				return 0d;
			return curve.getInterpolatedY_inLogXLogYDomain(value);
		}
		if (value > curve.getMaxY())
			// nothing with probability this high
			return 0d;
		if (value < curve.getMinY())
			// everything above this probability
			return curve.getMaxX();
		return curve.getFirstInterpolatedX_inLogXLogYDomain(value);
	}
	
	private TableBuilder buildMapLines(File resourcesDir, String imtLabel, String imtPrefix, boolean gmpe, CPT hazardCPT,
			boolean logPlot, GeoDataSet ti, GeoDataSet td, GeoDataSet etasUniform, GeoDataSet etas, boolean replotMaps)
					throws GMT_MapException, IOException {
		String labelAdd = gmpe ? gmpeRef.getShortName() : "CS";
		if (gmpe)
			imtPrefix += "_gmpe";
		else
			imtPrefix += "_cs";
		
		if (logPlot)
			labelAdd = "Log@-10@- "+labelAdd;
		
		// regular maps
		plotMap(logPlot ? asLog10(ti, hazardCPT.getMinValue()) : ti,
				labelAdd+", TI, "+imtLabel, hazardCPT, resourcesDir, imtPrefix+"_ti", replotMaps);
		plotMap(logPlot ? asLog10(td, hazardCPT.getMinValue()) : td,
				labelAdd+", TD, "+imtLabel, hazardCPT, resourcesDir, imtPrefix+"_td", replotMaps);
		plotMap(logPlot ? asLog10(etasUniform, hazardCPT.getMinValue()) : etasUniform,
				labelAdd+", Uniform ETAS, "+imtLabel, hazardCPT, resourcesDir, imtPrefix+"_etas_uni", replotMaps);
		plotMap(logPlot ? asLog10(etas, hazardCPT.getMinValue()) : etas,
				labelAdd+", ETAS, "+imtLabel, hazardCPT, resourcesDir, imtPrefix+"_etas", replotMaps);
		
		imtLabel = imtLabel.replaceAll(" (g)", "");
		labelAdd = labelAdd.replaceAll("Log@-10@- ", "");
		
		// diffs/gains
		plotDiffGainMaps(td, ti, labelAdd+", TD - TI, "+imtLabel, labelAdd+", TD/TI, "+imtLabel,
				resourcesDir, imtPrefix+"_td_ti", replotMaps);
		plotDiffGainMaps(etasUniform, ti, labelAdd+", UniETAS - TI, "+imtLabel, labelAdd+", UniETAS/TI, "+imtLabel,
				resourcesDir, imtPrefix+"_etas_uni_ti", replotMaps);
		plotDiffGainMaps(etas, ti, labelAdd+", ETAS - TI, "+imtLabel, labelAdd+", ETAS/TI, "+imtLabel,
				resourcesDir, imtPrefix+"_etas_ti", replotMaps);
		plotDiffGainMaps(etasUniform, td, labelAdd+", UniETAS - TD, "+imtLabel, labelAdd+", UniETAS/TD, "+imtLabel,
				resourcesDir, imtPrefix+"_etas_uni_td", replotMaps);
		plotDiffGainMaps(etas, td, labelAdd+", ETAS - TD, "+imtLabel, labelAdd+", ETAS/TD, "+imtLabel,
				resourcesDir, imtPrefix+"_etas_td", replotMaps);
		plotDiffGainMaps(etas, etasUniform, labelAdd+", ETAS - Uni, "+imtLabel, labelAdd+", ETAS/Uni, "+imtLabel,
				resourcesDir, imtPrefix+"_etas_uni", replotMaps);
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		
		waitOnMaps();
		
		table.addLine("", labelAdd+"-TI", labelAdd+"-TD", labelAdd+"-ETAS-Uniform", labelAdd+"-ETAS");
		table.addLine("**"+labelAdd+"-TI**",
				imageIfExists(resourcesDir, imtPrefix+"_ti.png"),
				imageIfExists(resourcesDir, imtPrefix+"_td_ti_diff.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni_ti_diff.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_ti_diff.png"));
		table.addLine("**"+labelAdd+"-TD**", 
				imageIfExists(resourcesDir, imtPrefix+"_td_ti_gain.png"),
				imageIfExists(resourcesDir, imtPrefix+"_td.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni_td_diff.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_td_diff.png"));
		table.addLine("**"+labelAdd+"-ETAS (Uniform)**",
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni_ti_gain.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni_td_gain.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni_diff.png"));
		table.addLine("**"+labelAdd+"-ETAS**",
				imageIfExists(resourcesDir, imtPrefix+"_etas_ti_gain.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_td_gain.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas_uni_gain.png"),
				imageIfExists(resourcesDir, imtPrefix+"_etas.png"));
		
		return table;
	}
	
	private static String imageIfExists(File resourcesDir, String fileName) {
		File file = new File(resourcesDir, fileName);
		if (file.exists())
			return "![Plot](resources/"+fileName+")";
		return "*(N/A)*";
	}
	
	private static GeoDataSet asLog10(GeoDataSet xyz, double minVal) {
		GeoDataSet ret = xyz.copy();
		ret.log10();
		for (int i=0; i<ret.size(); i++)
			if (!Double.isFinite(ret.get(i)))
				ret.set(i, minVal);
		return ret;
	}
	
	private static final double interp_spacing = 0.02;
	
	private static CPT divergentCPT;
	
	private synchronized static CPT getDivergentCPT(double min, double max) {
		if (divergentCPT == null) {
			// original CyberShake one
//			divergent = new CPT(-1d, 1d,
//					new Color(0, 104, 55),
//					new Color(26, 152, 80),
//					new Color(102, 189, 99),
//					new Color(166, 217, 106),
//					new Color(217, 239, 139),
//					new Color(255, 255, 191),
//					new Color(254, 224, 139),
//					new Color(253, 174, 97),
//					new Color(244, 109, 67),
//					new Color(215, 48, 39),
//					new Color(165, 0, 38));
			// modified to be slightly whiter in the middle and then get darker sooner
			divergentCPT = new CPT(-1d, 1d,
					new Color(0, 104, 55),
					new Color(26, 152, 80),
					new Color(102, 189, 99),
					new Color(158, 217, 117),
					new Color(206, 239, 170),
					new Color(255, 255, 230),
					new Color(254, 213, 170),
					new Color(253, 165, 107),
					new Color(244, 109, 67),
					new Color(215, 48, 39),
					new Color(165, 0, 38));
		}
		return divergentCPT.rescale(min, max);
	}
	
	private boolean plotDiffGainMaps(GeoDataSet xyz1, GeoDataSet xyz2, String diffLabel, String gainLabel, File resourcesDir, String prefix, boolean replotMaps)
			throws GMT_MapException, IOException {
		GeoDataSet diffXYZ = GeoDataSetMath.subtract(xyz1, xyz2);
		GeoDataSet gainXYZ = GeoDataSetMath.divide(xyz1, xyz2);
		gainXYZ.log10();
		gainLabel = "Log@-10@- "+gainLabel;
		
		double maxDiff = Math.max(Math.abs(diffXYZ.getMinZ()), Math.abs(diffXYZ.getMaxZ()));
		if (maxDiff == 0d)
			return false;
		CPT gainCPT = getDivergentCPT(-2, 2);
		CPT diffCPT = getDivergentCPT(-maxDiff, maxDiff);
		
		if (!plotMap(gainXYZ, gainLabel, gainCPT, resourcesDir, prefix+"_gain", replotMaps))
			return false;
		plotMap(diffXYZ, diffLabel, diffCPT, resourcesDir, prefix+"_diff", replotMaps);
		return true;
	}
	
	private ArrayDeque<Future<?>> mapFutures = new ArrayDeque<>();
	
	private void waitOnMaps() {
		if (map_parallel)
			System.out.println("Waiting on "+mapFutures.size()+" futures");
		while (!mapFutures.isEmpty()) {
			try {
				mapFutures.pop().get();
			} catch (InterruptedException | ExecutionException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
	}
	
	private static boolean map_parallel = true;
	
	private boolean plotMap(GeoDataSet xyz, String label, CPT cpt, File resourcesDir, String prefix, boolean replotMaps)
			throws GMT_MapException, IOException {
		if (!replotMaps && new File(resourcesDir, prefix+".png").exists())
			return true;
		boolean hasFinite = false;
		for (int i=0; i<xyz.size(); i++) {
			if (Double.isFinite(xyz.get(i))) {
				hasFinite = true;
				break;
			}
		}
		if (!hasFinite)
			return false;
//		if (Double.isNaN(Double.NaN))
//			return;
		if (map_parallel) {
			mapFutures.add(exec.submit(new Runnable() {
				
				@Override
				public void run() {
					try {
						doPlotMap(xyz, label, cpt, resourcesDir, prefix);
					} catch (GMT_MapException | IOException e) {
						throw ExceptionUtils.asRuntimeException(e);
					}
				}
			}));
		} else {
			doPlotMap(xyz, label, cpt, resourcesDir, prefix);
		}
		
		return true;
	}
	
	private void doPlotMap(GeoDataSet xyz, String label, CPT cpt, File resourcesDir, String prefix)
			throws GMT_MapException, IOException {
		cpt.setNanColor(Color.GRAY);
		
		GMT_Map map = new GMT_Map(study.getRegion(), xyz, interp_spacing, cpt);
		map.setInterpSettings(GMT_InterpolationSettings.getDefaultSettings());
		map.setTopoResolution(TopographicSlopeFile.US_SIX);
		map.setMaskIfNotRectangular(true);
		map.setCustomLabel(label);
		map.setBlackBackground(false);
		map.setRescaleCPT(false);
		map.setCustomScaleMin((double)cpt.getMinValue());
		map.setCustomScaleMax((double)cpt.getMaxValue());
		
		ArrayList<PSXYSymbol> xySymbols = new ArrayList<>();
		for (Location loc : xyz.getLocationList()) {
			xySymbols.add(new PSXYSymbol(new Point2D.Double(loc.getLongitude(), loc.getLatitude()),
					PSXYSymbol.Symbol.INVERTED_TRIANGLE, 0.05f, 0.01f, Color.BLACK, Color.BLACK));
		}
		
		if (scenarioLoc != null)
			xySymbols.add(new PSXYSymbol(new Point2D.Double(scenarioLoc.getLongitude(), scenarioLoc.getLatitude()),
					PSXYSymbol.Symbol.STAR, 0.2f, 0.01f, Color.RED.darker(), Color.RED.darker()));
		
		map.setSymbols(xySymbols);
		
		System.out.println("Data range for "+prefix+": "+xyz.getMinZ()+" "+xyz.getMaxZ());
		
		FaultBasedMapGen.plotMap(resourcesDir, prefix, false, map);
	}
	
	private File[] plotCHDs(File resourcesDir, FaultSystemRupSet rupSet, int parentSectionID, boolean plotCS) throws IOException {
		File[] ret = new File[timeSpans.length];
		
		List<FaultSectionPrefData> sects = new ArrayList<>();
		for (FaultSectionPrefData sect : rupSet.getFaultSectionDataList())
			if (sect.getParentSectionId() == parentSectionID)
				sects.add(sect);
		Preconditions.checkState(!sects.isEmpty());
		
		String parentName = sects.get(0).getParentSectionName();
		String parentPrefix = parentName.replaceAll("\\W+", "_");
		
		HashSet<Integer> rupIDs = new HashSet<>(rupSet.getRupturesForParentSection(parentSectionID));
		
		Location firstLoc = sects.get(0).getFaultTrace().first();
		Location lastLoc = sects.get(sects.size()-1).getFaultTrace().last();
		double latSpan = Math.abs(firstLoc.getLatitude() - lastLoc.getLatitude());
		double lonSpan = Math.abs(firstLoc.getLongitude() - lastLoc.getLongitude());
		
		boolean latitude = latSpan > lonSpan;
		
		double minX, maxX;
		if (latitude) {
			minX = Math.min(firstLoc.getLatitude(), lastLoc.getLatitude());
			maxX = Math.max(firstLoc.getLatitude(), lastLoc.getLatitude());
		} else {
			minX = Math.min(firstLoc.getLongitude(), lastLoc.getLongitude());
			maxX = Math.max(firstLoc.getLongitude(), lastLoc.getLongitude());
		}
		
		for (int t=0; t<timeSpans.length; t++) {
			ETASModProbConfig modProbConfig = modProbConfigs[t];
			
			// build list of raw hypocenter locations
			List<Location> rawHypos = new ArrayList<>();
			List<List<ETAS_EqkRupture>> catalogs = modProbConfig.getCatalogs();
			HashSet<Integer> triggeredMatchingRupIDs = new HashSet<>();
			for (List<ETAS_EqkRupture> catalog : catalogs) {
				for (ETAS_EqkRupture rup : catalog) {
					if (rup.getFSSIndex() >= 0 && rupIDs.contains(rup.getFSSIndex())) {
						triggeredMatchingRupIDs.add(rup.getFSSIndex());
						Location hypo = rup.getHypocenterLocation();
						// find out if hypo is on this section
						
						if (isHypocenterOnParentSect(rup.getFSSIndex(), hypo, rupSet, parentSectionID)) {
							// it's on this section
							rawHypos.add(hypo);
						}
					}
				}
			}
			
			HistogramFunction csCHD = null;
			HistogramFunction csUniformCHD = null;
			List<Location> mappedHypos = null;
			List<Location> csUniformHypos = null;
			if (plotCS) {
				// build list of mapped hypocenters
				mappedHypos = new ArrayList<>();
				csUniformHypos = new ArrayList<>();
				Map<Integer, IDPairing> fssToERFmappings = modProbConfig.getRupMappingTable();
				Map<IDPairing, Map<Integer, Double>> rvCounts = modProbConfig.getRVOccuranceCounts();
				List<Double> mappedFractionalOccurences = new ArrayList<>();
				List<Double> csUniformWeights = new ArrayList<>();
				for (int fssIndex : triggeredMatchingRupIDs) {
					IDPairing erfMapping = fssToERFmappings.get(fssIndex);
					if (erfMapping == null)
						continue;
					Map<Integer, Location> hypoLocs = modProbConfig.getRVHypocenters(erfMapping);
					Map<Integer, Double> rvOccurs = rvCounts.get(erfMapping);
					double uniWeight = 0d;
					if (rvOccurs != null) {
						for (Integer rvID : rvOccurs.keySet()) {
							if (rvOccurs.get(rvID) == 0d)
								continue;
							Location hypo = hypoLocs.get(rvID);
							if (isHypocenterOnParentSect(fssIndex, hypo, rupSet, parentSectionID)) {
								// it's on this section
								mappedHypos.add(hypo);
								double weight = rvOccurs.get(rvID);
								uniWeight += weight;
								mappedFractionalOccurences.add(weight);
							}
						}
					}
					for (Location hypo : hypoLocs.values()) {
						if (isHypocenterOnParentSect(fssIndex, hypo, rupSet, parentSectionID)) {
							csUniformHypos.add(hypo);
							csUniformWeights.add(uniWeight);
						}
					}
				}
				csCHD = calcCHD(mappedHypos, mappedFractionalOccurences, latitude, minX, maxX);
				csUniformCHD = calcCHD(csUniformHypos, csUniformWeights, latitude, minX, maxX);
			}
			
			
			HistogramFunction etasCHD = calcCHD(rawHypos, null, latitude, minX, maxX);
			
			List<XY_DataSet> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			funcs.add(etasCHD);
			etasCHD.setName("ETAS CHD");
			chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 3f, Color.BLACK));
			
			if (plotCS) {
				funcs.add(csCHD);
				csCHD.setName("CyberShake Mapped CHD");
				chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 3f, Color.BLUE));
				
				funcs.add(csUniformCHD);
				csUniformCHD.setName("CyberShake Uniform CHD");
				chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, PlotSymbol.CIRCLE, 3f, Color.RED));
			}
			
			String title = timeSpans[t]+" "+parentName+" CHDs";
			String prefix = "chd_"+parentPrefix+"_"+timeSpans[t].name();
			if (!plotCS)
				prefix += "_etas_only";
			
			String xAxisLabel = latitude ? "Latitude (degrees)" : "Longitude (degrees)";
			
			PlotSpec spec = new PlotSpec(funcs, chars, title, xAxisLabel, "Conditional Probability");
			spec.setLegendVisible(plotCS);
			
			PlotPreferences plotPrefs = PlotPreferences.getDefault();
			plotPrefs.setTickLabelFontSize(18);
			plotPrefs.setAxisLabelFontSize(20);
			plotPrefs.setPlotLabelFontSize(21);
			plotPrefs.setBackgroundColor(Color.WHITE);
			
			HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
			
			Range xRange = new Range(minX, maxX);
			Range yRange = new Range(0d, 1d);
			
			gp.drawGraphPanel(spec, false, false, xRange, yRange);
			gp.getChartPanel().setSize(800, 600);
			
			File pngFile = new File(resourcesDir, prefix+".png");
			gp.saveAsPNG(pngFile.getAbsolutePath());
			File pdfFile = new File(resourcesDir, prefix+".pdf");
			gp.saveAsPDF(pdfFile.getAbsolutePath());
			
			// now make hypo plot
			funcs = new ArrayList<>();
			chars = new ArrayList<>();
			
			XY_DataSet etasHypoFunc = getHypoFunc(rawHypos, latitude);
			etasHypoFunc.setName("ETAS Hypocenters");
			funcs.add(etasHypoFunc);
			chars.add(new PlotCurveCharacterstics(PlotSymbol.BOLD_X, 4f, Color.BLACK));
			
			double maxDepth = etasHypoFunc.getMaxY();
			
			if (plotCS) {
				XY_DataSet csHypoFunc = getHypoFunc(mappedHypos, latitude);
				csHypoFunc.setName("CyberShake Mapped Hypocenters");
				funcs.add(csHypoFunc);
				chars.add(new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 2f, new Color(0, 0, 255, 180)));
				
				XY_DataSet csUniFunc = getHypoFunc(csUniformHypos, latitude);
				csUniFunc.setName("CyberShake Uniform Hypocenters");
				funcs.add(csUniFunc);
				chars.add(new PlotCurveCharacterstics(PlotSymbol.CIRCLE, 1f, new Color(255, 0, 0, 127)));
				
				maxDepth = Math.max(maxDepth, csUniFunc.getMaxY());
			}
			
			if (scenarioLoc != null) {
				double minDist = Double.POSITIVE_INFINITY;
				for (FaultSectionPrefData sect : sects)
					for (Location loc : sect.getFaultTrace())
						minDist = Math.min(minDist, LocationUtils.horzDistanceFast(loc, scenarioLoc));
				if (minDist < 10d) {
					XY_DataSet scenarioXY = new DefaultXY_DataSet();
					scenarioXY.set(latitude ? scenarioLoc.getLatitude() : scenarioLoc.getLongitude(), scenarioLoc.getDepth());
					funcs.add(0, scenarioXY);
					chars.add(0, new PlotCurveCharacterstics(PlotSymbol.FILLED_INV_TRIANGLE, 8, Color.GREEN.darker()));
				}
			}
			
			PlotSpec hypoSpec = new PlotSpec(funcs, chars, title, xAxisLabel, "Depth (km)");
			spec.setLegendVisible(plotCS);
			
			Range depthRange = new Range(0d, maxDepth);

			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
			gp.drawGraphPanel(hypoSpec, false, false, xRange, depthRange);
			gp.getYAxis().setInverted(true);
			gp.getChartPanel().setSize(800, 600);
			
			pngFile = new File(resourcesDir, prefix+"_hypos.png");
			gp.saveAsPNG(pngFile.getAbsolutePath());
			
			// now plot combined
			List<PlotSpec> specs = new ArrayList<>();
			List<Range> xRanges = new ArrayList<>();
			List<Range> yRanges = new ArrayList<>();
			
			etasCHD.setName("ETAS");
			if (plotCS) {
				csCHD.setName("CyberShake Mapped");
				csUniformCHD.setName("CyberShake Uniform");
			}
			
			specs.add(spec);
			specs.add(hypoSpec);
			
			xRanges.add(xRange);
			yRanges.add(yRange);
			yRanges.add(depthRange);

			gp.setRenderingOrder(DatasetRenderingOrder.FORWARD);
			gp.drawGraphPanel(specs, false, false, xRanges, yRanges);
			gp.getYAxis().setInverted(false);
			CombinedDomainXYPlot plot = (CombinedDomainXYPlot)gp.getPlot();
			((XYPlot)plot.getSubplots().get(1)).getRangeAxis().setInverted(true);
			((XYPlot)plot.getSubplots().get(0)).setWeight(6);
			((XYPlot)plot.getSubplots().get(1)).setWeight(10);
			((XYPlot)plot.getSubplots().get(1)).setDatasetRenderingOrder(DatasetRenderingOrder.REVERSE);
			gp.getChartPanel().setSize(800, 1000);
			
			pngFile = new File(resourcesDir, prefix+"_hypos_combined.png");
			gp.saveAsPNG(pngFile.getAbsolutePath());
			
			ret[t] = pngFile;
		}
		
		return ret;
	}
	
	private HistogramFunction calcCHD(List<Location> hypos, List<Double> scalars, boolean latitude, double minX, double maxX) {
//		HistogramFunction chd = HistogramFunction.getEncompassingHistogram(minX, maxX, (maxX - minX)/20d);
		HistogramFunction chd = new HistogramFunction(minX, maxX, 21);
		chd = new HistogramFunction(minX+0.5*chd.getDelta(), 20, chd.getDelta());
		Preconditions.checkState(scalars == null || scalars.size() == hypos.size());
		
		for (int i=0; i<hypos.size(); i++) {
			Location hypo = hypos.get(i);
			double x = latitude ? hypo.getLatitude() : hypo.getLongitude();
			
			int index = chd.getClosestXIndex(x);
			double value = scalars == null ? 1d : scalars.get(i);
			
			chd.add(index, value);
		}
		
		chd.normalizeBySumOfY_Vals();
		
		return chd;
	}
	
	private XY_DataSet getHypoFunc(List<Location> hypos, boolean latitude) {
		double[] xs = new double[hypos.size()];
		double[] ys = new double[hypos.size()];
		for (int i=0; i<hypos.size(); i++) {
			Location hypo = hypos.get(i);
			if (latitude)
				xs[i] = hypo.getLatitude();
			else
				xs[i] = hypo.getLongitude();
			ys[i] = hypo.getDepth();
		}
		return new LightFixedXFunc(xs, ys);
	}
	
	private boolean isHypocenterOnParentSect(int fssIndex, Location hypo, FaultSystemRupSet rupSet, int parentSectionID) {
		double minDist = Double.POSITIVE_INFINITY;
		boolean closestIsMatch = false;
		for (FaultSectionPrefData sect : rupSet.getFaultSectionDataForRupture(fssIndex)) {
			for (Location loc : sect.getFaultTrace()) {
				double dist = LocationUtils.horzDistanceFast(hypo, loc);
				if (dist < minDist) {
					minDist = dist;
					closestIsMatch = sect.getParentSectionId() == parentSectionID;
				}
			}
		}
		return closestIsMatch;
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
	
	private class ModNameERF extends AbstractERF {

		private AbstractERF erf;
		private String name;

		public ModNameERF(AbstractERF erf, String name) {
			this.erf = erf;
			this.name = name;
		}

		@Override
		public int getNumSources() {
			return erf.getNumSources();
		}

		@Override
		public ProbEqkSource getSource(int idx) {
			return erf.getSource(idx);
		}

		@Override
		public void updateForecast() {
			erf.updateForecast();
		}

		@Override
		public String getName() {
			return name;
		}
		
	}
	
	private class ModProbERF extends AbstractERF {

		private AbstractERF erf;
		private double timeYears;
		private double origDuration;
		private String name;

		public ModProbERF(AbstractERF erf, double timeYears, String name) {
			this.erf = erf;
			this.timeYears = timeYears;
			origDuration = erf.getTimeSpan().getDuration();
			this.name = name;
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
			return name;
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
		
		File[] configFiles = {
				new File(simsDir, "2019_04_25-2009BombayBeachM48-u2mapped-noSpont-10yr/config.json"),
				new File(simsDir, "2019_04_25-2009BombayBeachM6-u2mapped-noSpont-10yr/config.json"),
				new File(simsDir, "2019_04_25-MojavePointM6-u2mapped-noSpont-10yr/config.json"),
				new File(simsDir, "2019_04_25-ParkfieldM6-u2mapped-noSpont-10yr/config.json")
		};
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		Vs30_Source vs30Source = Vs30_Source.Wills2015;
		
		AttenRelRef gmpeRef = AttenRelRef.ASK_2014;
		
		boolean replotETAS = false;
		boolean replotMaps = false;
		
		String[] highlightSiteNames = { "SBSM", "MRSD", "STNI", "PDU" };
		double[] periods = { 3d, 5d, 10d };
		double[] mapPeriods = { 5d };
		
		ETAS_Cybershake_TimeSpans[] timeSpans = ETAS_Cybershake_TimeSpans.values();
		
		for (File configFile : configFiles) {
			ETAS_Config config = ETAS_Config.readJSON(configFile);
			String dirPrefix = config.getSimulationName().replaceAll("\\W+", "_");
			File outputDir = new File(gitDir, dirPrefix);
			Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
			
			ETAS_ScenarioPageGen pageGen = new ETAS_ScenarioPageGen(study, vs30Source, config, mappingsCSVFile, timeSpans,
					highlightSiteNames, ampsCacheDir, gmpeRef);
			
			File etasPlotsDir = new File(outputDir, "etas_plots");
			if (!etasPlotsDir.exists() || !new File(etasPlotsDir, "README.md").exists() || replotETAS) {
				System.out.println("Writing standard ETAS plots...");
				SimulationMarkdownGenerator.generateMarkdown(configFile, pageGen.catalogsFile,
						config, etasPlotsDir, false, 0, 1, false, false, null);
			}
			
			try {
				pageGen.generatePage(outputDir, periods, mapPeriods, replotMaps);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.flush();
				System.exit(0);
			}
			pageGen.exec.shutdown();
		}
		study.getDB().destroy();
		System.exit(0);
	}

}
