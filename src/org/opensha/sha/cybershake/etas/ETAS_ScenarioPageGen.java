package org.opensha.sha.cybershake.etas;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
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
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;

import com.google.common.base.Preconditions;

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
	
	public ETAS_ScenarioPageGen(CyberShakeStudy study, Vs30_Source vs30Source, ETAS_Config etasConfig, File mappingsCSVFile,
			ETAS_Cybershake_TimeSpans[] timeSpans, String[] highlightSiteNames, File ampsCacheDir) throws IOException {
		this.study = study;
		this.timeSpans = timeSpans;
		this.etasConfig = etasConfig;
		
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
	}
	
	public void generatePage(File outputDir, double... periods) throws IOException {
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		StudyRotDProvider rawProv = new StudyRotDProvider(study, amps2db, periods, "CyberShake-TI");

		List<SimulationHazardCurveCalc<CSRupture>> tiCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		List<SimulationHazardCurveCalc<CSRupture>> tdCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		List<SimulationHazardCurveCalc<CSRupture>> etasCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		List<SimulationHazardCurveCalc<CSRupture>> etasUniformCalcs = new ArrayList<SimulationHazardCurveCalc<CSRupture>>();
		
		DiscretizedFunc xVals = SimulationHazardCurveCalc.getDefaultHazardCurve(4);
		for (int i=0; i<timeSpans.length; i++) {
			double durationYears = timeSpans[i].getTimeYears();
			tiCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					new NoEtasRupProbMod(study.getERF(), false, durationYears), "CyberShake-TI"), xVals));
			tdCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					new NoEtasRupProbMod(study.getERF(), true, durationYears), "CyberShake-TD"), xVals));
			etasCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					modProbConfigs[i].getRupProbModifier(), modProbConfigs[i].getRupVarProbModifier(), "CyberShake-ETAS"), xVals));
			etasUniformCalcs.add(new SimulationHazardCurveCalc<>(new StudyModifiedProbRotDProvider(rawProv,
					modProbConfigs[i].getRupProbModifier(), new UniformRVsRupVarProbMod(modProbConfigs[i].getRupVarProbModifier()),
					"CyberShake-ETAS Uniform"), xVals));
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
				
				table = MarkdownUtils.tableBuilder();
				
				table.initNewLine();
				table.addColumn("Time San");
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
			}
		}
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
		lines.add(tocIndex, "## Table Of Contents");

		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
	}
	
	private File plotHazardCurves(File resourcesDir, Site site, double period, ETAS_Cybershake_TimeSpans timeSpan,
			SimulationHazardCurveCalc<CSRupture> tiCalc, SimulationHazardCurveCalc<CSRupture> tdCalc,
			SimulationHazardCurveCalc<CSRupture> etasCalc, SimulationHazardCurveCalc<CSRupture> uniformEtasCalc)
					throws IOException {
		
		// actual duration handled by mod probs
		DiscretizedFunc tiCurve = tiCalc.calc(site, period, 1d);
		DiscretizedFunc tdCurve = tdCalc.calc(site, period, 1d);
		DiscretizedFunc etasCurve = etasCalc.calc(site, period, 1d);
		DiscretizedFunc uniformCurve = uniformEtasCalc.calc(site, period, 1d);
		
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
		
		PlotSpec spec = new PlotSpec(funcs, chars, site.getName()+" Hazard Curves",
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
		
		String prefix = "hazard_curves_"+site.getName()+"_"+optionalDigitDF.format(period)+"s_"+timeSpan.name().toLowerCase();
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

	public static void main(String[] args) throws IOException {
		File simsDir = new File("/home/kevin/OpenSHA/UCERF3/etas/simulations");
		File gitDir = new File("/home/kevin/git/cybershake-etas");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		File mappingsCSVFile = new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/u2_mapped_mappings.csv");
		
//		File configFile = new File(simsDir, "2019_01_11-2009BombayBeachM48-u2mapped-noSpont-10yr-8threads/config.json");
		File configFile = new File(simsDir, "2019_01_11-2009BombayBeachM6-u2mapped-noSpont-10yr-8threads/config.json");
//		File configFile = new File(simsDir, "2019_01_11-MojavePointM6-u2mapped-noSpont-10yr-8threads/config.json");
//		File configFile = new File(simsDir, "2019_01_11-ParkfieldM6-u2mapped-noSpont-10yr-8threads/config.json");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		Vs30_Source vs30Source = Vs30_Source.Wills2015;
		ETAS_Config config = ETAS_Config.readJSON(configFile);
		
		boolean replotETAS = false;
		
		String[] highlightSiteNames = { "SBSM", "MRSD", "STNI" };
		double[] periods = { 3d, 5d, 10d };
		
		String dirPrefix = config.getSimulationName().replaceAll("\\W+", "_");
		File outputDir = new File(gitDir, dirPrefix);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		ETAS_Cybershake_TimeSpans[] timeSpans = ETAS_Cybershake_TimeSpans.values();
		
		ETAS_ScenarioPageGen pageGen = new ETAS_ScenarioPageGen(study, vs30Source, config, mappingsCSVFile, timeSpans,
				highlightSiteNames, ampsCacheDir);
		
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
