package scratch.kevin.cybershake.ugms;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.disaggregation.DisaggregationCalculator;
import org.opensha.sha.cybershake.calc.mcer.MCERDataProductsCalc;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.plot.DisaggregationPlotter;
import org.opensha.sha.cybershake.plot.PlotType;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.param.IncludeBackgroundOption;
import org.opensha.sha.earthquake.param.IncludeBackgroundParam;
import org.opensha.sha.faultSurface.cache.CacheEnabledSurface;
import org.opensha.sha.gui.infoTools.DisaggregationPlotViewerWindow;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

import scratch.UCERF3.erf.mean.MeanUCERF3;
import scratch.UCERF3.erf.mean.MeanUCERF3.Presets;

public class DisaggBenchmarch {
	
	private enum Config {
		ONE_LEVEL_ONE_PERIOD(false, false),
		ONE_LEVEL_MULTI_PERIOD(false, true),
		MULTI_LEVEL_ONE_PERIOD(true, false),
		MULTI_LEVEL_MULTI_PERIOD(true, true);
		
		private boolean multiLevel;
		private boolean multiPeriod;
		private Config(boolean multiLevel, boolean multiPeriod) {
			this.multiLevel = multiLevel;
			this.multiPeriod = multiPeriod;
		}
		
		public List<Double> getCalcPeriods(double[] periods) {
			List<Double> list = Doubles.asList(periods);
			if (!multiPeriod)
				list = list.subList(0, 1);
			else
				Preconditions.checkState(list.size() > 1);
			return list;
		}
		
		public List<CybershakeIM> getCalcCyberShakeIMs(List<CybershakeIM> ims) {
			if (!multiPeriod)
				ims = ims.subList(0, 1);
			else
				Preconditions.checkState(ims.size() > 1);
			return ims;
		}
		
		@Override
		public String toString() {
			String ret;
			if (multiLevel)
				ret = "Multiple Hazard Levels";
			else
				ret = "Single Hazard Level";
			ret += ", ";
			if (multiPeriod)
				ret += "Multiple Periods";
			else
				ret += "Single Period";
			return ret;
		}
		
		public List<Double> getCalcLevels(double[] levels) {
			List<Double> list = Doubles.asList(levels);
			if (!multiLevel)
				list = list.subList(0, 1);
			else
				Preconditions.checkState(list.size() > 1);
			return list;
		}
		
		public static Config get(boolean multiLevel, boolean multiPeriod) {
			for (Config config : values())
				if (config.multiLevel == multiLevel && config.multiPeriod == multiPeriod)
					return config;
			return null;
		}
	}
	
	public static void main(String[] args) {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		
		boolean doCS = true;
		boolean doGMPE = true;
		boolean[] gmpeBranchAverages = { true, false };
		boolean[] gmpeExcludeBackgrounds = { true, false };
		
		File outputDir = new File("/home/kevin/CyberShake/MCER/disagg_benchmark");
		System.out.println("Cache dir: "+MCERDataProductsCalc.cacheDir);
		
		double[] periods = { 3d, 5d, 7.5, 10d };
		String[] siteNames = { "USC", "STNI", "SMCA", "PAS", "LAPD", "WNGC", "P22", "COO" };
		double[] imLevels = { 0.001, 0.002, 0.005, 0.01, 0.05 };
		
		Config[] configs = Config.values();
		
		File tempDir = Files.createTempDir();
		
		try {
			if (doCS) {
				Map<Config, List<Long>> configTimesMap = new HashMap<>();
				for (Config config : configs)
					configTimesMap.put(config, new ArrayList<>());
				
				Stopwatch csInitializationWatch = Stopwatch.createStarted();
				AbstractERF erf = study.getERF();
				CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), MCERDataProductsCalc.cacheDir, erf);
				List<CybershakeIM> ims = new ArrayList<>();
				for (double period : periods)
					ims.add(CybershakeIM.getSA(CyberShakeComponent.RotD100, period));
				csInitializationWatch.stop();
				long csInitializationMillis = csInitializationWatch.elapsed(TimeUnit.MILLISECONDS);
				System.out.println("Global CS initialization took: "+timeStr(csInitializationMillis));
				
				for (String siteName : siteNames) {
					System.out.println("Doing CyberShake, "+siteName);
					CybershakeRun run = study.runFetcher().forSite(siteName).fetch().get(0);
					System.out.println("Run: "+run);
					for (CybershakeIM im : ims)
						// make sure they're already cached on disk, will be cleared from memory
						amps2db.getAllIM_Values(run.getRunID(), im);
					
					for (Config config : configs) {
						amps2db.clearCache();
						Stopwatch watch = Stopwatch.createStarted();
						System.out.println("Doing "+config);
						DisaggregationPlotter disagg = new DisaggregationPlotter(study.getDB(), amps2db, run.getRunID(), erf,
								config.getCalcCyberShakeIMs(ims), null, null, config.getCalcLevels(imLevels), tempDir,
								Lists.newArrayList(PlotType.PDF, PlotType.PNG, PlotType.TXT));
						disagg.disaggregate();
						watch.stop();
						long millis = watch.elapsed(TimeUnit.MILLISECONDS);
						configTimesMap.get(config).add(millis);
						System.out.println(config+" took: "+timeStr(millis));
					}
				}
				
				System.out.println();
				System.out.println("=== CS Summary ===");
				for (Config config : configs) {
					List<Long> times = configTimesMap.get(config);
					long meanTime = 0;
					for (long time : times)
						meanTime += time;
					meanTime /= times.size();
					System.out.println(config+": "+timeStr(meanTime));
				}
				writeCSV(configTimesMap, new File(outputDir, "cs_disagg_bench.csv"), periods, imLevels, csInitializationMillis);
			}
			
			if (doGMPE) {
				for (boolean gmpeBranchAverage : gmpeBranchAverages) {
					for (boolean gmpeExcludeBackground : gmpeExcludeBackgrounds) {
						Map<Config, List<Long>> configTimesMap = new HashMap<>();
						for (Config config : configs)
							configTimesMap.put(config, new ArrayList<>());
						
						List<Site> sites = new ArrayList<>();
						SiteInfo2DB sites2db = new SiteInfo2DB(study.getDB());
						for (String siteName : siteNames)
							sites.add(new Site(sites2db.getLocationForSite(siteName), siteName));
						
						Stopwatch gmpeInitializationWatch = Stopwatch.createStarted();
						AttenuationRelationship gmpe = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
						gmpe.setParamDefaults();
						gmpe.setIntensityMeasure(SA_Param.NAME);
						HazardCurveCalculator curveCalc = new HazardCurveCalculator();
						MeanUCERF3 erf = new MeanUCERF3();
						if (gmpeBranchAverage)
							erf.setPreset(Presets.BOTH_FM_BRANCH_AVG);
						else
							erf.setPreset(Presets.COMPLETE_MODEL);
						if (gmpeExcludeBackground)
							erf.setParameter(IncludeBackgroundParam.NAME, IncludeBackgroundOption.EXCLUDE);
						erf.updateForecast();
						for (Site site : sites)
							site.addParameterList(gmpe.getSiteParams());
						gmpeInitializationWatch.stop();
						DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
						DiscretizedFunc logXVals = new ArbitrarilyDiscretizedFunc();
						for (Point2D pt : xVals)
							logXVals.set(Math.log(pt.getX()), pt.getY());
						long gmpeInitializationMillis = gmpeInitializationWatch.elapsed(TimeUnit.MILLISECONDS);
						
						double minMag = 6d;
						double deltaMag = 0.2;
						int numMags = (int)((8.6d - minMag)/deltaMag + 0.5);
						int numSourcesForDisag = 100;
						boolean showSourceDistances = true;
						double maxZAxis = Double.NaN;
						DisaggregationCalculator disagg = new DisaggregationCalculator();
						disagg.setMagRange(minMag, numMags, deltaMag);
						disagg.setNumSourcestoShow(numSourcesForDisag);
						disagg.setShowDistances(showSourceDistances);
						
						System.out.println("Global CS initialization took: "+timeStr(gmpeInitializationMillis));
						
						for (Site site : sites) {
							System.out.println("Doing GMPE, "+site.getName());
							
							for (Config config : configs) {
								// clear distance caches
								int cleared = 0;
								for (ProbEqkSource source : erf) {
									if (source.getSourceSurface() instanceof CacheEnabledSurface) {
										((CacheEnabledSurface)source.getSourceSurface()).clearCache();
										cleared++;
									}
									for (ProbEqkRupture rup : source) {
										if (rup.getRuptureSurface() instanceof CacheEnabledSurface) {
											((CacheEnabledSurface)rup.getRuptureSurface()).clearCache();
											cleared++;
										}
									}
								}
								System.out.println("Cleared caches on "+cleared+" surfaces");
								System.gc();
								
								Stopwatch watch = Stopwatch.createStarted();
								System.out.println("Doing "+config);
								for (double period : config.getCalcPeriods(periods)) {
									SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
									for (double iml : config.getCalcLevels(imLevels)) {
										Preconditions.checkState(disagg.disaggregate(Math.log(iml), site, gmpe, erf, curveCalc.getAdjustableParams()));
										disagg.setMaxZAxisForPlot(maxZAxis);
										String address = disagg.getDisaggregationPlotUsingServlet("asfd");
										String outputFileName = tempDir.getAbsolutePath()+File.separator+"gmpe_temp.pdf";
										DisaggregationPlotViewerWindow.saveAsPDF(
												address+DisaggregationCalculator.DISAGGREGATION_PLOT_PDF_NAME,
												outputFileName, "safda", "asdf", "asfda", "asdf");
									}
								}
								watch.stop();
								long millis = watch.elapsed(TimeUnit.MILLISECONDS);
								configTimesMap.get(config).add(millis);
								System.out.println(config+" took: "+timeStr(millis));
							}
						}
						
						System.out.println();
						System.out.println("=== GMPE Summary ===");
						for (Config config : configs) {
							List<Long> times = configTimesMap.get(config);
							long meanTime = 0;
							for (long time : times)
								meanTime += time;
							meanTime /= times.size();
							System.out.println(config+": "+timeStr(meanTime));
						}
						String csvName;
						if (gmpeBranchAverage)
							csvName = "gmpe_disagg_bench_branch_avg.csv";
						else
							csvName = "gmpe_disagg_bench_full.csv";
						if (gmpeExcludeBackground)
							csvName = csvName.replace(".csv", "_excl_bkg.csv");
						writeCSV(configTimesMap, new File(outputDir, csvName), periods, imLevels, gmpeInitializationMillis);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			study.getDB().destroy();
			if (tempDir.exists())
				FileUtils.deleteRecursive(tempDir);
		}
	}
	
	private static double averageTimeSecs(List<Long> times) {
		double sum = 0d;
		for (long time : times)
			sum += time;
		sum /= 1000d;
		return sum/times.size();
	}
	
	private static void writeCSV(Map<Config, List<Long>> configTimesMap, File csvFile, double[] periods, double[] levels,
			long initializationMillis) throws IOException {
		CSVFile<String> csv = new CSVFile<>(false);
		
		csv.addLine("Configuration", "# Tasks", "Total Time (s)", "Average Time Each (s)", "Average Time Additional (s)");
		
		double timeOne = averageTimeSecs(configTimesMap.get(Config.ONE_LEVEL_ONE_PERIOD));
		
		for (Config config : Config.values()) {
			if (!configTimesMap.containsKey(config))
				continue;
			List<String> line = new ArrayList<>();
			line.add(config.toString());
			int tasks = 1;
			if (config.multiLevel)
				tasks *= levels.length;
			if (config.multiPeriod)
				tasks *= periods.length;
			line.add(tasks+"");
			double avgTotTime = averageTimeSecs(configTimesMap.get(config));
			line.add(timeDF.format(avgTotTime));
			double avgTimeEach = avgTotTime/tasks;
			line.add(timeDF.format(avgTimeEach));
			if (tasks == 1) {
				line.add("N/A");
			} else {
				double avgAdditional = (avgTotTime - timeOne)/(tasks-1);
				line.add(timeDF.format(avgAdditional));
			}
			csv.addLine(line);
		}
		csv.addLine("");
		csv.addLine("Global Initialization Time (s)");
		csv.addLine(timeDF.format(initializationMillis/1000d));
		csv.writeToFile(csvFile);
	}
	
	private static final DecimalFormat timeDF = new DecimalFormat("0.00");
	
	private static String timeStr(long millis) {
		if (millis < 1000)
			return millis+" ms";
		double secs = millis/1000d;
		return timeStr(secs);
	}
	
	private static String timeStr(double secs) {
		if (secs < 60)
			return timeDF.format(secs)+" s";
		double mins = secs / 60d;
		return timeDF.format(mins)+" m";
	}

}
