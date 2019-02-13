package org.opensha.sha.cybershake.calc.mcer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.exceptions.ParameterException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveWriter;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.calc.mcer.ASCEDetLowerLimitCalc;
import org.opensha.sha.calc.mcer.CachedCurveBasedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.calc.mcer.MCErMapGenerator;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.mcer.UGMS_WebToolCalc.SpectraType;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import scratch.UCERF3.analysis.FaultBasedMapGen;
import scratch.kevin.util.ReturnPeriodUtils;

public class CyberShakeScatterWriter {
	
	public static final double bse2e_level = ReturnPeriodUtils.calcExceedanceProb(0.05, 50d, 1d);
	public static final double bse1e_level = ReturnPeriodUtils.calcExceedanceProb(0.2, 50d, 1d);
	public static final double sle_level = 1d-Math.exp(-1d/43d);
	
	public static void main(String[] args) throws IOException, GMT_MapException {
		File outputDir = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/scatter");
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		boolean calcIfNeeded = false;
		CyberShakeComponent component = CyberShakeComponent.RotD100;
		double[] periods = { 2,3,4,5,7.5,10 };
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		boolean cache = true;
		boolean replot = false;
		boolean reinterpolate = false;
		double spacing = 0.002;
		
		DBAccess db;
		if (calcIfNeeded)
			db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(true, true, study.getDBHost());
		else
			db = study.getDB();
		
		AbstractERF erf = study.getERF();
		List<CybershakeRun> runs = study.runFetcher().fetch();
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, MCERDataProductsCalc.cacheDir, erf);
		
		File detCacheFile = null, probCacheFile = null, rd50ProbCacheFile = null;
		if (cache) {
			// now cache
			File cacheDir = new File(outputDir, ".cs_cache");
			Preconditions.checkState(cacheDir.exists() || cacheDir.mkdir());
			
			String cachePrefix = study.name()+"_"+component.name();
			detCacheFile = new File(cacheDir, cachePrefix+"_deterministic.xml");
			probCacheFile = new File(cacheDir, cachePrefix+"_probabilistic_curve.xml");
			rd50ProbCacheFile = new File(cacheDir, study.name()+"_"+CyberShakeComponent.RotD50.name()+"_probabilistic_curve.xml");
		}
		
		CachedMCErDeterministicCalc csDetCalc = new CachedMCErDeterministicCalc(
				new CyberShakeMCErDeterministicCalc(amps2db, erf, component), detCacheFile);
		CachedCurveBasedMCErProbabilisticCalc csProbCalc = new CachedCurveBasedMCErProbabilisticCalc(
				new CyberShakeMCErProbabilisticCalc(db, component), probCacheFile);
		
		CyberShakeMCErProbabilisticCalc rawRd50Calc = new CyberShakeMCErProbabilisticCalc(db, CyberShakeComponent.RotD50);
		if (calcIfNeeded) {
			HazardCurveComputation calc = new HazardCurveComputation(db);
			calc.setPeakAmpsAccessor(new CachedPeakAmplitudesFromDB(db, new File("/data/kevin/cybershake/amps_cache/"), study.getERF()));
			rawRd50Calc.setCalculateCurves(true, calc);
		}
		CachedCurveBasedMCErProbabilisticCalc csRotd50ProbCalc = new CachedCurveBasedMCErProbabilisticCalc(
				rawRd50Calc, rd50ProbCacheFile);
		
		List<String> lines = new ArrayList<>();
		lines.add("# "+study.getName()+" MCER Maps");
		lines.add("");
		lines.add("**Study Details**");
		lines.add("");
		lines.addAll(study.getMarkdownMetadataTable());
		lines.add("");
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		File mcerSpectraFile = new File(outputDir, SpectraType.MCER.getFileName(spacing));
		Map<Location, DiscretizedFunc> mcerSpectrumMap = null;
		if (reinterpolate || !mcerSpectraFile.exists())
			mcerSpectrumMap = new HashMap<>();
		
		File mcerProbSpectraFile = new File(outputDir, SpectraType.MCER_PROB.getFileName(spacing));
		Map<Location, DiscretizedFunc> mcerProbSpectrumMap = null;
		if (reinterpolate || !mcerProbSpectraFile.exists())
			mcerProbSpectrumMap = new HashMap<>();
		
		File mcerDetSpectraFile = new File(outputDir, SpectraType.MCER_DET.getFileName(spacing));
		Map<Location, DiscretizedFunc> mcerDetSpectrumMap = null;
		if (reinterpolate || !mcerDetSpectraFile.exists())
			mcerDetSpectrumMap = new HashMap<>();
		
		File mcerDetLowerSpectraFile = new File(outputDir, SpectraType.MCER_DET_LOWER.getFileName(spacing));
		Map<Location, DiscretizedFunc> mcerDetLowerSpectrumMap = null;
		if (reinterpolate || !mcerDetLowerSpectraFile.exists())
			mcerDetLowerSpectrumMap = new HashMap<>();
		
		File bse2eSpectraFile = new File(outputDir, SpectraType.BSE_2E.getFileName(spacing));
		Map<Location, DiscretizedFunc> bse2eSpectrumMap = null;
		if (reinterpolate || !bse2eSpectraFile.exists())
			bse2eSpectrumMap = new HashMap<>();
		
		File bse1eSpectraFile = new File(outputDir, SpectraType.BSE_1E.getFileName(spacing));
		Map<Location, DiscretizedFunc> bse1eSpectrumMap = null;
		if (reinterpolate || !bse1eSpectraFile.exists())
			bse1eSpectrumMap = new HashMap<>();
		
		File sleSpectraFile = new File(outputDir, SpectraType.SLE.getFileName(spacing));
		Map<Location, DiscretizedFunc> sleSpectrumMap = null;
		if (reinterpolate || !sleSpectraFile.exists())
			sleSpectrumMap = new HashMap<>();
		
		for (double period : periods) {
			CybershakeIM im = CyberShakeMCErProbabilisticCalc.getIMsForPeriods(db, component, Lists.newArrayList(period)).get(0);
			System.out.println("Period "+(float)period+": "+im);
			HazardCurveFetcher fetcher = new HazardCurveFetcher(db, runs, im.getID());
			
			List<Site> sites = CyberShakeMCErMapGenerator.getSitesList(fetcher);
			System.out.println("Will write scatters for "+sites.size()+" sites");
			
			GeoDataSet mcerScatter = new ArbDiscrGeoDataSet(true);
			GeoDataSet probScatter = new ArbDiscrGeoDataSet(true);
			GeoDataSet detScatter = new ArbDiscrGeoDataSet(true);
			GeoDataSet detLowerScatter = new ArbDiscrGeoDataSet(true);
			GeoDataSet bse2eScatter = new ArbDiscrGeoDataSet(true);
			GeoDataSet bse1eScatter = new ArbDiscrGeoDataSet(true);
			GeoDataSet sleScatter = new ArbDiscrGeoDataSet(true);
			for (Site site : sites) {
				DiscretizedFunc probCurve = csProbCalc.calcHazardCurves(site, Lists.newArrayList(period)).get(period);
				double probVal = csProbCalc.calc(site, period);
				double detVal = csDetCalc.calc(site, period).getVal();
				double vs30;
				try {
					vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
				} catch (ParameterException e) {
					throw new IllegalStateException(e);
				}
				double detLowerVal;
				try {
					detLowerVal =  ASCEDetLowerLimitCalc.calc(period, vs30, site.getLocation());
				} catch (IllegalStateException e) {
					throw new IllegalStateException(e);
				}
				double mcer = MCErCalcUtils.calcMCER(detVal, probVal, detLowerVal);
				double bse2e = HazardDataSetLoader.getCurveVal(probCurve, false, bse2e_level);
				double bse1e = HazardDataSetLoader.getCurveVal(probCurve, false, bse1e_level);
				
				DiscretizedFunc rd50Curve = csRotd50ProbCalc.calcHazardCurves(site, Lists.newArrayList(period)).get(period);
				double sle = HazardDataSetLoader.getCurveVal(rd50Curve, false, sle_level);
				
				checkSet(mcerScatter, site, mcer, "MCER");
				checkSet(probScatter, site, probVal, "prob");
				checkSet(detScatter, site, detVal, "det");
				checkSet(detLowerScatter, site, detLowerVal, "det-lower");
				checkSet(bse2eScatter, site, bse2e, "BSE-2E");
				checkSet(bse1eScatter, site, bse1e, "BSE-1E");
				checkSet(sleScatter, site, sle, "SLE");
			}
			
			System.out.println("Done, writing...");
			String periodStr = optionalDigitDF.format(period);
			File mcerPlot = writePlot(mcerScatter, region, period, outputDir, "mcer_"+periodStr+"s", periodStr+"s MCER", replot);
			File probPlot = writePlot(probScatter, region, period, outputDir, "prob_"+periodStr+"s", periodStr+"s Probabilistic", replot);
			File detPlot = writePlot(detScatter, region, period, outputDir, "det_"+periodStr+"s", periodStr+"s Deterministic", replot);
			File detLowerPlot = writePlot(detLowerScatter, region, period, outputDir, "det_lower_"+periodStr+"s",
					periodStr+"s Det. Lower Limit", replot);
			File bse2ePlot = writePlot(bse2eScatter, region, period, outputDir, "bse_2e_"+periodStr+"s", periodStr+"s BSE-2E", replot);
			File bse1ePlot = writePlot(bse1eScatter, region, period, outputDir, "bse_1e_"+periodStr+"s", periodStr+"s BSE-1E", replot);
			File slePlot = writePlot(sleScatter, region, period, outputDir, "sle_"+periodStr+"s", periodStr+"s SLE", replot);
			
			lines.add("## "+periodStr+"s Maps");
			lines.add(topLink); lines.add("");
			
			TableBuilder table = MarkdownUtils.tableBuilder();
			table.addLine("**MCER Map**");
			table.addLine("![MCER]("+mcerPlot.getName()+")");
			lines.addAll(table.build());
			
			lines.add("");
			lines.add("### "+periodStr+"s MCER Ingredients");
			lines.add(topLink); lines.add("");
			
			table = MarkdownUtils.tableBuilder();
			table.addLine("**Probabilistic**", "**Deterministic**", "**Deterministic Lower Limit**");
			table.addLine("![Prob]("+probPlot.getName()+")", "![Det]("+detPlot.getName()+")", "![Det Lower Limit]("+detLowerPlot.getName()+")");
			lines.addAll(table.build());
			
			lines.add("");
			lines.add("### "+periodStr+"s BSE-2E, BSE-1E & SLE");
			lines.add(topLink); lines.add("");
			
			table = MarkdownUtils.tableBuilder();
			table.addLine("**BSE-2E**", "**BSE-1E**", "**SLE**");
			table.addLine("![BSE-2E]("+bse2ePlot.getName()+")", "![BSE-1E]("+bse1ePlot.getName()+")", "![SLE]("+slePlot.getName()+")");
			lines.addAll(table.build());
			
			lines.add("");
			
			System.out.println("Interpolating with GMT");
			if (mcerSpectrumMap != null) {
				GeoDataSet mcerInterpolatedData = interpolate(mcerScatter, region, spacing);
				loadSpecta(mcerSpectrumMap, mcerInterpolatedData, period);
			}
			if (mcerProbSpectrumMap != null) {
				GeoDataSet mcerProbInterpolatedData = interpolate(probScatter, region, spacing);
				loadSpecta(mcerProbSpectrumMap, mcerProbInterpolatedData, period);
			}
			if (mcerDetSpectrumMap != null) {
				GeoDataSet mcerDetInterpolatedData = interpolate(detScatter, region, spacing);
				loadSpecta(mcerDetSpectrumMap, mcerDetInterpolatedData, period);
			}
			if (mcerDetLowerSpectrumMap != null) {
				GeoDataSet mcerDetLowerInterpolatedData = interpolate(detLowerScatter, region, spacing);
				loadSpecta(mcerDetLowerSpectrumMap, mcerDetLowerInterpolatedData, period);
			}
			if (bse2eSpectrumMap != null) {
				GeoDataSet bse2eInterpolatedData = interpolate(bse2eScatter, region, spacing);
				loadSpecta(bse2eSpectrumMap, bse2eInterpolatedData, period);
			}
			if (bse1eSpectrumMap != null) {
				GeoDataSet bse1eInterpolatedData = interpolate(bse1eScatter, region, spacing);
				loadSpecta(bse1eSpectrumMap, bse1eInterpolatedData, period);
			}
			if (sleSpectrumMap != null) {
				GeoDataSet sleInterpolatedData = interpolate(sleScatter, region, spacing);
				loadSpecta(sleSpectrumMap, sleInterpolatedData, period);
			}
			
			if (cache) {
				try {
					((CachedMCErDeterministicCalc)csDetCalc).flushCache();
					((CachedCurveBasedMCErProbabilisticCalc)csProbCalc).flushCache();
					((CachedCurveBasedMCErProbabilisticCalc)csRotd50ProbCalc).flushCache();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		System.out.println("Writing interpolated spectra binary files");
		if (mcerSpectrumMap != null)
			new BinaryHazardCurveWriter(mcerSpectraFile).writeCurves(mcerSpectrumMap);
		if (mcerProbSpectrumMap != null)
			new BinaryHazardCurveWriter(mcerProbSpectraFile).writeCurves(mcerProbSpectrumMap);
		if (mcerDetSpectrumMap != null)
			new BinaryHazardCurveWriter(mcerDetSpectraFile).writeCurves(mcerDetSpectrumMap);
		if (mcerDetLowerSpectrumMap != null)
			new BinaryHazardCurveWriter(mcerDetLowerSpectraFile).writeCurves(mcerDetLowerSpectrumMap);
		if (bse2eSpectrumMap != null)
			new BinaryHazardCurveWriter(bse2eSpectraFile).writeCurves(bse2eSpectrumMap);
		if (bse1eSpectrumMap != null)
			new BinaryHazardCurveWriter(bse1eSpectraFile).writeCurves(bse1eSpectrumMap);
		if (sleSpectrumMap != null)
			new BinaryHazardCurveWriter(sleSpectraFile).writeCurves(sleSpectrumMap);
		System.out.println("Done wiritng, files, destroying DB connection");
		
		db.destroy();
		if (calcIfNeeded)
			study.getDB().destroy();
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
		lines.add(tocIndex, "## Table Of Contents");

		// write markdown
		System.out.print("Writing markdown...");
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
		System.out.println("DONE");
	}
	
	private static void checkSet(GeoDataSet scatter, Site site, double val, String name) {
		Preconditions.checkState(Double.isFinite(val), "non-finite val for %s at site %s: %s", name, site.getName(), val);
		Preconditions.checkState(val >= 0, "negative val for %s at site %s: %s", name, site.getName(), val);
		scatter.set(site.getLocation(), val);
	}
	private static final boolean log_plot = true;
	
	private static File writePlot(GeoDataSet scatter, Region region, double period, File outputDir, String prefix, String title, boolean replot)
			throws IOException, GMT_MapException {
		ArbDiscrGeoDataSet.writeXYZFile(scatter, new File(outputDir, prefix+".xyz"));
		File pngFile = new File(outputDir, prefix+".png");
		if (!replot && pngFile.exists()) {
			System.out.println(pngFile.getName()+" exists, skipping");
			return pngFile;
		}
		CPT cpt = CyberShake_GMT_MapGenerator.getHazardCPT();
		if (log_plot) {
			if (period <= 3d)
				cpt = cpt.rescale(-1d, 1d);
			else if (period <= 5d)
				cpt = cpt.rescale(-1.5, 0.5d);
			else
				cpt = cpt.rescale(-2d, 0d);
		}
		double contour = 0.1;
		if (prefix.contains("sle")) {
			if (log_plot) {
				cpt = cpt.rescale(-2.5d, -0.5d);
			} else {
				cpt = cpt.rescale(0d, 0.5d);
				contour = 0.05;
			}
		}
		GMT_Map map = MCErMapGenerator.buildScatterMap(region, scatter, false, period, title, cpt, log_plot);
		map.setSymbolSet(null);
		map.setContourIncrement(contour);
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
		Preconditions.checkState(pngFile.exists());
		return pngFile;
	}
	
	private static GeoDataSet interpolate(GeoDataSet scatter, Region region, double spacing) throws IOException {
		File tempDir = Files.createTempDir();
		
		double minLat = region.getMinLat();
		double maxTempLat = region.getMaxLat();
		double minLon = region.getMinLon();
		double maxTempLon = region.getMaxLon();

		// adjust the max lat and lon to be an exact increment (needed for xyz2grd)
		double maxLat = Math.rint(((maxTempLat-minLat)/spacing))*spacing +minLat;
		double maxLon = Math.rint(((maxTempLon-minLon)/spacing))*spacing +minLon;

		String regionStr = " -R"+(float)minLon+"/"+(float)maxLon+"/"+(float)minLat+"/"+(float)maxLat;
		
		File inputScatterFile = new File(tempDir, "scatter.xyz");
		ArbDiscrGeoDataSet.writeXYZFile(scatter, inputScatterFile);
		
		File grdFile = new File(tempDir, "interpolated.grd");
		
		runGMT("gmt surface "+inputScatterFile.getAbsolutePath()+" -G"+grdFile.getAbsolutePath()+" -I"+(float)spacing+"= "
				+regionStr+" -S\"1.0 \" -T0.0i0.1b -: -h0");
		File interpTextFile = new File(tempDir, "interpolated.txt");
		runGMT("gmt grd2xyz "+grdFile.getAbsolutePath()+" > "+interpTextFile.getAbsolutePath());
		ArbDiscrGeoDataSet data = ArbDiscrGeoDataSet.loadXYZFile(interpTextFile.getAbsolutePath(), false);
		
		FileUtils.deleteRecursive(tempDir);
		
		return data;
	}
	
	private static void runGMT(String command) throws IOException {
		String[] commandArray = { "/bin/bash", "-c", command };
		
		Process p = Runtime.getRuntime().exec(commandArray);
		int exit;
		try {
			exit = p.waitFor();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		System.out.println("command: "+command);
		System.out.println("exit: "+exit);
		if (exit != 0) {
			System.out.println("=== STDERR ===");
			printStream(p.getErrorStream());
			System.out.println("==============");
			System.out.println("=== STDOUT ===");
			printStream(p.getInputStream());
			System.out.println("==============");
		}
		Preconditions.checkState(exit == 0);
	}
	
	private static void printStream(InputStream stream) throws IOException {
		BufferedReader b = new BufferedReader(new InputStreamReader(stream));
		String line;
		while ((line = b.readLine()) != null)
			System.out.println(line);
	}
	
	private static void loadSpecta(Map<Location, DiscretizedFunc> spectrumMap, GeoDataSet data, double period) {
		if (spectrumMap.isEmpty()) {
			for (Location loc : data.getLocationList())
				spectrumMap.put(loc, new ArbitrarilyDiscretizedFunc());
		} else {
			Preconditions.checkState(spectrumMap.size() == data.size(),
					"data size mismatch, expected %s, have %s", spectrumMap.size(), data.size());
		}
		for (Location loc : data.getLocationList()) {
			double val = data.get(loc);
			spectrumMap.get(loc).set(period, val);
		}
	}
	
	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.##");

}
