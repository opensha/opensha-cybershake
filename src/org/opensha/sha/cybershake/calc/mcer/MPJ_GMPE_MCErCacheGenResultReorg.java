package org.opensha.sha.cybershake.calc.mcer;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.FileNameComparator;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.binFile.BinaryGeoDatasetRandomAccessFile;
import org.opensha.commons.util.binFile.BinaryXYZRandomAccessFile;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveWriter;
import org.opensha.sha.calc.hazardMap.BinaryRandomAccessHazardCurveWriter;
import org.opensha.sha.calc.hazardMap.components.BinaryCurveArchiver;
import org.opensha.sha.calc.mcer.MCErMapGenerator;
import org.opensha.sha.cybershake.calc.mcer.UGMS_WebToolCalc.DesignParameter;
import org.opensha.sha.cybershake.calc.mcer.UGMS_WebToolCalc.SpectraSource;
import org.opensha.sha.cybershake.calc.mcer.UGMS_WebToolCalc.SpectraType;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.util.component.ComponentConverter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

import scratch.UCERF3.analysis.FaultBasedMapGen;
import scratch.kevin.util.ReturnPeriodUtils;

public class MPJ_GMPE_MCErCacheGenResultReorg {
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		File mainDir = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen");
//		String prefix = "2016_09_30-ucerf3_downsampled_ngaw2_binary_0.02_";
//		String prefix = "2017_01_20-ucerf3_downsampled_ngaw2_binary_0.02_";
//		String prefix = "2017_05_19-ucerf3_downsampled_ngaw2_binary_0.02_";
//		String prefix = "2017_07_27-ucerf3_downsampled_ngaw2_binary_0.02_";
		String prefix = "2018_10_03-ucerf3_downsampled_ngaw2_binary_curves_0.01_";
		String dataFileName = "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_mcer.bin";
		String pgaPrefix = "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD50_pga";
		Component saComponent = Component.RotD100;
		String saFilePrefix = "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_sa_";
		File outputDir = new File(mainDir, prefix+"results");
		double spacing = 0.01;
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
//		File mcerFile = new File(new File(mainDir, prefix+"classCCD"), "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_mcer.bin");
//		File probFile = new File(new File(mainDir, prefix+"classCCD"), "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_prob.bin");
//		File detFile = new File(new File(mainDir, prefix+"classCCD"), "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_det.bin");
//		File tmp = new File("/tmp");
//		plotSpectrum(0.1, new BinaryHazardCurveReader(mcerFile.getAbsolutePath()).getCurveMap(), tmp, "CCD_mcer", spacing, region);
//		plotSpectrum(0.1, new BinaryHazardCurveReader(probFile.getAbsolutePath()).getCurveMap(), tmp, "CCD_prob", spacing, region);
//		plotSpectrum(0.1, new BinaryHazardCurveReader(detFile.getAbsolutePath()).getCurveMap(), tmp, "CCD_det", spacing, region);
//		System.exit(0);
		
		double[] plotPeriods = {0.1d, 1d};
//		double[] plotPeriods = null;
		boolean replot = false;
		
		List<String> lines = new ArrayList<>();
		lines.add("# GMPE MCER Maps");
		lines.add("");
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		File[] subDirs = mainDir.listFiles();
		Arrays.sort(subDirs, new FileNameComparator());
		for (File dir : subDirs) {
			if (!dir.isDirectory())
				continue;
			if (dir.equals(outputDir))
				continue;
			String dirName = dir.getName();
			if (!dirName.startsWith(prefix))
				continue;
			File dataFile = new File(dir, dataFileName);
			Preconditions.checkState(dataFile.exists(), "Data file doesn't exist: %s", dataFile.getAbsolutePath());
			
			System.out.println("Loading "+dirName);
			BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dataFile.getAbsolutePath());
			Map<Location, ArbitrarilyDiscretizedFunc> mcerMap = reader.getCurveMap();
			System.out.println("Loaded "+mcerMap.size());
			for (Location loc : mcerMap.keySet()) {
				ArbitrarilyDiscretizedFunc mcer = mcerMap.get(loc);
				Preconditions.checkNotNull(mcer);
				for (Point2D pt : mcer)
					Preconditions.checkState(Doubles.isFinite(pt.getY()));
			}
			System.out.println("All values validated\n");
			
			String identifier = dirName.substring(prefix.length());
			Preconditions.checkState(!identifier.isEmpty());
			File mcerOutFile = new File(outputDir, identifier+"_"+SpectraType.MCER.getFileName(spacing));
			Files.copy(dataFile, mcerOutFile);
			String mcerPlotPrefix = mcerOutFile.getName().replace(".bin", "");
			
			if (plotPeriods != null)
				for (double period : plotPeriods)
					plotSpectra(period, mcerMap, outputDir, identifier+", MCER",
							mcerPlotPrefix, spacing, region, replot);
			
			// now PGA M
			File pgaFile = new File(dir, pgaPrefix+"_g.bin");
			String pgaPlotPrefix = null;
			if (pgaFile.exists()) {
				System.out.println("Doing PGA M");
				ArbDiscrGeoDataSet pgaData = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(pgaFile);
				for (int i=0; i<pgaData.size(); i++)
					Preconditions.checkState(Doubles.isFinite(pgaData.get(i)),
							"Non Finite PGA M at index %s/%s: %s", i, pgaData.size(), pgaData.get(i));
				System.out.println("All PGA M values validated\n");
				File pgaOutFile = new File(outputDir,
						identifier+"_"+DesignParameter.PGAM.getFileName(spacing, SpectraType.MCER, SpectraSource.COMBINED));
				Files.copy(pgaFile, pgaOutFile);
				pgaPlotPrefix = pgaOutFile.getName().replace(".bin", "");
				
				if (plotPeriods != null)
					plotPGA(pgaData, outputDir, identifier+", PGAM", pgaPlotPrefix, spacing, region, replot);
			}
			
			// now BSE-2E, BSE-1E, and SLE
			DiscretizedFunc mcerXVals = mcerMap.values().iterator().next();
			Map<Location, ArbitrarilyDiscretizedFunc> bse2Espectra = new HashMap<>();
			Map<Location, ArbitrarilyDiscretizedFunc> bse1Espectra = new HashMap<>();
			Map<Location, ArbitrarilyDiscretizedFunc> sleSpectra = new HashMap<>();
			for (int p=0; p<mcerXVals.size(); p++) {
				double period = mcerXVals.getX(p);
				File saFile = new File(dir, saFilePrefix+(float)period+"s.bin");
				Preconditions.checkState(saFile.exists(), "SA file not found for p=%s: %s", (float)period, saFile);
				reader = new BinaryHazardCurveReader(saFile.getAbsolutePath());
				Map<Location, ArbitrarilyDiscretizedFunc> curveMap = reader.getCurveMap();
				Preconditions.checkState(mcerMap.size() == curveMap.size(),
						"curve size mismatch. have %s MCER, %s SA", mcerMap.size(), curveMap.size());
				
				for (Location loc : curveMap.keySet()) {
					DiscretizedFunc hazardCurve = curveMap.get(loc);
					DiscretizedFunc mcerSpectrum = mcerMap.get(loc);
					Preconditions.checkNotNull(mcerSpectrum, "No MCER spectra found for location: %s", loc);
//					double mcer = mcerSpectrum.getY(period);
					// TODO use ReturnPeriodUtils instead?
					double imlAt5_50 = calculateUniformHazardVal(hazardCurve, CyberShakeScatterWriter.bse2e_level);
					double imlAt20_50 = calculateUniformHazardVal(hazardCurve, CyberShakeScatterWriter.bse1e_level);
					double bse2e = imlAt5_50;
					double bse1e = imlAt20_50;
					
					ArbitrarilyDiscretizedFunc bse2Espectrum = bse2Espectra.get(loc);
					ArbitrarilyDiscretizedFunc bse1Espectrum = bse1Espectra.get(loc);
					ArbitrarilyDiscretizedFunc sleSpectrum = sleSpectra.get(loc);
					if (bse2Espectrum == null) {
						bse2Espectrum = new ArbitrarilyDiscretizedFunc();
						bse1Espectrum = new ArbitrarilyDiscretizedFunc();
						sleSpectrum = new ArbitrarilyDiscretizedFunc();
						bse2Espectra.put(loc, bse2Espectrum);
						bse1Espectra.put(loc, bse1Espectrum);
						sleSpectra.put(loc, sleSpectrum);
					}
					bse2Espectrum.set(period, bse2e);
					bse1Espectrum.set(period, bse1e);
					
					// now SLE, AFE = 1/43yr (approx 50% exceedance probability in 30 years), RotD50
					DiscretizedFunc rd50curve;
					if (saComponent == Component.RotD50)
						rd50curve = hazardCurve;
					else
						rd50curve = ComponentConverter.convert(saComponent, Component.RotD50, hazardCurve, period);
					double sleIML = calculateUniformHazardVal(rd50curve, CyberShakeScatterWriter.sle_level);
					sleSpectrum.set(period, sleIML);
				}
			}
			File bse2eOutFile = new File(outputDir, identifier+"_"+SpectraType.BSE_2E.getFileName(spacing));
			String bse2ePlotPrefix = bse2eOutFile.getName().replace(".bin", "");
			BinaryHazardCurveWriter bse2Ewriter = new BinaryHazardCurveWriter(bse2eOutFile);
			bse2Ewriter.writeCurves(bse2Espectra);
			File bse1eOutFile = new File(outputDir, identifier+"_"+SpectraType.BSE_1E.getFileName(spacing));
			String bse1ePlotPrefix = bse1eOutFile.getName().replace(".bin", "");
			BinaryHazardCurveWriter bse1Ewriter = new BinaryHazardCurveWriter(bse1eOutFile);
			bse1Ewriter.writeCurves(bse1Espectra);
			File sleOutFile = new File(outputDir, identifier+"_"+SpectraType.SLE.getFileName(spacing));
			String slePlotPrefix = sleOutFile.getName().replace(".bin", "");
			BinaryHazardCurveWriter sleWriter = new BinaryHazardCurveWriter(sleOutFile);
			sleWriter.writeCurves(sleSpectra);
			if (plotPeriods != null) {
				for (double period : plotPeriods) {
					plotSpectra(period, bse2Espectra, outputDir, identifier+", BSE-2E", bse2ePlotPrefix, spacing, region, replot);
					plotSpectra(period, bse1Espectra, outputDir, identifier+", BSE-1E", bse1ePlotPrefix, spacing, region, replot);
					plotSpectra(period, sleSpectra, outputDir, identifier+", SLE", slePlotPrefix, spacing, region, replot);
				}
			}
			
			// now BSE-1/2E and SLE PGA M
			File pgaCurvesFile = new File(dir, pgaPrefix+".bin");
			String bse2ePGAPlotPrefix = null;
			String bse1ePGAPlotPrefix = null;
			String slePGAPlotPrefix = null;
			if (pgaCurvesFile.exists()) {
				System.out.println("Doing BSE-1/2E and SLE PGA M");
				reader = new BinaryHazardCurveReader(pgaCurvesFile.getAbsolutePath());
				Map<Location, ArbitrarilyDiscretizedFunc> curveMap = reader.getCurveMap();
				Preconditions.checkState(mcerMap.size() == curveMap.size(),
						"curve size mismatch. have %s MCER, %s PGA", mcerMap.size(), curveMap.size());
				
				ArbDiscrGeoDataSet pgaData1 = new ArbDiscrGeoDataSet(false);
				ArbDiscrGeoDataSet pgaData2 = new ArbDiscrGeoDataSet(false);
				ArbDiscrGeoDataSet pgaDataSLE = new ArbDiscrGeoDataSet(false);
				for (Location loc : curveMap.keySet()) {
					DiscretizedFunc hazardCurve = curveMap.get(loc);
					double imlAt5_50 = calculateUniformHazardVal(hazardCurve, CyberShakeScatterWriter.bse2e_level);
					double imlAt20_50 = calculateUniformHazardVal(hazardCurve, CyberShakeScatterWriter.bse1e_level);
					pgaData2.set(loc, imlAt5_50);
					pgaData1.set(loc, imlAt20_50);
					double sleIML = calculateUniformHazardVal(hazardCurve, CyberShakeScatterWriter.sle_level);
					pgaDataSLE.set(loc, sleIML);
				}
				for (int i=0; i<pgaData1.size(); i++)
					Preconditions.checkState(Doubles.isFinite(pgaData1.get(i)),
							"Non Finite PGA M at index %s/%s: %s", i, pgaData1.size(), pgaData1.get(i));
				for (int i=0; i<pgaData2.size(); i++)
					Preconditions.checkState(Doubles.isFinite(pgaData2.get(i)),
							"Non Finite PGA M at index %s/%s: %s", i, pgaData2.size(), pgaData2.get(i));
				for (int i=0; i<pgaDataSLE.size(); i++)
					Preconditions.checkState(Doubles.isFinite(pgaDataSLE.get(i)),
							"Non Finite PGA M at index %s/%s: %s", i, pgaDataSLE.size(), pgaDataSLE.get(i));
				System.out.println("All PGA M values validated\n");
				
				File pgaOutFile2 = new File(outputDir,
						identifier+"_"+DesignParameter.PGAM.getFileName(spacing, SpectraType.BSE_2E, SpectraSource.COMBINED));
				BinaryGeoDatasetRandomAccessFile.writeGeoDataset(pgaData2, BinaryCurveArchiver.byteOrder, pgaOutFile2);
				bse2ePGAPlotPrefix = pgaOutFile2.getName().replace(".bin", "");
				
				File pgaOutFile1 = new File(outputDir,
						identifier+"_"+DesignParameter.PGAM.getFileName(spacing, SpectraType.BSE_1E, SpectraSource.COMBINED));
				BinaryGeoDatasetRandomAccessFile.writeGeoDataset(pgaData1, BinaryCurveArchiver.byteOrder, pgaOutFile1);
				bse1ePGAPlotPrefix = pgaOutFile1.getName().replace(".bin", "");
				
				File pgaOutFileSLE = new File(outputDir,
						identifier+"_"+DesignParameter.PGAM.getFileName(spacing, SpectraType.SLE, SpectraSource.COMBINED));
				BinaryGeoDatasetRandomAccessFile.writeGeoDataset(pgaDataSLE, BinaryCurveArchiver.byteOrder, pgaOutFileSLE);
				slePGAPlotPrefix = pgaOutFileSLE.getName().replace(".bin", "");
				
				if (plotPeriods != null) {
					plotPGA(pgaData2, outputDir, identifier+", BSE-2E PGAM", bse2ePGAPlotPrefix, spacing, region, replot);
					plotPGA(pgaData1, outputDir, identifier+", BSE-1E PGAM", bse1ePGAPlotPrefix, spacing, region, replot);
					plotPGA(pgaDataSLE, outputDir, identifier+", SLE PGAM", slePGAPlotPrefix, spacing, region, replot);
				}
			}
			
			if (plotPeriods != null)
				lines.addAll(getPlotLines(plotPeriods, topLink, identifier, mcerPlotPrefix, pgaPlotPrefix, bse2ePlotPrefix,
						bse1ePlotPrefix, bse2ePGAPlotPrefix, bse1ePGAPlotPrefix, slePlotPrefix, slePGAPlotPrefix));
		}
		
		// now calculate D_default
		/*
		 * From CB 5/26/17
		 * If the user selects “D (default, per Sect. 11.4.3, ASCE 7-16)”, then the MCER response spectrum is obtained 
		 * as follows:
				1.       Obtain the MCER response spectrum for Site Class C
				2.       Obtain the MCER response spectrum for Site Class D
				3.       Take the envelop of the two MCER response spectra from Steps 1 & 2
							i.e., at each natural period, select the larger of the two spectral accelerations from
							the first two steps. The result is the MCER response spectrum for this default case.
			
			and do the exact same for BSE-2E and BSE-1E
		 */
		System.out.println("Creating D_default");
		SpectraType[] typesForD_default = {SpectraType.MCER, SpectraType.BSE_2E, SpectraType.BSE_1E, SpectraType.SLE };
		String mcerPlotPrefix = null, bse2ePlotPrefix = null, bse1ePlotPrefix = null, slePlotPrefix = null;
		for (SpectraType type : typesForD_default) {
			File dIn = new File(outputDir, "classD_"+type.getFileName(spacing));
			File cIn = new File(outputDir, "classC_"+type.getFileName(spacing));
			if (!dIn.exists()) {
				System.out.println("Skipping "+type);
				continue;
			} else {
				System.out.println("Processing "+type);
				Preconditions.checkState(cIn.exists());
			}
			File dDefaultOut = new File(outputDir, "classD_default_"+type.getFileName(spacing));
			BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dIn.getAbsolutePath());
			// read first one this way to preserve site order
			List<Location> curveLocs = Lists.newArrayList();
			Map<Location, ArbitrarilyDiscretizedFunc> dMap = Maps.newHashMap();
			ArbitrarilyDiscretizedFunc curve = reader.nextCurve();
			while (curve != null) {
				Location loc = reader.currentLocation();
				dMap.put(loc, curve);
				curveLocs.add(loc);
				curve = reader.nextCurve();
			}
			reader = new BinaryHazardCurveReader(cIn.getAbsolutePath());
			Map<Location, ArbitrarilyDiscretizedFunc> cMap = reader.getCurveMap();
			Preconditions.checkState(dMap.size() == cMap.size());
			BinaryRandomAccessHazardCurveWriter dDefaultWrite = new BinaryRandomAccessHazardCurveWriter(
					dDefaultOut, ByteOrder.BIG_ENDIAN, dMap.size(), dMap.values().iterator().next());
			dDefaultWrite.initialize();
			Map<Location, ArbitrarilyDiscretizedFunc> dDeafultMap = Maps.newHashMap();
			for (int i=0; i<curveLocs.size(); i++) {
				Location loc = curveLocs.get(i);
				DiscretizedFunc dFunc = dMap.get(loc);
				DiscretizedFunc cFunc = cMap.get(loc);
				Preconditions.checkState(dFunc.size() == cFunc.size());
				ArbitrarilyDiscretizedFunc dDefaultFunc = new ArbitrarilyDiscretizedFunc();
				for (int j=0; j<dFunc.size(); j++) {
					double dDefaultVal = Math.max(dFunc.getY(j), cFunc.getY(j));
					dDefaultFunc.set(dFunc.getX(j), dDefaultVal);
				}
				dDeafultMap.put(loc, dDefaultFunc);
				dDefaultWrite.writeCurve(i, loc, dDefaultFunc);
			}
			dDefaultWrite.close();
			if (plotPeriods != null) {
				String plotPrefix = dDefaultOut.getName().replace(".bin", "");
				if (type == SpectraType.MCER)
					mcerPlotPrefix = plotPrefix;
				else if (type == SpectraType.BSE_2E)
					bse2ePlotPrefix = plotPrefix;
				else if (type == SpectraType.BSE_1E)
					bse1ePlotPrefix = plotPrefix;
				else if (type == SpectraType.SLE)
					slePlotPrefix = plotPrefix;
				for (double period : plotPeriods)
					plotSpectra(period, dDeafultMap, outputDir, "classD (default), "+type, plotPrefix, spacing, region, replot);
			}
		}
		
		// now the same but for PGA
		SpectraType[] pgaTypes = { SpectraType.MCER, SpectraType.BSE_2E, SpectraType.BSE_1E, SpectraType.SLE };
		String pgaPlotPrefix = null, bse2ePGAPlotPrefix = null, bse1ePGAPlotPrefix = null, slePGAPlotPrefix = null;
		for (SpectraType type : pgaTypes) {
			String fileName = DesignParameter.PGAM.getFileName(spacing, type, SpectraSource.COMBINED);
			File dInPGA = new File(outputDir, "classD_"+fileName);
			if (dInPGA.exists()) {
				System.out.println("Creating PGAM for "+type+" D_default");
				File cInPGA = new File(outputDir, "classC_"+fileName);
				ArbDiscrGeoDataSet dPGA = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(dInPGA);
				ArbDiscrGeoDataSet cPGA = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(cInPGA);
				Preconditions.checkState(dPGA.size() == cPGA.size());
				File dDefaultPGAOut = new File(outputDir, "classD_default_"+fileName);
				ArbDiscrGeoDataSet dDefaultPGAData = new ArbDiscrGeoDataSet(false);
				for (int i=0; i<dPGA.size(); i++) {
					Location loc = dPGA.getLocation(i);
					Preconditions.checkState(loc.equals(cPGA.getLocation(i)));
					double val = Math.max(dPGA.get(i), cPGA.get(i));
					dDefaultPGAData.set(loc, val);
				}
				BinaryGeoDatasetRandomAccessFile.writeGeoDataset(dDefaultPGAData, BinaryCurveArchiver.byteOrder, dDefaultPGAOut);
				if (plotPeriods != null) {
					String title = type.name()+" PGAM";
					String plotPrefix = dDefaultPGAOut.getName().replace(".bin", "");
					if (type == SpectraType.MCER)
						pgaPlotPrefix = plotPrefix;
					else if (type == SpectraType.BSE_2E)
						bse2ePGAPlotPrefix = plotPrefix;
					else if (type == SpectraType.BSE_1E)
						bse1ePGAPlotPrefix = plotPrefix;
					else if (type == SpectraType.SLE)
						slePGAPlotPrefix = plotPrefix;
					plotPGA(dDefaultPGAData, outputDir, title, plotPrefix, spacing, region, replot);
				}
			}
		}
		
		
		if (plotPeriods != null) {
			lines.addAll(getPlotLines(plotPeriods, topLink, "classD (Default)", mcerPlotPrefix, pgaPlotPrefix, bse2ePlotPrefix,
					bse1ePlotPrefix, bse2ePGAPlotPrefix, bse1ePGAPlotPrefix, slePlotPrefix, slePGAPlotPrefix));
			
			// add TOC
			lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
			lines.add(tocIndex, "## Table Of Contents");

			// write markdown
			MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
		}
	}

	private static List<String> getPlotLines(double[] plotPeriods, String topLink, String identifier,
			String mcerPlotPrefix, String pgaPlotPrefix, String bse2ePlotPrefix, String bse1ePlotPrefix,
			String bse2ePGAPlotPrefix, String bse1ePGAPlotPrefix, String slePlotPrefix, String slePGAPlotPrefix) {
		List<String> lines = new ArrayList<>();
		
		identifier = identifier.replaceAll("class", "Class ");
		
		lines.add("## "+identifier);
		lines.add(topLink); lines.add("");
		
		for (double period : plotPeriods) {
			lines.add("## "+identifier+", "+(float)period+"s");
			lines.add(topLink); lines.add("");
			
			TableBuilder table = MarkdownUtils.tableBuilder();
			
			table.initNewLine();
			table.addColumn("MCER");
			if (bse2ePlotPrefix != null) {
				table.addColumn("BSE-2E");
				table.addColumn("BSE-1E");
			}
			if (slePlotPrefix != null)
				table.addColumn("SLE");
			table.finalizeLine();
			
			table.initNewLine();
			table.addColumn("![MCER]("+mcerPlotPrefix+"_"+(float)period+".png)");
			if (bse2ePlotPrefix != null) {
				table.addColumn("![BSE-2E]("+bse2ePlotPrefix+"_"+(float)period+".png)");
				table.addColumn("![BSE-1E]("+bse1ePlotPrefix+"_"+(float)period+".png)");
			}
			if (slePlotPrefix != null)
				table.addColumn("![SLE]("+slePlotPrefix+"_"+(float)period+".png)");
			table.finalizeLine();
			
			lines.addAll(table.build());
			lines.add("");
		}
		
		if (pgaPlotPrefix != null || bse2ePGAPlotPrefix != null) {
			lines.add("## "+identifier+", PGAM");
			lines.add(topLink); lines.add("");
			
			TableBuilder table = MarkdownUtils.tableBuilder();
			
			table.initNewLine();
			if (pgaPlotPrefix != null)
				table.addColumn("PGAM");
			if (bse2ePGAPlotPrefix != null)
				table.addColumn("BSE-2E PGAM");
			if (bse1ePGAPlotPrefix != null)
				table.addColumn("BSE-2E PGAM");
			if (slePGAPlotPrefix != null)
				table.addColumn("SLE PGAM");
			table.finalizeLine();
			
			table.initNewLine();
			if (pgaPlotPrefix != null)
				table.addColumn("![PGAM]("+pgaPlotPrefix+".png)");
			if (bse2ePGAPlotPrefix != null)
				table.addColumn("![PGAM]("+bse2ePGAPlotPrefix+".png)");
			if (bse1ePGAPlotPrefix != null)
				table.addColumn("![PGAM]("+bse1ePGAPlotPrefix+".png)");
			if (slePGAPlotPrefix != null)
				table.addColumn("![PGAM]("+slePGAPlotPrefix+".png)");
			table.finalizeLine();
			
			lines.addAll(table.build());
			lines.add("");
		}
		
		return lines;
	}
	
	private static double calculateUniformHazardVal(DiscretizedFunc curve, double targetProb, double targetDuration) {
		double prob = ReturnPeriodUtils.calcExceedanceProb(targetProb, targetDuration, 1d);
		return calculateUniformHazardVal(curve, prob);
	}
	
	private static double calculateUniformHazardVal(DiscretizedFunc curve, double prob) {
		double iml;
		try {
			iml = curve.getFirstInterpolatedX_inLogXLogYDomain(prob);
		} catch (RuntimeException e) {
			System.out.println("prob: "+(float)prob);
			System.out.println("curve: "+curve);
			System.out.println();
			System.out.flush();
			throw e;
		}
		return iml;
	}
	
	private static void plotSpectra(double period, Map<Location, ? extends DiscretizedFunc> curveMap,
			File outputDir, String identifier, String prefix, double spacing, Region region, boolean replot)
					throws GMT_MapException, IOException {
		ArbDiscrGeoDataSet xyz = new ArbDiscrGeoDataSet(false);
		for (Location loc : curveMap.keySet()) {
			double val = curveMap.get(loc).getY(period);
			xyz.set(loc, val);
		}
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance();
		double contour;
		if (prefix.contains("_sle")) {
			cpt = cpt.rescale(0d, 0.5);
			contour = 0.05;
		} else if (period == 1d) {
			cpt = cpt.rescale(0d, 2d);
			contour = 0.1;
		} else if (period == 0.1d) {
			cpt = cpt.rescale(0d, 3d);
			contour = 0.2;
		} else {
			cpt = cpt.rescale(0d, xyz.getMaxZ());
			contour = 0.1;
		}
		GMT_Map map = new GMT_Map(region, xyz, spacing, cpt);
		String label = identifier+", "+(float)period+"s SA";
		prefix += "_"+(float)period;
		if (!replot && new File(outputDir, prefix+".png").exists()) {
			System.out.println("Skipping "+label);
			return;
		}
		MCErMapGenerator.applyGMTSettings(map, cpt, label); 
		map.setContourIncrement(contour);
		FaultBasedMapGen.LOCAL_MAPGEN = false;
		System.out.println("Plotting map: "+label);
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
	}
	
	private static void plotPGA(GeoDataSet xyz, File outputDir, String label, String prefix, double spacing, Region region, boolean replot)
			throws GMT_MapException, IOException {
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance();
		cpt = cpt.rescale(0d, 1.5d);
		GMT_Map map = new GMT_Map(region, xyz, spacing, cpt);
		if (!replot && new File(outputDir, prefix+".png").exists()) {
			System.out.println("Skipping "+label);
			return;
		}
		MCErMapGenerator.applyGMTSettings(map, cpt, label);
		map.setContourIncrement(0.1);
		FaultBasedMapGen.LOCAL_MAPGEN = false;
		System.out.println("Plotting map: "+label);
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
	}

}
