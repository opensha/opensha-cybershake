package org.opensha.sha.cybershake.calc.mcer;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
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
import org.opensha.commons.util.binFile.BinaryGeoDatasetRandomAccessFile;
import org.opensha.commons.util.binFile.BinaryXYZRandomAccessFile;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.hazardMap.BinaryRandomAccessHazardCurveWriter;
import org.opensha.sha.calc.mcer.MCErMapGenerator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class MPJ_GMPE_MCErCacheGenResultReorg {
	
	public static void main(String[] args) throws Exception {
		File mainDir = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen");
//		String prefix = "2016_09_30-ucerf3_downsampled_ngaw2_binary_0.02_";
//		String prefix = "2017_01_20-ucerf3_downsampled_ngaw2_binary_0.02_";
		String prefix = "2017_05_19-ucerf3_downsampled_ngaw2_binary_0.02_";
		String dataFileName = "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_mcer.bin";
		String pgaFileName = "NGAWest_2014_NoIdr_MeanUCERF3_downsampled_RotD100_pga.bin";
		File outputDir = new File(mainDir, prefix+"results");
		double spacing = 0.02;
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
		
		for (File dir : mainDir.listFiles()) {
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
			Map<Location, ArbitrarilyDiscretizedFunc> map = reader.getCurveMap();
			System.out.println("Loaded "+map.size());
			for (Location loc : map.keySet()) {
				ArbitrarilyDiscretizedFunc mcer = map.get(loc);
				Preconditions.checkNotNull(mcer);
				for (Point2D pt : mcer)
					Preconditions.checkState(Doubles.isFinite(pt.getY()));
			}
			System.out.println("All values validated\n");
			
			String identifier = dirName.substring(prefix.length());
			Preconditions.checkState(!identifier.isEmpty());
			Files.copy(dataFile, new File(outputDir, identifier+".bin"));
			
			if (plotPeriods != null)
				for (double period : plotPeriods)
					plotSpectrum(period, map, outputDir, identifier, spacing, region, replot);
			
			// now PGA
			File pgaFile = new File(dir, pgaFileName);
			if (pgaFile.exists()) {
				System.out.println("Doing PGA");
				ArbDiscrGeoDataSet pgaData = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(pgaFile);
				for (int i=0; i<pgaData.size(); i++)
					Preconditions.checkState(Doubles.isFinite(pgaData.get(i)),
							"Non Finite PGA at index %s/%s: %s", i, pgaData.size(), pgaData.get(i));
				System.out.println("All PGA values validated\n");
				Files.copy(pgaFile, new File(outputDir, identifier+"_pga.bin"));
				
				if (plotPeriods != null) {
					plotPGA(pgaData, outputDir, identifier, spacing, region, replot);
				}
			}
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
		 */
		System.out.println("Creating D_default");
		File dIn = new File(new File(mainDir, prefix+"classD"), dataFileName);
		Preconditions.checkState(dIn.exists());
		File cIn = new File(new File(mainDir, prefix+"classC"), dataFileName);
		Preconditions.checkState(cIn.exists());
		File dDefaultOut = new File(outputDir, "classD_default.bin");
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
		if (plotPeriods != null)
			for (double period : plotPeriods)
				plotSpectrum(period, dDeafultMap, outputDir, "classD_default", spacing, region, replot);
		
		// now the same but for PGA
		File dInPGA = new File(new File(mainDir, prefix+"classD"), pgaFileName);
		if (dInPGA.exists()) {
			System.out.println("Creating PGA D_default");
			File cInPGA = new File(new File(mainDir, prefix+"classC"), pgaFileName);
			ArbDiscrGeoDataSet dPGA = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(dInPGA);
			ArbDiscrGeoDataSet cPGA = BinaryGeoDatasetRandomAccessFile.loadGeoDataset(cInPGA);
			Preconditions.checkState(dPGA.size() == cPGA.size());
			File dDefaultPGAOut = new File(outputDir, "classD_default_pga.bin");
			BinaryGeoDatasetRandomAccessFile dDefaultPGA = new BinaryGeoDatasetRandomAccessFile(
					dDefaultPGAOut, ByteOrder.BIG_ENDIAN, dPGA.size());
			dDefaultPGA.initialize();
			ArbDiscrGeoDataSet dDefaultPGAData = new ArbDiscrGeoDataSet(false);
			for (int i=0; i<dPGA.size(); i++) {
				Location loc = dPGA.getLocation(i);
				Preconditions.checkState(loc.equals(cPGA.getLocation(i)));
				double val = Math.max(dPGA.get(i), cPGA.get(i));
				dDefaultPGA.write(i, loc, val);
				dDefaultPGAData.set(loc, val);
			}
			dDefaultPGA.close();
			if (plotPeriods != null) {
				plotPGA(dDefaultPGAData, outputDir, "classD_default", spacing, region, replot);
			}
		}
	}
	
	private static void plotSpectrum(double period, Map<Location, ? extends DiscretizedFunc> curveMap,
			File outputDir, String identifier, double spacing, Region region, boolean replot)
					throws GMT_MapException, IOException {
		ArbDiscrGeoDataSet xyz = new ArbDiscrGeoDataSet(false);
		for (Location loc : curveMap.keySet()) {
			double val = curveMap.get(loc).getY(period);
			xyz.set(loc, val);
		}
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance();
		if (period == 1d) {
			cpt = cpt.rescale(0d, 2d);
		} else if (period == 0.1d) {
			cpt = cpt.rescale(0d, 3d);
		} else {
			cpt = cpt.rescale(0d, xyz.getMaxZ());
		}
		GMT_Map map = new GMT_Map(region, xyz, spacing, cpt);
		String label = identifier+", MCER, "+(float)period+"s SA";
		String prefix = identifier+"_"+(float)period;
		if (!replot && new File(outputDir, prefix+".png").exists()) {
			System.out.println("Skipping "+label);
			return;
		}
		MCErMapGenerator.applyGMTSettings(map, cpt, label);
		FaultBasedMapGen.LOCAL_MAPGEN = false;
		System.out.println("Plotting map: "+label);
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
	}
	
	private static void plotPGA(GeoDataSet xyz, File outputDir, String identifier, double spacing, Region region, boolean replot)
			throws GMT_MapException, IOException {
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance();
		cpt = cpt.rescale(0d, 1.5d);
		GMT_Map map = new GMT_Map(region, xyz, spacing, cpt);
		String label = identifier+", PGA G";
		String prefix = identifier+"_pga";
		if (!replot && new File(outputDir, prefix+".png").exists()) {
			System.out.println("Skipping "+label);
			return;
		}
		MCErMapGenerator.applyGMTSettings(map, cpt, label);
		FaultBasedMapGen.LOCAL_MAPGEN = false;
		System.out.println("Plotting map: "+label);
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
	}

}
