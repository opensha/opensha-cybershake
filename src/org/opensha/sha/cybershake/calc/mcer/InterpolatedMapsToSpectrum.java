package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveWriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class InterpolatedMapsToSpectrum {

	public static void main(String[] args) throws FileNotFoundException, IOException {
//		boolean sameDir = false;
//		File studyDir = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100");
		double spacing = 0.002;
		boolean sameDir = true;
		File studyDir = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/interp_tests");
		File outputFile = new File(studyDir, "mcer_spectrum_"+(float)spacing+".bin");
		
		boolean isInputLog = false;
		
		double[] periods = { 2,3,4,5,7.5,10 };
//		double[] periods = { 2 };
		
		// load in the data
		Map<Location, DiscretizedFunc> spectrumMap = Maps.newHashMap();
		
		for (double period : periods) {
			File interpDataFile;
			String dirName = CyberShakeMCErMapGenerator.getPeriodDirName(period);
			if (sameDir) {
				interpDataFile = new File(studyDir, dirName+"_interp_"+(float)spacing+".txt");
			} else {
				System.out.println("Loading "+dirName);
				File periodDir = new File(studyDir, dirName);
				checkExists(periodDir);
				File saDir = new File(periodDir, "sa");
				checkExists(saDir);
				interpDataFile = new File(saDir, "combined_mcer_sa_map_data_interpolated.txt");
			}
			
			checkExists(interpDataFile);
			ArbDiscrGeoDataSet data = ArbDiscrGeoDataSet.loadXYZFile(interpDataFile.getAbsolutePath(), false);
			System.out.println("Loaded data for "+data.size()+" locations");
			if (spectrumMap.isEmpty()) {
				for (Location loc : data.getLocationList())
					spectrumMap.put(loc, new ArbitrarilyDiscretizedFunc());
			} else {
				Preconditions.checkState(spectrumMap.size() == data.size(),
						"data size mismatch, expected %s, have %s", spectrumMap.size(), data.size());
			}
			for (Location loc : data.getLocationList()) {
				double val = data.get(loc);
				if (isInputLog)
					val = Math.pow(10, val);
				spectrumMap.get(loc).set(period, val);
			}
		}
		
		System.out.println("Writing interpolated spectrum to: "+outputFile.getAbsolutePath());
		BinaryHazardCurveWriter writer = new BinaryHazardCurveWriter(outputFile);
		writer.writeCurves(spectrumMap);
	}
	
	private static void checkExists(File file) {
		Preconditions.checkState(file.exists(), "file doesn't exist: %s", file.getAbsolutePath());
	}

}
