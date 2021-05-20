package org.opensha.sha.cybershake.maps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.cybershake.ModProbConfig;
import org.opensha.sha.cybershake.bombay.ModProbConfigFactory;
import org.opensha.sha.cybershake.bombay.ScenarioBasedModProbConfig;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

public class PosterImageGen {

	protected static void saveCurves(String webAddr, String mainDir, String name, InterpDiffMapType type) throws IOException {
		if (!webAddr.endsWith("/"))
			webAddr += "/";
		webAddr += type.getPrefix();
		String pngAddr72 = webAddr + ".72.png";
		String pngAddr300 = webAddr + ".300.png";
		String psAddr = webAddr + ".ps";
		
		File pngFile300 = new File(mainDir + File.separator + name + ".300.png");
		File pngFile72 = new File(mainDir + File.separator + name + ".72.png");
		File psFile = new File(mainDir + File.separator + name + ".ps");
		
		FileUtils.downloadURL(pngAddr72, pngFile72);
		FileUtils.downloadURL(pngAddr300, pngFile300);
		FileUtils.downloadURL(psAddr, psFile);
	}
	
	private static void writeLocsFile(String fileName) throws IOException {
		FileWriter fw = new FileWriter(fileName);
		
		for (ModProbConfig config : ModProbConfigFactory.modProbConfigs.values()) {
			if (config instanceof ScenarioBasedModProbConfig) {
				Location hypo = ((ScenarioBasedModProbConfig)config).getHypocenter();
				fw.write((float)hypo.getLatitude() + " " + (float)hypo.getLongitude() + " " + config.getName() + "\n");
			}
		}
		
		fw.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String mainDir = "/home/kevin/CyberShake/oef/images";
		new File(mainDir).mkdirs();
		
		boolean logPlot = true;
		int imTypeID = 21;
//		int velModelID = 1; // TODO replaced with dataset ID
		boolean isProbAt_IML = true;
		double val = 0.2;
		
		boolean gainOnly = false;
		
		ScalarIMR baseMapIMR = AttenRelRef.CB_2008.instance(null);
		baseMapIMR.setParamDefaults();
		HardCodedInterpDiffMapCreator.setTruncation(baseMapIMR, 3.0);
		
		Double normCustomMin = -8.259081006598409;
		Double normCustomMax = -2.5;
		
		Double gainCustomMin = 0.0;
		Double gainCustomMax = 2.2;
		
		String normLabel = "POE "+(float)val+"G 3sec SA in 1 day";
		String gainLabel = "Probability Gain";
		
		InterpDiffMapType[] types = { InterpDiffMapType.INTERP_NOMARKS };
		HardCodedInterpDiffMapCreator.gainPlotTypes = types;
		HardCodedInterpDiffMapCreator.normPlotTypes = types;

//		try {
//			writeLocsFile(mainDir + "/locs.txt");
//			System.exit(0);
//			for (ModProbConfig config : ModProbConfigFactory.modProbConfigs.values()) {
//				String name = config.getName().replaceAll(" ", "");
//				if (!gainOnly) {
//					String normAddr = 
//						HardCodedInterpDiffMapCreator.getMap(logPlot, velModelID, imTypeID, normCustomMin, normCustomMax,
//								isProbAt_IML, val, baseMapIMR, config, false, normLabel);
//					saveCurves(normAddr, mainDir, name, InterpDiffMapType.INTERP_NOMARKS);
//				}
//				if (config instanceof ScenarioBasedModProbConfig) {
//					String gainAddr = 
//						HardCodedInterpDiffMapCreator.getMap(logPlot, velModelID, imTypeID, gainCustomMin, gainCustomMax,
//								isProbAt_IML, val, baseMapIMR, config, true, gainLabel);
//					saveCurves(gainAddr, mainDir, name+"_gain", InterpDiffMapType.INTERP_NOMARKS);
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			System.exit(2);
//		}
		System.exit(0);
	}

}
