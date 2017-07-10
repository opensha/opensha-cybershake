package org.opensha.sha.cybershake;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPOutputStream;

import org.opensha.commons.geo.Location;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.EvenlyGridCenteredSurface;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;

public class ERF_Rupture_File_Writer {

	public static void writeRuptureFile(ProbEqkRupture rup, int srcID, int rupID, File dir, boolean gZip) throws IOException {
		Writer fw;
		String fileName =  srcID+"_"+rupID+".txt";
		if (gZip)
			fw = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(new File(dir, fileName+".gz"))), "UTF-8");
		else
			fw = new FileWriter(new File(dir, fileName));

		EvenlyGriddedSurface rawSurf = (EvenlyGriddedSurface)rup.getRuptureSurface();
		AbstractEvenlyGriddedSurface centeredSurf = new EvenlyGridCenteredSurface(
				rawSurf);

		fw.write("Probability = "+rup.getProbability()+"\n");
		fw.write("Magnitude = "+(float)rup.getMag()+"\n");
		fw.write("GridSpacing = "+(float)rawSurf.getAveGridSpacing()+"\n");
		fw.write("NumRows = "+centeredSurf.getNumRows()+"\n");
		fw.write("NumCols = "+centeredSurf.getNumCols()+"\n");

		double[] strikes = ERF2DB.getLocalStrikeList(rawSurf);

		double rake = rup.getAveRake();
		double dip = centeredSurf.getAveDip();

		fw.write("#   Lat         Lon         Depth      Rake    Dip     Strike"+"\n");
		for (int row=0; row<centeredSurf.getNumRows(); row++) {
			for (int col=0; col<centeredSurf.getNumCols(); col++) {
				Location loc = centeredSurf.get(row, col);
				fw.write((float)loc.getLatitude()+"    "+(float)loc.getLongitude()+"    "
						+(float)loc.getDepth()+"    "+(float)rake+"    "+(float)dip
						+"    "+(float)strikes[col]+"\n");
			}
		}

		fw.close();
	}

	private static HashMap<Integer, ArrayList<Integer>> loadRups(File file) throws FileNotFoundException, IOException {
		HashMap<Integer, ArrayList<Integer>> rups = new HashMap<Integer, ArrayList<Integer>>();

		for (String line : FileUtils.loadFile(file.getAbsolutePath())) {
			String[] split = line.split(" ");
			int sourceID = Integer.parseInt(split[0]);
			int rupID = Integer.parseInt(split[1]);
			if (!rups.containsKey(sourceID)) {
				rups.put(sourceID, new ArrayList<Integer>());
			}
			rups.get(sourceID).add(rupID);
		}

		return rups;
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		ERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		erf.updateForecast();
		boolean gZip = false;

		//		File dir = new File("/home/kevin/CyberShake/rupSurfaces/TEST_35");
		File dir = new File("/auto/rcf-104/CyberShake2007/ruptures/ALL_200m");

		HashMap<Integer, ArrayList<Integer>> rups = null;

		//		File rupsFile = new File(dir, "TEST_35.txt");
		//		rups = loadRups(rupsFile);

		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			if (rups != null && !rups.containsKey(sourceID))
				continue;

			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				if (rups != null && !rups.get(sourceID).contains(rupID))
					continue;

				ProbEqkRupture rup = source.getRupture(rupID);
				writeRuptureFile(rup, sourceID, rupID, dir, gZip);
			}
		}
	}

}
