package scratch.kevin.cybershake.simulators;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipException;

import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;

import com.google.common.base.Preconditions;

import scratch.kevin.bbp.BBP_SimZipLoader;
import scratch.kevin.bbp.BBP_Site;

public class RotD_CSV_Compare {

	public static void main(String[] args) throws IOException {
		File origFile = new File("/home/kevin/markdown/cybershake-analysis/study_20_5_rsqsim_4983_skip65k/"
				+ "site_hazard_USC_ASK2014_Vs30Simulation.bak/resources/USC_rd50s.csv.gz");
		File newFile = new File("/data/kevin/markdown/cybershake-analysis/study_21_12_rsqsim_4983_skip65k/"
				+ "site_hazard_USC_ASK2014_Vs30Simulation/resources/USC_rd50s.csv.gz");
		
		File outputDir = new File("/tmp");
		
		String prefix = "erf_58_61_USC";
		String origName = "ERF58";
		String newName = "ERF61";
		String siteName = "USC";
		String imtName = "3s RD50";
		
		CSVFile<String> origCSV = CSVFile.readFile(origFile, true);
		CSVFile<String> newCSV = CSVFile.readFile(newFile, true);
		
		System.out.println("Orig has "+origCSV.getNumRows()+" rows");
		System.out.println("New has "+newCSV.getNumRows()+" rows");
		
		Map<Integer, Double> origRDs = loadRodDs(origCSV);
		Map<Integer, Double> newRDs = loadRodDs(newCSV);
		
		// swap them out for BBP instead
//		File bbpDir = new File("/data/kevin/bbp/parallel");
//		File bbpDirOrig = new File(bbpDir, "2020_05_05-rundir4983_stitched-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		File bbpDirNew = new File(bbpDir, "2021_11_07-rundir4983_stitched-all-m6.5-skipYears65000-noHF-vmLA_BASIN_500-cs500Sites");
//		origRDs = loadBBPRotDs(new File(bbpDirOrig, "results_rotD.zip"), origRDs.keySet(), 3d);
//		newRDs = loadBBPRotDs(new File(bbpDirNew, "results_rotD.zip"), origRDs.keySet(), 3d);
//		prefix = "erf_58_61_USC_bbp";
//		origName = "Old BBP";
//		newName = "New BBP";
		
		DefaultXY_DataSet scatter = new DefaultXY_DataSet();
		
		DefaultXY_DataSet scatterDiff = new DefaultXY_DataSet();
		
		DefaultXY_DataSet scatterDist = new DefaultXY_DataSet();
		
		MinMaxAveTracker diffTrack = new MinMaxAveTracker();
		
		Map<Integer, Double> dists = loadDists(origCSV);
		
		double maxDiff = 0d;
		int maxID = -1;
		
		for (Integer eventID : origRDs.keySet()) {
			double origRD = origRDs.get(eventID);
			Double newRD = newRDs.get(eventID);
			
			if (newRD == null) {
				System.out.println("Missing event "+eventID+" with rd="+origRD);
				continue;
			}
			double diff = newRD - origRD;
			
			scatter.set(origRD, newRD);
			diffTrack.addValue(diff);
			
			scatterDiff.set(origRD, diff);
			
			double dist = dists.get(eventID);
			scatterDist.set(dist, diff);
			
			if (Math.abs(diff) > maxDiff) {
				maxDiff = Math.abs(diff);
				maxID = eventID;
			}
		}
		
		System.out.println("Ln diffs: "+diffTrack);
		
		System.out.println("Max of "+maxDiff+" for event "+maxID);
		
		for (Integer eventID : newRDs.keySet())
			if (!origRDs.containsKey(eventID))
				System.out.println("Have new event "+eventID+" with rd="+newRDs.get(eventID));
		
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		Color ptColor = new Color(0, 0, 0, 40);
		Color avgColor = new Color(0, 255, 0, 127);
		
		funcs.add(scatter);
		chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, ptColor));
		
		double min = Math.floor(Math.min(scatter.getMinX(), scatter.getMinY()));
		double max = Math.ceil(Math.max(scatter.getMaxX(), scatter.getMaxY()));
		Range range = new Range(min, max);
		
		DefaultXY_DataSet line = new DefaultXY_DataSet();
		line.set(min, min);
		line.set(max, max);
		
		funcs.add(line);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
		
		String title = origName+" vs "+newName+", "+siteName;
		
		PlotSpec spec = new PlotSpec(funcs, chars, title, "Ln "+origName+" "+imtName, "Ln "+newName+" "+imtName);
		
		HeadlessGraphPanel gp = PlotUtils.initHeadless();
		
		gp.drawGraphPanel(spec, false, false, range, range);
		
		PlotUtils.writePlots(outputDir, prefix+"_compare", gp, 1000, -1, true, false, false);
		
		funcs = new ArrayList<>();
		chars = new ArrayList<>();
		
		funcs.add(scatterDiff);
		chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, ptColor));
		
		funcs.add(binnedAvg(scatterDiff, 10));
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, avgColor));
		
		String diffLabel = "Ln "+imtName+" Diff ("+newName+" - "+origName+")";
		spec = new PlotSpec(funcs, chars, title, "Ln "+origName+" "+imtName, diffLabel);
		
		gp.drawGraphPanel(spec, false, false, range, null);
		
		PlotUtils.writePlots(outputDir, prefix+"_compare_diff", gp, 1000, 1000, true, false, false);
		
		funcs = new ArrayList<>();
		chars = new ArrayList<>();
		
		funcs.add(scatterDist);
		chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, ptColor));
		
		funcs.add(binnedAvg(scatterDist, 10));
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, avgColor));
		
		spec = new PlotSpec(funcs, chars, title, "Distance Rup (km)", diffLabel);
		
		gp.drawGraphPanel(spec, false, false, null, null);
		
		PlotUtils.writePlots(outputDir, prefix+"_compare_dists", gp, 1000, 1000, true, false, false);
	}
	
	private static EvenlyDiscretizedFunc binnedAvg(DefaultXY_DataSet scatter, int num) {
		EvenlyDiscretizedFunc discr = new EvenlyDiscretizedFunc(scatter.getMinX(), scatter.getMaxX(), num);
		
		int[] counts = new int[num];
		for (Point2D pt : scatter) {
			int bin = discr.getClosestXIndex(pt.getX());
			counts[bin]++;
			discr.add(bin, pt.getY());
		}
		
		for (int i=0; i<num; i++)
			discr.set(i, discr.getY(i)/(double)counts[i]);
		
		return discr;
	}
	
	private static Map<Integer, Double> loadRodDs(CSVFile<String> csv) {
		Map<Integer, Double> ret = new HashMap<>();
		for (int row=1; row<csv.getNumRows(); row++) {
			int eventID = csv.getInt(row, 0);
			double rd50 = csv.getDouble(row, 5);
			ret.put(eventID, rd50);
		}
		return ret;
	}
	
	private static Map<Integer, Double> loadDists(CSVFile<String> csv) {
		Map<Integer, Double> ret = new HashMap<>();
		for (int row=1; row<csv.getNumRows(); row++) {
			int eventID = csv.getInt(row, 0);
			double dist = csv.getDouble(row, 2);
			ret.put(eventID, dist);
		}
		return ret;
	}
	
	private static Map<Integer, Double> loadBBPRotDs(File zipFile, Collection<Integer> eventIDs, double period)
			throws ZipException, IOException {
		List<BBP_Site> bbpSites = BBP_Site.readFile(zipFile.getParentFile());
		BBP_Site usc = null;
		for (BBP_Site site : bbpSites)
			if (site.getName().trim().equals("USC"))
				usc = site;
		Preconditions.checkNotNull(usc);
		BBP_SimZipLoader loader = new BBP_SimZipLoader(zipFile, bbpSites);
		Map<Integer, Double> ret = new HashMap<>();
		for (int eventID : eventIDs) {
			try {
				DiscretizedFunc rd50s = loader.readRotD50(usc, "event_"+eventID);
				ret.put(eventID, Math.log(rd50s.getInterpolatedY(period)));
			} catch (Exception e) {};
		}
		return ret;
	}

}
