package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.StatUtils;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;

public class RunAmpCompare {

	public static void main(String[] args) throws IOException {
//		CyberShakeStudy study = CyberShakeStudy.STUDY_19_3_RSQSIM_ROT_2585;
//		int runID1 = 7014;
//		int runID2 = 7054;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
//		int runID1 = 6707;
//		int runID2 = 7036;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
////		int runID1 = 6707;
////		int runID2 = 7036;
////		int runID1 = 5622;
////		int runID2 = 5855;
////		int runID1 = 5612;
////		int runID2 = 5847;
//		int runID1 = 7052;
//		int runID2 = 7055;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
//		int runID1 = 7052;
//		int runID2 = 7218;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
//		int runID1 = 3837; // 0.5hz
//		int runID2 = 3842; // 1hz
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz;
//		int runID1 = 7237; // 0.5hz
//		int runID2 = 7236; // 1hz
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8; // just needs to be U2 and production
		int runID1 = 4293; // 0.5hz
		int runID2 = 8686; // 1hz
		
//		File outputDir = new File("/home/kevin/CyberShake/rotation_debug");
//		File outputDir = new File("/tmp/cs_old_1hz");
		File outputDir = new File("/tmp/cs_debug");
		
//		Range distRange = null;
//		Range magRange = null;
		Range distRange = new Range(0d, 70d);
		Range magRange = new Range(6d, 7d);
		
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
//		CybershakeIM[] ims = {
//				CybershakeIM.getSA(CyberShakeComponent.RotD100, 3d),
//		};
		CybershakeIM[] ims = {
				CybershakeIM.getSA(CyberShakeComponent.RotD50, 2d),
				CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d),
				CybershakeIM.getSA(CyberShakeComponent.RotD50, 5d),
				CybershakeIM.getSA(CyberShakeComponent.RotD50, 10d)
//				CybershakeIM.getSA(CyberShakeComponent.GEOM_MEAN, 3d),
//				CybershakeIM.getSA(CyberShakeComponent.GEOM_MEAN, 5d),
//				CybershakeIM.getSA(CyberShakeComponent.GEOM_MEAN, 10d)
		};
		
		// if true, we average across all RVs instead of comparing them directly
		// needs to be true if the rupture generators are different
		boolean averageAcrossRVs = true;
		
		CSVFile<String> csv = new CSVFile<>(true);
		List<String> header = new ArrayList<>();
		header.add("");
		for (CybershakeIM im : ims)
			header.add((float)im.getVal()+"s "+im.getComponent().getShortName());
		csv.addLine(header);
		List<List<String>> statsCols = new ArrayList<>();
		List<String> statsLabels = null;
		
		DBAccess db = study.getDB();
		Runs2DB runs2db = new Runs2DB(db);
		
		CybershakeRun run1 = runs2db.getRun(runID1);
		CybershakeRun run2 = runs2db.getRun(runID2);
		String siteName = new SiteInfo2DB(db).getSiteFromDB(run1.getSiteID()).short_name;
		String prefix = siteName+"_"+runID2+"_vs_"+runID1;
		
		DecimalFormat oDF = new DecimalFormat("0.##");
		if (magRange != null)
			prefix += "_m"+oDF.format(magRange.getLowerBound())+"_"+oDF.format(magRange.getUpperBound());
		if (distRange != null)
			prefix += "_r"+oDF.format(distRange.getLowerBound())+"_"+oDF.format(distRange.getUpperBound());
		
		Preconditions.checkNotNull(run1);
		Preconditions.checkNotNull(run2);
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, null, study.getERF());
		
		AbstractERF erf = null;
		if (magRange != null || distRange != null)
			erf = study.getERF();
		Location siteLoc = null;
		if (distRange != null)
			siteLoc = new SiteInfo2DB(db).getLocationForSiteID(run1.getSiteID());
		
		for (CybershakeIM im : ims) {
			double[][][] amps1;
			double[][][] amps2;
			try {
				amps1 = amps2db.getAllIM_Values(run1.getRunID(), im);
				amps2 = amps2db.getAllIM_Values(run2.getRunID(), im);
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			}
			
			System.out.println(im);
			MinMaxAveTracker absDiffTrack = new MinMaxAveTracker();
			MinMaxAveTracker ratioTrack = new MinMaxAveTracker();
			
			Preconditions.checkState(amps1.length == amps2.length,
					"Source lenghts inconsistent, %s != %s", amps1.length, amps2.length);

			DefaultXY_DataSet scatter = new DefaultXY_DataSet();
			DefaultXY_DataSet scatterDiff = new DefaultXY_DataSet();
			
			double maxAbsDiff = 0d;
			String maxAbsStr = null;
			double maxPosRatio = 1d;
			String maxRatioStr = null;
			
			List<Double> linearDiffs = new ArrayList<>();
			List<Double> linearAbsDiffs = new ArrayList<>();
			List<Double> logDiffs = new ArrayList<>();
			List<Double> logAbsDiffs = new ArrayList<>();
			List<Double> linearRatios = new ArrayList<>();
			
			double maxLinearAbsDiff = 0d;
			String maxLinearAbsDiffSource = null;
			double maxLogAbsDiff = 0d;
			String maxLogAbsDiffSource = null;
			double maxRatio = 0d;
			String maxRatioSource = null;
			double minRatio = Double.POSITIVE_INFINITY;
			String minRatioSource = null;
			
			for (int sourceID=0; sourceID<amps1.length; sourceID++) {
				if (amps1[sourceID] == null) {
					Preconditions.checkState(amps2[sourceID] == null, "no sourceID=%s amps for run %s, but %s amps for run %s",
							sourceID, runID1, amps2[sourceID] == null ? 0 : amps2[sourceID].length, runID2);
					continue;
				}
				Preconditions.checkArgument(amps1[sourceID].length == amps2[sourceID].length,
						"run %s has %s rups for source %s, but %s has %s",
						runID1, amps1[sourceID].length, sourceID, runID2, amps2[sourceID].length);
				for (int rupID=0; rupID<amps1[sourceID].length; rupID++) {
					if (amps1[sourceID][rupID] == null) {
						Preconditions.checkState(amps2[sourceID][rupID] == null,
								"no sourceID=%s, rupID=%s amps for run %s, but %s amps for run %s",
								sourceID, rupID, runID1, amps2[sourceID][rupID] == null ? 0 : amps2[sourceID][rupID].length, runID2);
						continue;
					}
					if (magRange != null) {
						double mag = erf.getRupture(sourceID, rupID).getMag();
						if (!magRange.contains(mag))
							continue;
					}
					if (magRange != null) {
						RuptureSurface surf = erf.getRupture(sourceID, rupID).getRuptureSurface();
						double dist = surf.getDistanceRup(siteLoc);
						if (!distRange.contains(dist))
							continue;
					}
					double[] rvVals1, rvVals2;
					if (averageAcrossRVs) {
						rvVals1 = new double[] {StatUtils.mean(amps1[sourceID][rupID])};
						rvVals2 = new double[] {StatUtils.mean(amps2[sourceID][rupID])};
					} else {
						rvVals1 = amps1[sourceID][rupID];
						rvVals2 = amps2[sourceID][rupID];
					}
					Preconditions.checkArgument(rvVals1.length == rvVals2.length,
							"run %s has %s rups for source %s rup %s, but %s has %s",
							runID1, rvVals1.length, sourceID, rupID, runID2, rvVals2.length);
					for (int rvID=0; rvID<rvVals1.length; rvID++) {
						double v1 = rvVals1[rvID];
						double v2 = rvVals2[rvID];
						v1 /= HazardCurveComputation.CONVERSION_TO_G;
						v2 /= HazardCurveComputation.CONVERSION_TO_G;
						scatter.set(Math.log(v1), Math.log(v2));
						
						double diff = v2 - v1;
						double absDiff = Math.abs(diff);
						double logDiff = Math.log(v2) - Math.log(v1);
						
						scatterDiff.set(Math.log(v1), logDiff);
						double absLogDiff = Math.abs(logDiff);
						double ratio = v2/v1;
						linearDiffs.add(v2 - v1);
						linearAbsDiffs.add(Math.abs(v2 - v1));
						logDiffs.add(Math.log(v2) - Math.log(v1));
						logAbsDiffs.add(Math.abs(Math.log(v2) - Math.log(v1)));
						linearRatios.add(v2/v1);
						String rupStr = "Source "+sourceID+", Rup "+rupID+", RV "+rvID;
						if (absDiff > maxLinearAbsDiff) {
							maxLinearAbsDiff = absDiff;
							maxLinearAbsDiffSource = rupStr;
						}
						if (absLogDiff > maxLogAbsDiff) {
							maxLogAbsDiff = absLogDiff;
							maxLogAbsDiffSource = rupStr;
						}
						if (ratio > maxRatio) {
							maxRatio = ratio;
							maxRatioSource = rupStr;
						}
						if (ratio < minRatio) {
							minRatio = ratio;
							minRatioSource = rupStr;
						}
						
						absDiffTrack.addValue(absDiff);
						if (ratio < 1)
							ratio = 1d/ratio;
						ratioTrack.addValue(ratio);
						if (absDiff > maxAbsDiff) {
							maxAbsDiff = absDiff;
							maxAbsStr = rupStr+": |"+v1+" - "+v2+"| = "+absDiff;
						}
						if (ratio > maxPosRatio) {
							maxPosRatio = ratio;
							if (v1 > v2)
								maxRatioStr = rupStr+": "+v1+" / "+v2+" = "+ratio;
							else
								maxRatioStr = rupStr+": "+v2+" / "+v1+" = "+ratio;
						}
					}
				}
			}
			
			System.out.println("Absolute Differences:");
			System.out.println("\t"+absDiffTrack);
			System.out.println("\tLargest: "+maxAbsStr);
			System.out.println("Ratio:");
			System.out.println("\t"+ratioTrack);
			System.out.println("\tLargest: "+maxRatioStr);
			
//			List<XY_DataSet> funcs = new ArrayList<>();
//			List<PlotCurveCharacterstics> chars = new ArrayList<>();
//			
//			funcs.add(scatter);
//			chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 2f, Color.BLACK));
//			PlotSpec spec = new PlotSpec(funcs, chars, "Run ID Comparison, "+(float)im.getVal()+"s "+im.getComponent(),
//					"Run "+runID1, "Run "+runID2);
//			HeadlessGraphPanel gp = new HeadlessGraphPanel();
//			gp.setTickLabelFontSize(18);
//			gp.setAxisLabelFontSize(24);
//			gp.setPlotLabelFontSize(24);
//			gp.setLegendFontSize(28);
//			gp.setBackgroundColor(Color.WHITE);
//			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
//			
//			double minVal = Math.min(scatter.getMinX(), scatter.getMinY());
//			double maxVal = Math.min(scatter.getMaxX(), scatter.getMaxY());
//			minVal = Math.pow(10, Math.floor(Math.log10(minVal)));
//			maxVal = Math.pow(10, Math.ceil(Math.log10(maxVal)));
//			
//			Range range = new Range(minVal, maxVal);
//			
//			gp.drawGraphPanel(spec, true, true, range, range);
//			
//			File file = new File(outputDir, prefix+"_scatter_"+(float)im.getVal()+"s_"+im.getComponent().getShortName());
//			gp.getChartPanel().setSize(800, 600);
//			gp.saveAsPNG(file.getAbsolutePath()+".png");
//			gp.saveAsPDF(file.getAbsolutePath()+".pdf");
			
			String myPrefix = prefix+"_scatter_"+(float)im.getVal()+"s_"+im.getComponent().getShortName();
			
			String imtName = im.getVal()+"s "+im.getMeasure().getShortName();
			
			List<XY_DataSet> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			int numPts = scatter.size();
			int ptAlpha;
			if (numPts > 100000)
				ptAlpha = 40;
			else if (numPts > 50000)
				ptAlpha = 60;
			else if (numPts > 10000)
				ptAlpha = 80;
			else if (numPts > 5000)
				ptAlpha = 100;
			else
				ptAlpha = 127;
			Color ptColor = new Color(0, 0, 0, ptAlpha);
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
			
			String title = runID1+" vs "+runID2+", "+siteName;
			
			PlotSpec spec = new PlotSpec(funcs, chars, title, "Ln "+runID1+" "+imtName, "Ln "+runID2+" "+imtName);
			
			HeadlessGraphPanel gp = PlotUtils.initHeadless();
			
			gp.drawGraphPanel(spec, false, false, range, range);
			
			PlotUtils.writePlots(outputDir, myPrefix+"_compare", gp, 1000, -1, true, false, false);
			
			funcs = new ArrayList<>();
			chars = new ArrayList<>();
			
			funcs.add(scatterDiff);
			chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, ptColor));
			
			funcs.add(binnedAvg(scatterDiff, 10));
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, avgColor));
			
			String diffLabel = "Ln "+imtName+" Diff ("+runID2+" - "+runID1+")";
			spec = new PlotSpec(funcs, chars, title, "Ln "+runID1+" "+imtName, diffLabel);
			
			gp.drawGraphPanel(spec, false, false, range, null);
			
			PlotUtils.writePlots(outputDir, myPrefix+"_compare_diff", gp, 1000, 1000, true, false, false);
			
			List<String> statsCol = new ArrayList<>();
			statsCols.add(statsCol);
			if (statsLabels == null) {
				statsLabels = new ArrayList<>();
				statsLabels.add("Differences of linear values (g)");
				statsLabels.add("Min");
				statsLabels.add("Max");
				statsLabels.add("Mean");
				statsLabels.add("Std Dev");
				statsLabels.add("Mean Abs");
				statsLabels.add("Source for max abs diff");
				statsLabels.add("");
				statsLabels.add("Differences of natural-log values");
				statsLabels.add("Min");
				statsLabels.add("Max");
				statsLabels.add("Mean");
				statsLabels.add("Std Dev");
				statsLabels.add("Mean Abs");
				statsLabels.add("Source for max abs log diff");
				statsLabels.add("");
				statsLabels.add("Linear Ratios");
				statsLabels.add("Min");
				statsLabels.add("Max");
				statsLabels.add("Mean");
				statsLabels.add("Source for min ratio");
				statsLabels.add("Source for max ratio");
			}
			double[] linearDiffsArray = Doubles.toArray(linearDiffs);
			double[] linearAbsDiffsArray = Doubles.toArray(linearAbsDiffs);
			statsCol.add("");
			statsCol.add((float)StatUtils.min(linearDiffsArray)+"");
			statsCol.add((float)StatUtils.max(linearDiffsArray)+"");
			statsCol.add((float)StatUtils.mean(linearDiffsArray)+"");
			statsCol.add((float)Math.sqrt(StatUtils.variance(linearDiffsArray))+"");
			statsCol.add((float)StatUtils.mean(linearAbsDiffsArray)+"");
			statsCol.add(maxLinearAbsDiffSource);

			double[] logDiffsArray = Doubles.toArray(logDiffs);
			double[] logAbsDiffsArray = Doubles.toArray(logAbsDiffs);
			statsCol.add("");
			statsCol.add("");
			statsCol.add((float)StatUtils.min(logDiffsArray)+"");
			statsCol.add((float)StatUtils.max(logDiffsArray)+"");
			statsCol.add((float)StatUtils.mean(logDiffsArray)+"");
			statsCol.add((float)Math.sqrt(StatUtils.variance(logDiffsArray))+"");
			statsCol.add((float)StatUtils.mean(logAbsDiffsArray)+"");
			statsCol.add(maxLogAbsDiffSource);
			
			double[] lineaRatiosArray = Doubles.toArray(linearRatios);
			statsCol.add("");
			statsCol.add("");
			statsCol.add((float)StatUtils.min(lineaRatiosArray)+"");
			statsCol.add((float)StatUtils.max(lineaRatiosArray)+"");
			statsCol.add((float)StatUtils.mean(lineaRatiosArray)+"");
			statsCol.add(minRatioSource);
			statsCol.add(maxRatioSource);
		}
		
		for (int i=0; i<statsLabels.size(); i++) {
			List<String> line = new ArrayList<>();
			line.add(statsLabels.get(i));
			for (List<String> statsCol : statsCols)
				line.add(statsCol.get(i));
			csv.addLine(line);
		}
		
		csv.writeToFile(new File(outputDir, prefix+"_stats.csv"));
		
		db.destroy();
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

}
