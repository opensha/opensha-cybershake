package scratch.kevin.cybershake.saPaper2024;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.ui.RectangleEdge;
import org.jfree.chart.ui.TextAnchor;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.xyz.ArbDiscrXYZ_DataSet;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZPlotSpec;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.cpt.CPT;

import com.google.common.base.Preconditions;

public class CheckboardPlot {

	public static void main(String[] args) throws IOException {
		List<File> refDirs = new ArrayList<>();
		List<String> prefixes = new ArrayList<>();
		List<String> outPrefixes = new ArrayList<>();
		List<Boolean> stdDevs = new ArrayList<>();
		List<Boolean> doIndvs = new ArrayList<>();
		List<String> titles = new ArrayList<>();
		List<double[]> outPeriods = new ArrayList<>();
		
		refDirs.add(new File("/home/kevin/markdown/cybershake-analysis/study_22_12_hf/gmpe_comparisons_NGAWest_2014_NoIdr_Vs30Thompson2020/resources"));
		prefixes.add("detrend_residuals");
		outPrefixes.add("residuals_ngaw2_lf");
		stdDevs.add(false);
		doIndvs.add(false);
		titles.add(" ");
		outPeriods.add(new double[] {2, 3, 5, 10});
		
		refDirs.add(new File("/home/kevin/markdown/cybershake-analysis/study_22_12_hf/gmpe_comparisons_NGAWest_2014_NoIdr_Vs30Thompson2020/resources"));
		prefixes.add("detrend_residuals");
		outPrefixes.add("residuals_ngaw2_hf");
		stdDevs.add(false);
		doIndvs.add(false);
		titles.add(" ");
		outPeriods.add(new double[] {0.1, 0.2, 0.5, 1});
		
		File outputDir = new File("/home/kevin/Documents/papers/2024_CyberShake_22_12/figures");
		
		CPT residualCPT = GMT_CPT_Files.GMT_POLAR.instance().rescale(-1.5, 1.5);
		residualCPT.setNanColor(Color.WHITE);
		CPT stdDevCPT = GMT_CPT_Files.BLACK_RED_YELLOW_UNIFORM.instance().reverse().rescale(0, 1d);
		stdDevCPT.setNanColor(Color.WHITE);
		
		DecimalFormat oDF = new DecimalFormat("0.##");
		
		HeadlessGraphPanel gp = PlotUtils.initHeadless();
		
		gp.getPlotPrefs().setPlotLabelFontSize(36);
		gp.getPlotPrefs().setAxisLabelFontSize(28);
		gp.getPlotPrefs().setTickLabelFontSize(26);
		Font annFont = new Font(Font.SANS_SERIF, Font.BOLD, 36);
		
		for (int n=0; n<refDirs.size(); n++) {
			File refDir = refDirs.get(n);
			String prefix = prefixes.get(n);
			String outPrefix = outPrefixes.get(n);
			String title = titles.get(n);
			boolean stdDev = stdDevs.get(n);
			boolean doIndv = doIndvs.get(n);
			
			CPT cpt = stdDev ? stdDevCPT : residualCPT;
			
			List<PlotSpec> specs = new ArrayList<>();
//			Range xRange = null;
//			Range yRange = null;
			Range xRange = new Range(10d, 200d);
			Range yRange = new Range(6.5d, 8d);
			
			double[] periods = outPeriods.get(n);
			
			for (int p=0; p<periods.length; p++) {
				String perPrefix;
				String perLabel;
				if (periods[p] == -1) {
					perPrefix = "pgv";
					perLabel = "PGV";
				} else {
					Preconditions.checkState(periods[p] > 0d);
					perPrefix = oDF.format(periods[p])+"s";
					perLabel = oDF.format(periods[p])+"s SA";
				}
				File csvFile = new File(refDir, prefix+"_"+perPrefix+".csv");
				CSVFile<String> csv = CSVFile.readFile(csvFile, true);
				
				ArbDiscrXYZ_DataSet xyz = new ArbDiscrXYZ_DataSet();
				
				double minDist = Double.POSITIVE_INFINITY;
				double maxDist = Double.NEGATIVE_INFINITY;
				double minMag = Double.POSITIVE_INFINITY;
				double maxMag = Double.NEGATIVE_INFINITY;
				
				for (int row=1; row<csv.getNumRows(); row++) {
					double distMin = csv.getDouble(row, 0);
					double distMax = csv.getDouble(row, 1);
					double magMin = csv.getDouble(row, 2);
					double magMax = csv.getDouble(row, 3);
					
					minDist = Math.min(minDist, distMin);
					maxDist = Math.max(maxDist, distMax);
					minMag = Math.min(minMag, magMin);
					maxMag = Math.max(maxMag, magMax);
					
					int count = csv.getInt(row, 4);
					
					double val;
					if (count == 0)
						val = Double.NaN;
					else if (stdDev && count == 1)
						val = Double.NaN;
					else if (stdDev)
						val = csv.getDouble(row, 10);
					else
						val = csv.getDouble(row, 5);
					
					double x = 0.5*(Math.log10(distMin) + Math.log10(distMax));
					double y = 0.5*(magMin + magMax);
					xyz.set(x, y, val);
				}
				
				if (xRange == null) {
//					xRange = new Range(Math.log10(minDist), Math.log10(maxDist));
					xRange = new Range(minDist, maxDist);
					yRange = new Range(minMag, maxMag);
				}
				
				XYZPlotSpec spec = new XYZPlotSpec(xyz, cpt, title, "Distance Rup  (km)", "Magnitude", "Mean Residual  (ln)");
				spec.setCPTPosition(RectangleEdge.LEFT);
				spec.setCPTTickUnit(stdDev ? 0.2 : 0.5);
				
				XYTextAnnotation perAnn = new XYTextAnnotation("  "+perLabel, xRange.getLowerBound(),
						yRange.getLowerBound() + 0.98*yRange.getLength());
				perAnn.setFont(annFont);
				perAnn.setTextAnchor(TextAnchor.TOP_LEFT);
				spec.addPlotAnnotation(perAnn);
				
				specs.add(spec);
				
				if (doIndv) {
					gp.drawGraphPanel(spec, true, false, xRange, yRange);
					PlotUtils.setYTick(gp, 0.2d);
					
					PlotUtils.writePlots(outputDir, outPrefix+"_"+perPrefix, gp, 800, 600, true, true, false);
				}
			}
			
			int width = 400 + 600*specs.size();
			List<Range> xRanges = new ArrayList<>();
			for (int i=0; i<specs.size(); i++)
				xRanges.add(xRange);
			List<Range> yRanges = List.of(yRange);
			gp.drawGraphPanel(specs, true, false, xRanges, yRanges);
//			PlotUtils.setXTick(gp, 0.25d);
			PlotUtils.setYTick(gp, 0.2d);
			PlotUtils.setSubplotGap(gp, 80);
			
			PlotUtils.writePlots(outputDir, outPrefix+"_combined", gp, width, 600, true, true, false);
		}
	}

}
