package org.opensha.sha.cybershake.etas;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.uncertainty.UncertainArbDiscFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotUtils;

import scratch.UCERF3.erf.ETAS.analysis.ETAS_HazardChangePlot;

public class ETAS_CombinedHazardGainPlot {

	public static void main(String[] args) throws IOException {
		File baseDir = new File("/data/kevin/git/cybershake-etas");
		
		File mojaveDir = new File(baseDir, "Mojave_Point_M6");
		File bombayDir = new File(baseDir, "2009_Bombay_Beach_M6");
		File mojaveCSVFile = new File(mojaveDir, "etas_plots/plots/hazard_change_100km_m7.0.csv");
		File bombayCSVFile = new File(bombayDir, "etas_plots/plots/hazard_change_100km_m7.0.csv");

		CSVFile<String> bombayCSV = CSVFile.readFile(bombayCSVFile, true);
		CSVFile<String> mojaveCSV = CSVFile.readFile(mojaveCSVFile, true);
		
		File outputDir = new File("/home/kevin/Documents/papers/2021_OEF_Directivity/figures");
		String prefix = "figure_2";
		
		int minMag = 7;
		
		Range xRange = new Range(1d / (365.25 * 24), 10d);
		Range yRange = new Range(1d, 10000);
		
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		boolean[] trueFalse = {true, false};
		
		for (boolean isBombay : trueFalse) {
			CSVFile<String> csv;
			Color color;
			String name;
			if (isBombay) {
				csv = bombayCSV;
				color = Color.CYAN.darker();
				name = "Bombay Beach M6";
			} else {
				csv = mojaveCSV;
				color = Color.MAGENTA.darker();
				name = "Mojave M6";
			}
			
			DiscretizedFunc gainFunc = new ArbitrarilyDiscretizedFunc();
			DiscretizedFunc upperFunc = new ArbitrarilyDiscretizedFunc();
			DiscretizedFunc lowerFunc = new ArbitrarilyDiscretizedFunc();
			
			for (int row=1; row<csv.getNumRows(); row++) {
				double duration = csv.getDouble(row, 0);
				double tdProb = csv.getDouble(row, 2);
				double etasProb = csv.getDouble(row, 3);
				double lowProb = csv.getDouble(row, 4);
				double highProb = csv.getDouble(row, 5);
				
				gainFunc.set(duration, etasProb/tdProb);
				upperFunc.set(duration, highProb/tdProb);
				lowerFunc.set(duration, lowProb/tdProb);
			}
			
			gainFunc.setName(name);
			funcs.add(gainFunc);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 4f, color));
			
//			UncertainArbDiscDataset combFunc = new UncertainArbDiscDataset(gainFunc, lowerFunc, upperFunc);
//			combFunc.setName("95% conf");
//			funcs.add(combFunc);
//			Color transColor = new Color(color.getRed(), color.getGreen(), color.getBlue(), 60);
//			chars.add(new PlotCurveCharacterstics(PlotLineType.SHADED_UNCERTAIN, 4f, transColor));
		}
		
		List<XYTextAnnotation> anns = ETAS_HazardChangePlot.buildPlotAnnotations(false, funcs, chars, yRange);
		for (XYTextAnnotation ann : anns) {
			String text = ann.getText();
			text = text.replace(" hr", " hour");
			text = text.replace(" d", " day");
			text = text.replace(" wk", " week");
			text = text.replace(" mo", " month");
			text = text.replace(" yr", " year");
			ann.setText(text);
		}
		
		PlotSpec spec = new PlotSpec(funcs, chars, " ", "Forecast Timespan (years)", "M≥"+minMag+" Probability Gain");
		spec.setPlotAnnotations(anns);
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = PlotUtils.initHeadless();
		gp.drawGraphPanel(spec, true, true, xRange, yRange);
		PlotUtils.writePlots(outputDir, prefix, gp, 1000, 850, true, true, true);
		
		// now hazard curves
		
		xRange = new Range(1e-2, 1e1);
		yRange = new Range(1e-8, 1e-1);
		for (boolean gmpe : trueFalse) {
			String csvPrefix;
			if (gmpe) {
				csvPrefix = "hazard_curves_gmpe_STNI_5s_one_week";
				prefix = "figure_4a";
			} else {
				csvPrefix = "hazard_curves_cs_STNI_5s_one_week";
				prefix = "figure_4b";
			}
			
			funcs = new ArrayList<>();
			chars = new ArrayList<>();
			
			for (boolean isBombay : trueFalse) {
				File resources;
				Color color;
				String name;
				if (isBombay) {
					resources = new File(bombayDir, "resources");
					color = Color.CYAN.darker();
					name = "Bombay Beach M6";
				} else {
					resources = new File(mojaveDir, "resources");
					color = Color.MAGENTA.darker();
					name = "Mojave M6";
				}
				
				if (funcs.isEmpty()) {
					// add long term
					CSVFile<String> longTermCSV = CSVFile.readFile(new File(resources, csvPrefix+"_long_term.csv"), true);
					DiscretizedFunc[] curves = ETAS_ScenarioPageGen.curvesFromCSV(longTermCSV);
					DiscretizedFunc tiCurve = curves[0];
					DiscretizedFunc tdCurve = curves[1];
					
					tiCurve.setName("  Long-Term Time-Independent");
					funcs.add(tiCurve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 4f, Color.BLUE));
					
					tdCurve.setName("  Long-Term Time-Dependent");
					funcs.add(tdCurve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 4f, Color.GREEN.darker()));
				}
				
				CSVFile<String> etasCSV = CSVFile.readFile(new File(resources, csvPrefix+"_etas.csv"), true);
				DiscretizedFunc[] curves = ETAS_ScenarioPageGen.curvesFromCSV(etasCSV);
				DiscretizedFunc uniformCurve = curves[0];
				DiscretizedFunc etasCurve = curves[1];
				
				uniformCurve.setName(name+", Uniform CHD");
				funcs.add(uniformCurve);
				chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, color));
				
				etasCurve.setName("  "+name+", ETAS CHD");
				funcs.add(etasCurve);
				chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 4f, color));
				
				spec = new PlotSpec(funcs, chars, " ", "5s SA", "One Week Probability of Exceedance");
				spec.setLegendInset(true);
				
				gp = PlotUtils.initHeadless();
				gp.setLegendFontSize(22);
				gp.drawGraphPanel(spec, true, true, xRange, yRange);
				PlotUtils.writePlots(outputDir, prefix, gp, 1000, 850, true, true, true);
			}
		}
	}

}
