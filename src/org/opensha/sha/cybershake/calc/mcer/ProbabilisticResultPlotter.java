package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.plot.PlotType;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.util.component.ComponentTranslation;

import com.google.common.collect.Lists;

public class ProbabilisticResultPlotter {
	
	static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
	
	private static void writeCSV(CyberShakeSiteRun site, CyberShakeComponent comp, DiscretizedFunc csSpectrum,
			DiscretizedFunc avgSpectrum, List<? extends ScalarIMR> gmpes, List<DiscretizedFunc> gmpeSpectrums,
			List<Double> periods, File outputFile, boolean vel) throws IOException {
//		writeCSV(site, comp, MCErCalcUtils.saToPsuedoVel(csSpectrum), gmpes, sasToPsuedoVel(gmpeSpectrums),
//				periods, outputFile);
		String units = "(g)";
		if (vel) {
			 csSpectrum = MCErCalcUtils.saToPsuedoVel(csSpectrum);
			 if (gmpeSpectrums != null)
				 gmpeSpectrums = sasToPsuedoVel(gmpeSpectrums);
			 if (avgSpectrum != null)
				 avgSpectrum = MCErCalcUtils.saToPsuedoVel(avgSpectrum);
			 units = "(cm/s)";
		}
		if (gmpes == null) {
			gmpes = Lists.newArrayList();
			gmpeSpectrums = Lists.newArrayList();
		}
		
		CSVFile<String> csv = new CSVFile<String>(true);
		
		List<String> header = Lists.newArrayList("Site Short Name", "Run ID", "Component",
				"Period", "CyberShake RTGM "+units);
		if (avgSpectrum != null)
			header.add("Weight Averaged RTGM");
		for (ScalarIMR gmpe : gmpes) {
			header.add(gmpe.getShortName()+" Metadata");
			header.add(gmpe.getShortName()+" RTGM "+units);
		}
		csv.addLine(header);
		
		List<ComponentTranslation> gmpeTrans = Lists.newArrayList();
		for (ScalarIMR gmpe : gmpes) {
			ComponentTranslation myGMPETrans =
					MCErCalcUtils.getSupportedComponentConverter(gmpe, comp.getGMPESupportedComponents());
			if (myGMPETrans == null) {
				gmpeTrans.add(null);
			} else {
				gmpeTrans.add(myGMPETrans);
			}
		}
		for (double period : periods) {
			List<String> line = Lists.newArrayList(site.getCS_Site().short_name, site.getCS_Run().getRunID()+"",
					comp.getShortName(), (float)period+"");
			if (csSpectrum.hasX(period))
				line.add(csSpectrum.getY(period)+"");
			else
				line.add("");
			if (avgSpectrum != null) {
				if (avgSpectrum.hasX(period))
					line.add(avgSpectrum.getY(period)+"");
				else
					line.add("");
			}
			for (int i=0; i<gmpes.size(); i++) {
				ComponentTranslation myGMPETrans = gmpeTrans.get(i);
				if (myGMPETrans == null) {
					line.add("GMPE component translation not needed or not supported");
				} else {
					line.add("GMPE component ("+myGMPETrans.getFromComponent()+") scaled to "
							+myGMPETrans.getToComponent()+" via factor of "+myGMPETrans.getScalingFactor(period)
							+" from "+myGMPETrans.getName());
				}
				DiscretizedFunc gmpeSpectrum = gmpeSpectrums.get(i);
				if (gmpeSpectrum.hasX(period))
					line.add(gmpeSpectrum.getY(period)+"");
				else
					line.add("");
			}
			csv.addLine(line);
		}
		
		csv.writeToFile(outputFile);
	}
	
	static void plotProbMCEr(CyberShakeSiteRun site, CyberShakeComponent comp, DiscretizedFunc csSpectrum,
			List<? extends ScalarIMR> gmpes, List<DiscretizedFunc> gmpeSpectrums,
			List<Double> periods, List<PlotType> plotTypes, boolean velPlot, File outputDir) throws IOException {
		plotProbMCEr(site, comp, csSpectrum, null, gmpes, gmpeSpectrums, periods, plotTypes, velPlot, outputDir);
	}
	
	static void plotProbMCEr(CyberShakeSiteRun site, CyberShakeComponent comp, DiscretizedFunc csSpectrum,
			DiscretizedFunc avgSpectrum, List<? extends ScalarIMR> gmpes, List<DiscretizedFunc> gmpeSpectrums,
			List<Double> periods, List<PlotType> plotTypes, boolean velPlot, File outputDir) throws IOException {
		String siteName = site.getCS_Site().short_name;
		
		String namePrefix = siteName+"_run"+site.getCS_Run().getRunID()+"_RTGM";
		String dateStr = dateFormat.format(new Date());
		PlotSpec spec = null;
		PlotSpec velSpec = null;
		
		for (PlotType type : plotTypes) {
			String name = namePrefix+"_"+comp.getShortName()+"_";
			if (velPlot)
				name += "vel_";
			name += dateStr+"."+type.getExtension();
			File outputFile = new File(outputDir, name);
			switch (type) {
			case CSV:
				writeCSV(site, comp, csSpectrum, avgSpectrum, gmpes, gmpeSpectrums,
						periods, outputFile, velPlot);
				break;
			case PDF:
				if (spec == null) {
					spec = getSpectrumPlot(siteName, comp, csSpectrum, gmpeSpectrums, "RTGM", "(g)");
					if (velPlot)
						velSpec = getSpectrumPlot(siteName, comp, MCErCalcUtils.saToPsuedoVel(csSpectrum),
								sasToPsuedoVel(gmpeSpectrums), "RTGM PSV", "(cm/sec)");
				}
				writeSpec(spec, outputFile, type, xLog, yLog, xRangeSA, yRangeSA);
				if (velPlot)
					writeSpec(velSpec, outputFile, type, xLog, yLog,
							xRangeVel, yRangeVel);
				break;
			case PNG:
				if (spec == null) {
					spec = getSpectrumPlot(siteName, comp, csSpectrum, gmpeSpectrums, "RTGM", "(g)");
					if (velPlot)
						velSpec = getSpectrumPlot(siteName, comp, MCErCalcUtils.saToPsuedoVel(csSpectrum),
								sasToPsuedoVel(gmpeSpectrums), "RTGM PSV", "(cm/sec)");
				}
				writeSpec(spec, outputFile, type, xLog, yLog, xRangeSA, yRangeSA);
				if (velPlot)
					writeSpec(velSpec, outputFile, type, xLog, yLog,
							xRangeVel, yRangeVel);
				break;

			default:
				throw new IllegalArgumentException("Unsupported plot type: "+type.getExtension());
			}
		}
	}
	
	static final boolean xLog = true;
	static final boolean yLog = true;
	static final Range xRangeSA = new Range(1d, 10d);
	static final Range yRangeSA = new Range(2e-2, 2e0);
	static final Range xRangeVel = new Range(1d, 10d);
//	static final Range yRangeVel = new Range(30, 300);
	static final Range yRangeVel = new Range(2e1, 2e3);
	
	static PlotSpec getSpectrumPlot(String siteName, CyberShakeComponent comp,
			DiscretizedFunc csSpectrum, List<DiscretizedFunc> gmpeSpectrums,
			String calcType, String units) {

		List<DiscretizedFunc> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();

		funcs.add(csSpectrum);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f,
				PlotSymbol.FILLED_CIRCLE, 4f, Color.BLACK));

		if (gmpeSpectrums != null && !gmpeSpectrums.isEmpty()) {
			funcs.addAll(gmpeSpectrums);
			for (int i=0; i<gmpeSpectrums.size(); i++) {
				PlotLineType plt;
				if (gmpeSpectrums.get(i).getName().contains("2014"))
					plt = PlotLineType.DOTTED;
				else
					plt = PlotLineType.DASHED;
				chars.add(new PlotCurveCharacterstics(plt, 2f,
						PlotSymbol.FILLED_CIRCLE, 4f, gmpeColors.get(i % gmpeColors.size())));
			}
		}

		PlotSpec spec = new PlotSpec(funcs, chars, siteName+" Spectrum", "Period (s)",
				comp.getShortName()+" "+calcType+" "+units);
		spec.setLegendVisible(true);
		return spec;
	}
	
	static List<DiscretizedFunc> sasToPsuedoVel(
			List<DiscretizedFunc> funcs) {
		if (funcs == null)
			return null;
		List<DiscretizedFunc> velFuncs = Lists.newArrayList();
		for (DiscretizedFunc saFunc : funcs)
			velFuncs.add(MCErCalcUtils.saToPsuedoVel(saFunc));
		return velFuncs;
	}
	
	private static final List<Color> gmpeColors = Lists.newArrayList(
    		new Color(255, 150, 150),
    		new Color(150, 150, 255),
    		new Color(150, 255, 150),
    		new Color(255, 180, 35),
    		new Color(160, 255, 255));
	
	static void writeSpec(PlotSpec spec, File outputFile,	PlotType plotType,
			boolean xLog, boolean yLog, Range xRange, Range yRange) throws IOException {

		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setBackgroundColor(Color.WHITE);
		gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);

		gp.drawGraphPanel(spec, xLog, yLog, xRange, yRange);
		gp.getChartPanel().setSize(1000, 800);
		gp.setVisible(true);

		gp.validate();
		gp.repaint();

		switch (plotType) {
		case PDF:
			gp.saveAsPDF(outputFile.getAbsolutePath());
			break;
		case PNG:
			gp.saveAsPNG(outputFile.getAbsolutePath());
			break;

		default:
			throw new IllegalArgumentException("Unsupported plot type: "+plotType.getExtension());
		}
	}

}
