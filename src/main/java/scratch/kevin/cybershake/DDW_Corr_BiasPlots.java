package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.ui.RectangleAnchor;
import org.jfree.data.Range;
import org.opensha.commons.calc.magScalingRelations.magScalingRelImpl.Ellsworth_B_WG02_MagAreaRel;
import org.opensha.commons.calc.magScalingRelations.magScalingRelImpl.HanksBakun2002_MagAreaRel;
import org.opensha.commons.calc.magScalingRelations.magScalingRelImpl.Somerville_2006_MagAreaRel;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotUtils;

public class DDW_Corr_BiasPlots {

	public static void main(String[] args) throws IOException {
		EvenlyDiscretizedFunc fixedCorrFunc = new EvenlyDiscretizedFunc(6.5d, 8.5d, 1000);
		EvenlyDiscretizedFunc buggyCorrFunc = new EvenlyDiscretizedFunc(6.5d, 8.5d, 1000);
		
		Ellsworth_B_WG02_MagAreaRel ellB = new Ellsworth_B_WG02_MagAreaRel();
		HanksBakun2002_MagAreaRel hb = new HanksBakun2002_MagAreaRel();
		Somerville_2006_MagAreaRel som = new Somerville_2006_MagAreaRel();
		
		double smallArea = Math.min(ellB.getMedianArea(6d), hb.getMedianArea(6d));
		double bigArea = Math.max(ellB.getMedianArea(9d), hb.getMedianArea(9d));
		EvenlyDiscretizedFunc u2AreaMagFunc = new EvenlyDiscretizedFunc(smallArea, bigArea, 10000);
		for (int i=0; i<u2AreaMagFunc.size(); i++) {
			double area = u2AreaMagFunc.getX(i);
			double ellBMag = ellB.getMedianMag(area);
			double hbMag = hb.getMedianMag(area);
			double mag = 0.5*(ellBMag + hbMag);
			u2AreaMagFunc.set(i, mag);
		}
		
		for (int i=0; i<fixedCorrFunc.size(); i++) {
			double mag = fixedCorrFunc.getX(i);
			double u2Area = u2AreaMagFunc.getFirstInterpolatedX(mag);
			double ellBArea = ellB.getMedianArea(mag);
			double hbArea = hb.getMedianArea(mag);
			double somArea = som.getMedianArea(mag);
			double aveU2Area = 0.5*(ellBArea + hbArea);
			
			double ellBMag = ellB.getMedianMag(u2Area);
			double hbMag = hb.getMedianMag(u2Area);
			double somMag = hb.getMedianMag(somArea);
			double calcMag = 0.5*(ellBMag + hbMag);
			
			double fixedCorr = som.getMedianArea(mag) / u2Area;
			double bugCorr = som.getMedianArea(ellBMag) / u2Area;
			
			fixedCorrFunc.set(i, fixedCorr);
			buggyCorrFunc.set(i, bugCorr);
			
			if (i % 100 == 0) {
				System.out.println("M"+(float)mag);
				System.out.println("\tAreas: U2="+(float)u2Area+", EllB="+(float)ellBArea+", HB="+(float)hbArea
						+", Ave="+(float)aveU2Area+", Som="+(float)somArea);
				System.out.println("\tMags: EllB="+(float)ellBMag+", HB="+(float)hbMag
						+", Ave="+(float)calcMag+", Som="+(float)somMag);
				System.out.println("\tCorrections: fixed="+(float)fixedCorr+", buggy="+(float)bugCorr);
			}
		}
		
		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		buggyCorrFunc.setName("U2 Buggy Segmented A-Faults");
		funcs.add(buggyCorrFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED.darker()));
		
		fixedCorrFunc.setName("Intended DDW Correction");
		funcs.add(fixedCorrFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE.darker()));
		
		Range xRange = new Range(fixedCorrFunc.getMinX(), fixedCorrFunc.getMaxX());
		Range yRange = new Range(1d, 2d);
		
		PlotSpec spec = new PlotSpec(funcs, chars, "CyberShake DDW Correction BugFix", "Magnitude", "DDW Correction Factor");
		spec.setLegendInset(RectangleAnchor.TOP_LEFT);
		
		HeadlessGraphPanel gp = PlotUtils.initHeadless();
		
		gp.drawGraphPanel(spec, false, false, xRange, yRange);
		
		PlotUtils.writePlots(new File("/tmp"), "cs_ddw_corr_bug", gp, 800, 700, true, true, false);
	}

}
