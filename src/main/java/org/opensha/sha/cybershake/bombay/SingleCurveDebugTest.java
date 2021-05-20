package org.opensha.sha.cybershake.bombay;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.jfree.chart.ChartUtils;
import org.jfree.data.Range;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.GraphPanel;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.cybershake.plot.HazardCurvePlotCharacteristics;
import org.opensha.sha.gui.infoTools.IMT_Info;

public class SingleCurveDebugTest {
	
	HeadlessGraphPanel gp;
	
	public SingleCurveDebugTest() {
		gp = new HeadlessGraphPanel();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		double increaseMultFactor = 1000;
		
		Div365ProbModifier div365 = new Div365ProbModifier();
		BombayBeachHazardCurveCalc bombay = new BombayBeachHazardCurveCalc(db, increaseMultFactor,
				BombayBeachHazardCurveCalc.BOMBAY_LOC, 10d, "andreas", false);
		
		HazardCurveComputation calc = new HazardCurveComputation(db);
		
		ArbitrarilyDiscretizedFunc func = IMT_Info.getUSGS_PGA_Function();
		
		ArrayList<Double> imlVals = new ArrayList<Double>();
		
		for (int i=0; i<func.size(); i++) {
			imlVals.add(func.getX(i));
		}
		
//		int runID = 517; // s435
//		int runID = 215; // PTWN
		int runID = 576; // s758
		
		CybershakeIM imType = new PeakAmplitudesFromDB(db).getSupportedIMForPeriod(3d, IMType.SA, CyberShakeComponent.GEOM_MEAN, runID);
		
		calc.setRupProbModifier(div365);
		DiscretizedFunc origCurve = calc.computeHazardCurve(imlVals, runID, imType);
		ArbitrarilyDiscretizedFunc.writeSimpleFuncFile(origCurve, "/tmp/origCurve_"+runID+".txt");
		
		calc.setRupVarProbModifier(bombay);
		DiscretizedFunc modCurve = calc.computeHazardCurve(imlVals, runID, imType);
		ArbitrarilyDiscretizedFunc.writeSimpleFuncFile(modCurve, "/tmp/modCurve_"+runID+".txt");
		
		SingleCurveDebugTest plot = new SingleCurveDebugTest();
		
		plot.plot(origCurve, modCurve, "/tmp/test_curves_" + runID + ".png");
		
		System.exit(0);
	}
	
	public void plot(DiscretizedFunc origCurve, DiscretizedFunc modCurve, String outFile) throws IOException {
		ArrayList<DiscretizedFunc> curves = new ArrayList<DiscretizedFunc>();
		curves.add(origCurve);
		curves.add(modCurve);
		
		boolean xLog = true;
		boolean yLog = true;
		
		Range xRange = new Range(1e-4, 1e1);
		Range yRange = new Range(1e-16, 1e0);
		
		String title = "CyberShake Curves";
		
		this.gp.drawGraphPanel("3s SA", "Probability Rate (1/yr)", curves, null, xLog, yLog, title, xRange, yRange);
		this.gp.setVisible(true);
		
		this.gp.validate();
		this.gp.repaint();
		
		System.out.println("Saving PNG to: " + outFile);
		ChartUtils.saveChartAsPNG(new File(outFile), gp.getChartPanel().getChart(), 600, 500);
	}

}
