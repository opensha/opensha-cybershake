package scratch.kevin.cybershake;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.data.Range;
import org.opensha.commons.data.function.HistogramFunction;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;

public class U2MagHist {
	
	public static void main(String[] args) throws SQLException, IOException {
		MeanUCERF2 u2 = (MeanUCERF2) MeanUCERF2_ToDB.createUCERF2ERF();
		u2.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_INCLUDE);
		u2.setParameter(UCERF2.BACK_SEIS_RUP_NAME, UCERF2.BACK_SEIS_RUP_POINT);
		double duration = 1d;
		u2.setParameter(UCERF2.PROB_MODEL_PARAM_NAME, UCERF2.PROB_MODEL_POISSON);
		u2.getTimeSpan().setDuration(duration);
		
		int erfID = 36;
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		HistogramFunction allFunc = new HistogramFunction(5.05, 41, 0.1);
		HistogramFunction u2FaultFunc = new HistogramFunction(5.05, 41, 0.1);
		HistogramFunction u2InCSFunc = new HistogramFunction(5.05, 41, 0.1);
		
		HistogramFunction allRateFunc = new HistogramFunction(5.05, 41, 0.1);
		HistogramFunction u2FaultRateFunc = new HistogramFunction(5.05, 41, 0.1);
		HistogramFunction u2InCSRateFunc = new HistogramFunction(5.05, 41, 0.1);
		
		int numFaultNotInCS = 0;
		
		u2.updateForecast();
		for (int sourceID=0; sourceID<u2.getNumSources(); sourceID++) {
			ProbEqkSource source = u2.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double mag = rup.getMag();
				int magI = allFunc.getClosestXIndex(mag);
				
				double rate = rup.getMeanAnnualRate(duration);
				
				allFunc.add(magI, 1d);
				allRateFunc.add(magI, rate);
				boolean fault = !rup.getRuptureSurface().isPointSurface();
				if (fault) {
					u2FaultFunc.add(magI, 1d);
					u2FaultRateFunc.add(magI, rate);
					
					String sql = "SELECT * from Ruptures WHERE ERF_ID = "+"'"+erfID+"' and "+
							"Source_ID = '"+sourceID+"' and Rupture_ID = '"+rupID+"'";
					ResultSet rs = db.selectData(sql);
					
					if (rs.next()) {
						u2InCSFunc.add(magI, 1d);
						u2InCSRateFunc.add(magI, rate);
					} else {
						if (numFaultNotInCS < 10)
							System.out.println("Source "+sourceID+", Rup "+rupID+" is not in CS!");
						numFaultNotInCS++;
					}
				}
			}
		}
		
		System.out.println("Num fault not in CS: "+numFaultNotInCS);
		
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		allFunc.setName("All Ruptures");
		funcs.add(allFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.BLUE));
		
		u2FaultFunc.setName("Fault Ruptures");
		funcs.add(u2FaultFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.RED));
		
		u2InCSFunc.setName("Fault Ruptures in CS");
		funcs.add(u2InCSFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.BLACK));
		
		File outputDir = new File("/tmp");
		
		PlotSpec spec = new PlotSpec(funcs, chars, "Mag", "Count", "U2 Mags");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(24);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(28);
		gp.setBackgroundColor(Color.WHITE);
		
		Range xRange = null;
		Range yRange = null;
		
		gp.drawGraphPanel(spec, false, false, xRange, yRange);
		
		File file = new File(outputDir, "u2_mag_counts");
		gp.getChartPanel().setSize(800, 600);
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		
		funcs = new ArrayList<>();
		chars = new ArrayList<>();
		
		allRateFunc.setName("All Ruptures");
		funcs.add(allRateFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.BLUE));
		
		u2FaultRateFunc.setName("Fault Ruptures");
		funcs.add(u2FaultRateFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.RED));
		
		u2InCSRateFunc.setName("Fault Ruptures in CS");
		funcs.add(u2InCSRateFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.BLACK));
		
		spec = new PlotSpec(funcs, chars, "Mag", "U2 Mags", "Annual Rate");
		spec.setLegendVisible(true);
		
		gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(24);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(28);
		gp.setBackgroundColor(Color.WHITE);
		
		gp.drawGraphPanel(spec, false, false, xRange, yRange);
		
		file = new File(outputDir, "u2_mag_rates");
		gp.getChartPanel().setSize(800, 600);
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		
		double minNonZero = Double.POSITIVE_INFINITY;
		double maxY = Math.pow(10, Math.ceil(Math.log10(allRateFunc.getMaxY())));
		for (XY_DataSet xy : funcs)
			for (Point2D pt : xy)
				if (pt.getY() > 0)
					minNonZero = Math.min(minNonZero, pt.getY());
		double minY = Math.pow(10, Math.floor(Math.log10(minNonZero)));
		yRange = new Range(minY, maxY);
		
		gp.drawGraphPanel(spec, false, true, xRange, yRange);
		
		file = new File(outputDir, "u2_mag_rates_log");
		gp.getChartPanel().setSize(800, 600);
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
		
		db.destroy();
	}

}
