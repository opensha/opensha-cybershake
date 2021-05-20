package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.data.Range;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.mapping.PoliticalBoundariesData;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.simulators.srf.SRF_PointData;

public class SRF_LocationPlot {

	public static void main(String[] args) throws IOException {
		File srfFile = new File("/home/kevin/Simulators/catalogs/rundir2585_1myr/"
				+ "cybershake_rotation_inputs/cs_source_rup_files/684/323/684_323_event9458786.srf");
		List<SRF_PointData> srf = SRF_PointData.readSRF(srfFile);
		
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		XY_DataSet[] outlines = PoliticalBoundariesData.loadCAOutlines();
		PlotCurveCharacterstics caChar = new PlotCurveCharacterstics(PlotLineType.SOLID, 1f, Color.BLACK);
		
		MinMaxAveTracker latTrack = new MinMaxAveTracker();
		MinMaxAveTracker lonTrack = new MinMaxAveTracker();
		
		for (XY_DataSet outline : outlines) {
			funcs.add(outline);
			chars.add(caChar);
		}
		
		XY_DataSet pointXY = new DefaultXY_DataSet();
		
		for (SRF_PointData point : srf) {
			Location loc = point.getLocation();
			pointXY.set(loc.getLongitude(), loc.getLatitude());
			latTrack.addValue(loc.getLatitude());
			lonTrack.addValue(loc.getLongitude());
		}
		
		funcs.add(pointXY);
		chars.add(new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 3f, Color.BLUE));
		
		PlotSpec spec = new PlotSpec(funcs, chars, srfFile.getName(), "Longitude", "Latitude");
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(24);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(28);
		gp.setBackgroundColor(Color.WHITE);
		gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
		
		double maxSpan = latTrack.getMax() - latTrack.getMin();
		maxSpan = Math.max(maxSpan, lonTrack.getMax() - lonTrack.getMin());
		maxSpan *= 1.2;
		maxSpan = Math.max(maxSpan, 3);
		double halfSpan = 0.5*maxSpan;
		
		double centerLat = latTrack.getAverage();
		double centerLon = lonTrack.getAverage();
		
		Range xRange = new Range(centerLon - halfSpan, centerLon + halfSpan);
		Range yRange = new Range(centerLat - halfSpan, centerLat + halfSpan);
		
		gp.drawGraphPanel(spec, false, false, xRange, yRange);
		
		gp.getChartPanel().setSize(1000, 1000);
		gp.saveAsPNG("/tmp/srf_debug_plot.png");
	}

}
