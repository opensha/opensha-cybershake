package org.opensha.sha.cybershake.calc.mcer.data;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.ui.TextAnchor;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.cybershake.calc.mcer.GriddedSpectrumInterpolator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class GriddedSpectrumValidateAgainstOrig {

	public static void main(String[] args) throws Exception {
//		File dataFile = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/mcer_spectrum_interp_linear.bin");
//		double spacing = 0.02;
//		double spacing = 0.01;
//		double spacing = 0.005;
//		double spacing = 0.001;
		double spacing = 0.002;
		File dataFile = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/interp_tests/mcer_spectrum_"+(float)spacing+".bin");
		BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dataFile.getAbsolutePath());
		Map<Location, ArbitrarilyDiscretizedFunc> curves = reader.getCurveMap();
		
		GriddedSpectrumInterpolator interp = new GriddedSpectrumInterpolator(curves, spacing);
		
		int periodIndex = 0;
		ArbDiscrGeoDataSet scatter = ArbDiscrGeoDataSet.loadXYZFile("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/2s/sa/"
				+ "combined_mcer_sa_map_data_scatter.txt", true);
		
		MinMaxAveTracker interpTrack = new MinMaxAveTracker();
		MinMaxAveTracker closestTrack = new MinMaxAveTracker();
		MinMaxAveTracker interpPercentTrack = new MinMaxAveTracker();
		MinMaxAveTracker closestPercentTrack = new MinMaxAveTracker();
		Region reg = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		
		HashSet<Location> prevLocs = new HashSet<Location>();
		
		DefaultXY_DataSet diffXYZ = new DefaultXY_DataSet();
		DefaultXY_DataSet diffXYZtoClosest = new DefaultXY_DataSet();
		
		Location maxResidualLoc = null;
		double max = 0d;
		
		for (Location loc : scatter.getLocationList()) {
			if (!reg.contains(loc))
				continue;
			double val = scatter.get(loc);
			double interpVal = interp.getInterpolated(loc).getY(periodIndex);
			double closestVal = interp.getClosest(loc).getY(periodIndex);
			System.out.println((float)val+"\t"+(float)interpVal+"\t"+(float)closestVal);
			interpTrack.addValue(Math.abs(val - interpVal));
			closestTrack.addValue(Math.abs(val - closestVal));
			double pDiff = DataUtils.getPercentDiff(val, interpVal);
			if (pDiff > max) {
				max = pDiff;
				maxResidualLoc = loc;
			}
			interpPercentTrack.addValue(pDiff);
			closestPercentTrack.addValue(DataUtils.getPercentDiff(val, closestVal));
			Preconditions.checkState(!prevLocs.contains(loc));
			prevLocs.add(loc);
			Location gridLoc = interp.getClosestGridLoc(loc);
			double dist = LocationUtils.horzDistanceFast(loc, gridLoc);
			diffXYZ.set(dist, pDiff);
			double minDistToClosest = Double.POSITIVE_INFINITY;
			for (Location loc2 : scatter.getLocationList()) {
				if (loc == loc2)
					continue;
				minDistToClosest = Math.min(minDistToClosest, LocationUtils.horzDistanceFast(loc, loc2));
			}
			diffXYZtoClosest.set(minDistToClosest, pDiff);
		}
		
		System.out.println("Interpolated: "+interpTrack);
		System.out.println("Interpolated %: "+interpPercentTrack);
		System.out.println("closest: "+closestTrack);
		System.out.println("closest %: "+closestPercentTrack);
		
		System.out.println("Max of "+max+" at: "+maxResidualLoc);
		
		List<XY_DataSet> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		funcs.add(diffXYZ);
		chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, Color.BLACK));
		
		PlotSpec spec = new PlotSpec(funcs, chars, (float)spacing+" Residuals", "Distance to Grid Point (km)", "% Difference");
		
		List<XYTextAnnotation> anns = Lists.newArrayList();
		float mean = (float)interpPercentTrack.getAverage();
		XYTextAnnotation meanAnn = new XYTextAnnotation(" Mean: "+mean+" %", 0, max*0.95);
		meanAnn.setTextAnchor(TextAnchor.TOP_LEFT);
		meanAnn.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 20));
		anns.add(meanAnn);
		XYTextAnnotation maxAnn = new XYTextAnnotation(" Max: "+(float)max+" %", 0, max*0.85);
		maxAnn.setTextAnchor(TextAnchor.TOP_LEFT);
		maxAnn.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 20));
		anns.add(maxAnn);
		spec.setPlotAnnotations(anns);
		
		GraphWindow gw = new GraphWindow(spec);
		gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
		
		File outDir = dataFile.getParentFile();
		gw.saveAsPNG(new File(outDir, "residuals_"+(float)spacing+".png").getAbsolutePath());
		
//		funcs = Lists.newArrayList();
//		chars = Lists.newArrayList();
//		
//		funcs.add(diffXYZtoClosest);
//		chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, Color.BLACK));
//		
//		spec = new PlotSpec(funcs, chars, (float)spacing+" Residuals", "Distance to Closest Other CS Site (km)", "% Difference");
//		
//		gw = new GraphWindow(spec);
//		gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
//		
//		gw.saveAsPNG(new File(outDir, "residuals_"+(float)spacing+"_to_cs.png").getAbsolutePath());
	}

}
