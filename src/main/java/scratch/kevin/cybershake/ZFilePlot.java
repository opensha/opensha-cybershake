package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.util.binFile.BinaryMesh2DCalculator.DataType;
import org.jfree.data.Range;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.binFile.GeolocatedRectangularBinaryMesh2DCalculator;
import org.opensha.commons.util.cpt.CPT;

import com.google.common.base.Preconditions;

public class ZFilePlot {

	public static void main(String[] args) throws IOException, GMT_MapException {
		if (!(args.length == 8 || args.length == 10)) {
			System.err.println("USAGE: <min-lat> <min-lon> <nx> <ny> <grid-spacing> <max-z> <data-file> <label>"
					+ " [<compare-file> <compare-label>]");
			System.exit(2);
		}
		
		double minLat = Double.parseDouble(args[0]);
		double minLon = Double.parseDouble(args[1]);
		int nx = Integer.parseInt(args[2]);
		int ny = Integer.parseInt(args[3]);
		double gridSpacing = Double.parseDouble(args[4]);
		double maxZ = Double.parseDouble(args[5]);
		File dataFile = new File(args[6]);
		Preconditions.checkArgument(dataFile.exists(), "Data file doesn't exist: "+args[6]);
		String label = args[7];
		File compareFile = null;
		String compareLabel = "";
		if (args.length == 10) {
			compareFile = new File(args[8]);
			Preconditions.checkArgument(compareFile.exists(), "Data file doesn't exist: "+args[8]);
			compareLabel = args[9];
		}
		
		File outputDir = new File(System.getProperty("user.dir"));
		Preconditions.checkState(outputDir.exists() && outputDir.isDirectory());
		
		double plotSpacing = Math.max(gridSpacing, 0.02);
		
		GeolocatedRectangularBinaryMesh2DCalculator meshCalc =
				new GeolocatedRectangularBinaryMesh2DCalculator(DataType.FLOAT, nx, ny, minLat, minLon, true, true, gridSpacing);
		
		BasinDepthCompare.map_parallel = false;
		
		GriddedGeoDataSet xyz = build(meshCalc, dataFile, plotSpacing);
		
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(0d, maxZ);
		cpt.setNanColor(Color.GRAY);
//		cpt.setBelowMinColor(Color.GRAY);
//		cpt.setAboveMaxColor(cpt.getMaxColor());
		
		String prefix = label.replaceAll(" ", "_");
		
		BasinDepthCompare.plotMaps(outputDir, prefix, xyz, xyz.getRegion(), 0d, maxZ, label, cpt, false, false);
		
		if (compareFile != null) {
			GriddedGeoDataSet xyz2 = build(meshCalc, compareFile, plotSpacing);
			
			String prefix2 = compareLabel.replaceAll(" ", "_");
			BasinDepthCompare.plotMaps(outputDir, prefix2, xyz2, xyz2.getRegion(), 0d, maxZ, compareLabel, cpt, false, false);
			
			CPT ratioCPT = GMT_CPT_Files.GMT_POLAR.instance().rescale(-1d, 1d);
			ratioCPT.setNanColor(Color.GRAY);
			ratioCPT.setBelowMinColor(cpt.getMinColor());
			ratioCPT.setAboveMaxColor(cpt.getMaxColor());
			
			GriddedGeoDataSet ratio = new GriddedGeoDataSet(xyz.getRegion(), false);
			
			DefaultXY_DataSet scatter = new DefaultXY_DataSet();
			double minNonZero = Double.POSITIVE_INFINITY;
			double max = 0d;
			
			for (int i=0; i<ratio.size(); i++) {
				double v1 = xyz.get(i);
				double v2 = xyz2.get(i);
				
				if (Double.isNaN(v1) || Double.isNaN(v2)) {
					ratio.set(i, Double.NaN);
					continue;
				}
				
				if (v1 > 0 && v1 < minNonZero)
					minNonZero = v1;
				if (v2 > 0 && v2 < minNonZero)
					minNonZero = v2;
				max = Math.max(max, v1);
				max = Math.max(max, v2);
				
				scatter.set(v2, v1);
				ratio.set(i, Math.log10(v1/v2));
			}
			
			String ratioLabel = "Log10("+label+" / "+compareLabel+")";
			String ratioPrefix = prefix+"_vs_"+prefix2;
			
			BasinDepthCompare.plotMaps(outputDir, ratioPrefix+"_map", ratio, xyz.getRegion(),
					-1d, 1d, ratioLabel, ratioCPT, false, false);
			
			List<XY_DataSet> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			funcs.add(scatter);
			chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 2f, Color.BLACK));
			
			DefaultXY_DataSet oneToOne = new DefaultXY_DataSet();
			oneToOne.set(0d, 0d);
			oneToOne.set(minNonZero, minNonZero);
			oneToOne.set(max, max);
			
			funcs.add(oneToOne);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
			
			PlotSpec spec = new PlotSpec(funcs, chars, "Basin Depth Scatter Comparison", compareLabel, label);
			PlotPreferences plotPrefs = PlotPreferences.getDefault();
			plotPrefs.setTickLabelFontSize(18);
			plotPrefs.setAxisLabelFontSize(20);
			plotPrefs.setPlotLabelFontSize(21);
			plotPrefs.setBackgroundColor(Color.WHITE);
			
			HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
			
			Range range = new Range(0d, max);
			gp.drawGraphPanel(spec, false, false, range, range);
			gp.getChartPanel().setSize(800, 600);
			gp.saveAsPNG(new File(outputDir, ratioPrefix+"_scatter").getAbsolutePath()+".png");
			gp.saveAsPDF(new File(outputDir, ratioPrefix+"_scatter").getAbsolutePath()+".pdf");
		}
	}
	
	private static GriddedGeoDataSet build(GeolocatedRectangularBinaryMesh2DCalculator meshCalc, File dataFile, double spacing)
			throws IOException {
		Location minLoc = new Location(meshCalc.getMinLat(), meshCalc.getMinLon());
		Location maxLoc = new Location(meshCalc.getMaxLat(), meshCalc.getMaxLon());
		Region region = new Region(minLoc, maxLoc);
		GriddedRegion gridReg = new GriddedRegion(region, spacing, minLoc);
		GriddedGeoDataSet xyz = new GriddedGeoDataSet(gridReg, false);
		
		RandomAccessFile file = new RandomAccessFile(dataFile, "r");
		
		byte[] recordBuffer = new byte[4];
		ByteBuffer record = ByteBuffer.wrap(recordBuffer);
		record.order(ByteOrder.LITTLE_ENDIAN);
		FloatBuffer floatBuff = record.asFloatBuffer();
		
		for (int i=0; i<xyz.size(); i++) {
			Location loc = xyz.getLocation(i);
			long filePos = meshCalc.calcClosestLocationFileIndex(loc);
			if (filePos < 0 || filePos > meshCalc.getMaxFilePos()) {
				xyz.set(i, Double.NaN);
			} else {
				file.seek(filePos);
				file.read(recordBuffer);
				
				// this is in meters
				double val = floatBuff.get(0);
				if (val < 0)
					val = Double.NaN;
				xyz.set(i, val/1000d);
			}
		}
		
		file.close();
		
		return xyz;
	}

}
