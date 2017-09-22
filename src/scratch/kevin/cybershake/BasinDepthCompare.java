package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jfree.data.Range;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.CVM4i26BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
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
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.GMT_MapGenerator;
import org.opensha.commons.mapping.gmt.elements.CoastAttributes;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.cpt.CPT;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class BasinDepthCompare {
	
	private static final boolean map_parallel = true;
	
	public static void main(String[] args) throws IOException, GMT_MapException {
		File outputDir = new File("/home/kevin/CyberShake/basin");
		
		boolean generateKML = false;
		
//		String type = CVM4i26BasinDepth.TYPE_DEPTH_TO_2_5;
////		File dataDir = new File("/home/kevin/CyberShake/basin");
////		String outFilePrefix = "cvm426_z25";
////		String mapLabelPrefix = "CVM-S4.26 Z2.5";
////		File newFile = new File(dataDir, "cvms426_z25_map_full.bin");
////		String outFilePrefix = "cvm426m01_z25";
////		String mapLabelPrefix = "CVM-S4.26-01 Z2.5";
////		File newFile = new File(dataDir, "cvms426m01_z25_map_full.bin");
//		File dataDir = new File("/home/kevin/CyberShake/basin");
//		String outFilePrefix = "cca06_z25";
//		String mapLabelPrefix = "CCA-06 Z2.5";
//		File newFile = new File(dataDir, "cca06_z25_map_full.bin");
//		File oldFile = new File(dataDir, "cca_depth_2.5.bin");
//		double dataMin = 0d;
//		double dataMax = 10d;
		
		String type = CVM4i26BasinDepth.TYPE_DEPTH_TO_1_0;
//		File dataDir = new File("/home/kevin/CyberShake/basin");
//		String outFilePrefix = "cvm426_z10";
//		String mapLabelPrefix = "CVM-S4.26 Z1.0";
//		File newFile = new File(dataDir, "cvms426_z10_map_full.bin");
//		String outFilePrefix = "cvm426m01_z10";
//		String mapLabelPrefix = "CVM-S4.26-01 Z1.0";
//		File newFile = new File(dataDir, "cvms426m01_z10_map_full.bin");
//		File oldFile = new File(dataDir, "depth_1.0.bin");
		File dataDir = new File("/home/kevin/CyberShake/basin");
		String outFilePrefix = "cca06_z10";
		String mapLabelPrefix = "CCA-06 Z1.0";
		File newFile = new File(dataDir, "cca06_z10_map_full.bin");
		File oldFile = new File(dataDir, "cca_depth_1.0.bin");
		double dataMin = 0d;
		double dataMax = 2d;
		
		double fullDiscr = 0.01;
		double zoomDiscr = 0.001;
		
//		String willsPrefix = "wills2015_vs30";
//		SiteData<Double> oldFetch = new CVM4i26BasinDepth(type, oldFile);
//		SiteData<Double> newFetch = new CVM4i26BasinDepth(type, newFile);
		
		String willsPrefix = "wills2015_vs30_cca";
		SiteData<Double> oldFetch = new CVM_CCAi6BasinDepth(type, oldFile);
		SiteData<Double> newFetch = new CVM_CCAi6BasinDepth(type, newFile);
		
		WillsMap2015 vs30Fetch = new WillsMap2015();
		
		System.out.println(oldFetch.getValue(new Location(32.5, -120)));
		System.out.println(newFetch.getValue(new Location(32.5, -120)));
		
		Region fullReg = oldFetch.getApplicableRegion();
		GriddedRegion fullGridReg = new GriddedRegion(fullReg, fullDiscr, null);
		
		Region zoomReg = null;
		GriddedRegion zoomGridReg = null;
		if (oldFetch instanceof CVM4i26BasinDepth)
			zoomReg = new Region(new Location(34.3, -117.5), new Location(33.9, -116.9));
		else if (oldFetch instanceof CVM_CCAi6BasinDepth)
			zoomReg = new Region(new Location(34.8, -118.4), new Location(33.8, -121));
		if (zoomReg != null)
			zoomGridReg = new GriddedRegion(zoomReg, zoomDiscr, null);
		
		System.out.println("Fetching full, old");
		GriddedGeoDataSet oldFullData = getGeo(fullGridReg, oldFetch.getValues(fullGridReg.getNodeList()));
		System.out.println("Fetching full, new");
		GriddedGeoDataSet newFullData = getGeo(fullGridReg, newFetch.getValues(fullGridReg.getNodeList()));
		System.out.println("Fetching Vs30 full");
		GriddedGeoDataSet vs30FullData = getGeo(fullGridReg, vs30Fetch.getValues(fullGridReg.getNodeList()));
		GriddedGeoDataSet oldZoomData = null, newZoomData = null, vs30ZoomData = null;
		if (zoomReg != null) {
			System.out.println("Fetching zoom, old");
			oldZoomData = getGeo(zoomGridReg, oldFetch.getValues(zoomGridReg.getNodeList()));
			System.out.println("Fetching zoom, new");
			newZoomData = getGeo(zoomGridReg, newFetch.getValues(zoomGridReg.getNodeList()));
			System.out.println("Fetching Vs30 zoom");
			vs30ZoomData = getGeo(zoomGridReg, vs30Fetch.getValues(zoomGridReg.getNodeList()));
		}
		
		CPT dataCPT = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(dataMin, dataMax);
		dataCPT.setBelowMinColor(dataCPT.getMinColor());
		dataCPT.setAboveMaxColor(dataCPT.getMaxColor());
		System.out.println("Mapping full, old");
		plotMaps(outputDir, outFilePrefix+"_orig_full", oldFullData, fullReg, dataMin, dataMax,
				mapLabelPrefix+", Origial", dataCPT, false, generateKML);
		System.out.println("Mapping full, new");
		plotMaps(outputDir, outFilePrefix+"_new_full", newFullData, fullReg, dataMin, dataMax,
				mapLabelPrefix+", New", dataCPT, false, generateKML);
		CPT vs30CPT = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(150, 800);
		System.out.println("Mapping Vs30 full");
		plotMaps(outputDir, willsPrefix+"_full", vs30FullData, fullReg, 150d, 800d,
				"Wills 2015 Vs30", vs30CPT, false, generateKML);
		System.out.println("Mapping Vs30 zoom");
		if (zoomReg != null) {
			System.out.println("Mapping zoom, old");
			plotMaps(outputDir, outFilePrefix+"_orig_zoom", oldZoomData, zoomReg, dataMin, dataMax,
					mapLabelPrefix+", Origial", dataCPT, false, false);
			System.out.println("Mapping zoom, new");
			plotMaps(outputDir, outFilePrefix+"_new_zoom", newZoomData, zoomReg, dataMin, dataMax,
					mapLabelPrefix+", New", dataCPT, false, false);
			plotMaps(outputDir, willsPrefix+"_zoom", vs30ZoomData, zoomReg, 150d, 800d,
					"Wills 2015 Vs30", vs30CPT, false, generateKML);
		}
		
		// now scatter
		DefaultXY_DataSet scatter = new DefaultXY_DataSet();
		double minNonZero = Double.POSITIVE_INFINITY;
		double max = 0d;
		for (int i=0; i<oldFullData.size(); i++) {
			double oldVal = oldFullData.get(i);
			double newVal = newFullData.get(i);
			
			if (Double.isNaN(oldVal) || Double.isNaN(newVal) || (oldFetch instanceof CVM_CCAi6BasinDepth && oldVal == 15d))
				continue;
			
			if (oldVal > 0 && oldVal < minNonZero)
				minNonZero = oldVal;
			if (newVal > 0 && newVal < minNonZero)
				minNonZero = oldVal;
			max = Math.max(max, oldVal);
			max = Math.max(max, newVal);
			
			scatter.set(oldVal, newVal);
		}
		
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
		
		PlotSpec spec = new PlotSpec(funcs, chars, mapLabelPrefix, "Original Values", "New Values");
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(18);
		plotPrefs.setAxisLabelFontSize(20);
		plotPrefs.setPlotLabelFontSize(21);
		plotPrefs.setBackgroundColor(Color.WHITE);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
		
		Range range = new Range(0d, max);
		gp.drawGraphPanel(spec, false, false, range, range);
		gp.getChartPanel().setSize(800, 600);
		gp.saveAsPNG(new File(outputDir, outFilePrefix+"_scatter").getAbsolutePath()+".png");
		gp.saveAsPDF(new File(outputDir, outFilePrefix+"_scatter").getAbsolutePath()+".pdf");
		
		waitOnMaps();
	}
	
	private static GriddedGeoDataSet getGeo(GriddedRegion gridReg, List<Double> vals) {
		Preconditions.checkState(gridReg.getNodeCount() == vals.size());
		
		GriddedGeoDataSet xyz = new GriddedGeoDataSet(gridReg, false);
		for (int i=0; i<gridReg.getNodeCount(); i++)
			xyz.set(i, vals.get(i));
		
		return xyz;
	}
	
	private static void plotMaps(File outputDir, String prefix, GriddedGeoDataSet data, Region region,
			Double customMin, Double customMax, String label, CPT cpt, boolean rescaleCPT, boolean googleEarth)
					throws GMT_MapException, IOException {
		
		System.out.println("Creating map instance...");
		GMT_Map map = new GMT_Map(region, data, data.getRegion().getLatSpacing(), cpt);
		
		map.setCustomLabel(label);
//		map.setTopoResolution(TopographicSlopeFile.US_SIX);
		map.setTopoResolution(null);
		map.setUseGMTSmoothing(false);
		map.setCoast(new CoastAttributes(Color.GRAY, 1));
		map.setLogPlot(false);
		map.setDpi(300);
		map.setCustomScaleMin(customMin);
		map.setCustomScaleMax(customMax);
		map.setRescaleCPT(rescaleCPT);
		map.setBlackBackground(false);
		
		if (googleEarth) {
			map.setGenerateKML(true);
			GMT_MapGenerator.JAVA_PATH = "java";
			GMT_MapGenerator.JAVA_CLASSPATH = "/home/kevin/workspace/opensha-dev/build/libs/opensha-dev-all.jar";
		}
		
		System.out.println("Making map...");
		FaultBasedMapGen.LOCAL_MAPGEN = true;
		
		Runnable run = new Runnable() {
			
			@Override
			public void run() {
				try {
					FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		
		if (map_parallel) {
			if (mapExec == null) {
				mapExec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				mapFutures = new ArrayList<>();
			}
			mapFutures.add(mapExec.submit(run));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			run.run();
		}
	}
	
	private static void waitOnMaps() {
		if (mapFutures == null)
			return;
		System.out.println("Waiting on "+mapFutures.size()+" maps...");
		for (Future<?> future : mapFutures) {
			try {
				future.get();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.print("DONE");
		mapExec.shutdown();
	}
	
	private static ExecutorService mapExec;
	private static List<Future<?>> mapFutures;

}
