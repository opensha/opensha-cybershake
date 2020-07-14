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
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.CVM4i26BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.data.siteData.impl.ThompsonVs30_2018;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSetMath;
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
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;

public class Vs30ModelCompare {
	
	static boolean map_parallel = true;
	
	public static void main(String[] args) throws IOException, GMT_MapException {
		File mainOutputDir = new File("/home/kevin/git/misc-research/vs30_model_compare");
		Preconditions.checkState(mainOutputDir.exists() || mainOutputDir.mkdir());
		
		FaultBasedMapGen.LOCAL_MAPGEN = true;
		boolean generateKML = false;
		
		Region zoomRegion = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		
		File outputDir = new File(mainOutputDir, "thompson_2018_vs_wills_2015");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		String title = "Vs30";
		double dataMin = 150;
		double dataMax = 900;
		double maxDiff = 150d;
		
		SiteData<Double> prov1 = new ThompsonVs30_2018();
		String name1 = prov1.getName();
		String prefix1 = "thompson_2018";
		
		SiteData<Double> prov2 = new WillsMap2015();
		String name2 = prov2.getName();
		String prefix2 = "wills_2015";
		
		double fullDiscr = 0.01;
//		double zoomDiscr = dataProvs.get(0).getResolution();
		double zoomDiscr = 0.001;
		
		Region fullReg = prov1.getApplicableRegion();
		GriddedRegion fullGridReg = new GriddedRegion(fullReg, fullDiscr, null);
		GriddedRegion zoomGridReg = new GriddedRegion(zoomRegion, zoomDiscr, null);
		
		System.out.println("Fetching datas");
		GriddedGeoDataSet fullData1 = getGeo(fullGridReg, prov1.getValues(fullGridReg.getNodeList()));
		GriddedGeoDataSet fullData2 = getGeo(fullGridReg, prov2.getValues(fullGridReg.getNodeList()));
		GeoDataSet fullDiff = GeoDataSetMath.subtract(fullData1, fullData2);
		GriddedGeoDataSet zoomData1 = getGeo(zoomGridReg, prov1.getValues(zoomGridReg.getNodeList()));
		GriddedGeoDataSet zoomData2 = getGeo(zoomGridReg, prov2.getValues(zoomGridReg.getNodeList()));
		GeoDataSet zoomDiff = GeoDataSetMath.subtract(zoomData1, zoomData2);

		CPT dataCPT = GMT_CPT_Files.RAINBOW_UNIFORM.instance().rescale(dataMin, dataMax);
		dataCPT.setBelowMinColor(dataCPT.getMinColor());
		dataCPT.setAboveMaxColor(dataCPT.getMaxColor());
		dataCPT.setNanColor(Color.GRAY);
		
		CPT diffCPT = GMT_CPT_Files.GMT_POLAR.instance().rescale(-maxDiff, maxDiff);
		diffCPT.setBelowMinColor(dataCPT.getMinColor());
		diffCPT.setAboveMaxColor(dataCPT.getMaxColor());
		diffCPT.setNanColor(Color.GRAY);
		
		List<String> lines = new ArrayList<>();
		lines.add("# "+title);
		lines.add("");
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		lines.add("## Maps");
		lines.add(topLink); lines.add("");
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine(name1, name2, "Difference");
		
		table.initNewLine();
		System.out.println("Mapping full, "+name1);
		plotMaps(resourcesDir, prefix1+"_full", fullData1, fullReg, fullDiscr, dataMin, dataMax,
				title+", "+name1, dataCPT, false, generateKML);
		table.addColumn("![Map]("+resourcesDir.getName()+"/"+prefix1+"_full.png)");
		System.out.println("Mapping full, "+name2);
		plotMaps(resourcesDir, prefix2+"_full", fullData2, fullReg, fullDiscr, dataMin, dataMax,
				title+", "+name2, dataCPT, false, generateKML);
		table.addColumn("![Map]("+resourcesDir.getName()+"/"+prefix2+"_full.png)");
		System.out.println("Mapping full diff");
		plotMaps(resourcesDir, "full_diff", fullDiff, fullReg, fullDiscr, -maxDiff, maxDiff,
				name1+" - "+name2, diffCPT, false, generateKML);
		table.addColumn("![Map]("+resourcesDir.getName()+"/full_diff.png)");
		table.finalizeLine();
		
		table.initNewLine();
		System.out.println("Mapping zoom, "+name1);
		plotMaps(resourcesDir, prefix1+"_zoom", zoomData1, zoomRegion, zoomDiscr, dataMin, dataMax,
				title+", "+name1, dataCPT, false, generateKML);
		table.addColumn("![Map]("+resourcesDir.getName()+"/"+prefix1+"_zoom.png)");
		System.out.println("Mapping zoom, "+name2);
		plotMaps(resourcesDir, prefix2+"_zoom", zoomData2, zoomRegion, zoomDiscr, dataMin, dataMax,
				title+", "+name2, dataCPT, false, generateKML);
		table.addColumn("![Map]("+resourcesDir.getName()+"/"+prefix2+"_zoom.png)");
		System.out.println("Mapping zoom diff");
		plotMaps(resourcesDir, "zoom_diff", zoomDiff, zoomRegion, zoomDiscr, -maxDiff, maxDiff,
				name1+" - "+name2, diffCPT, false, generateKML);
		table.addColumn("![Map]("+resourcesDir.getName()+"/zoom_diff.png)");
		table.finalizeLine();
		
		lines.addAll(table.build());
		
		lines.add("## Scatter Comparison");
		lines.add(topLink); lines.add("");
		
		// now scatter
		DefaultXY_DataSet scatter = new DefaultXY_DataSet();
		double minNonZero = Double.POSITIVE_INFINITY;
		double max = 0d;
		for (int k=0; k<fullData1.size(); k++) {
			double v1 = fullData1.get(k);
			double v2 = fullData2.get(k);
			
			if (Double.isNaN(v1) || Double.isNaN(v2))
				continue;
			
			if (v1 > 0 && v1 < minNonZero)
				minNonZero = v1;
			if (v2 > 0 && v2 < minNonZero)
				minNonZero = v2;
			max = Math.max(max, v1);
			max = Math.max(max, v2);
			
			scatter.set(v1, v2);
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
		
		PlotSpec spec = new PlotSpec(funcs, chars, title+" Scatter", name1, name2);
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(18);
		plotPrefs.setAxisLabelFontSize(20);
		plotPrefs.setPlotLabelFontSize(21);
		plotPrefs.setBackgroundColor(Color.WHITE);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
		
		Range range = new Range(0d, max);
		gp.drawGraphPanel(spec, false, false, range, range);
		gp.getChartPanel().setSize(800, 600);
		
		String prefix = prefix1+"_vs_"+prefix2+"_scatter";
		gp.saveAsPNG(new File(resourcesDir, prefix).getAbsolutePath()+".png");
		gp.saveAsPDF(new File(resourcesDir, prefix).getAbsolutePath()+".pdf");
		lines.add("![Scatter]("+resourcesDir.getName()+"/"+prefix+".png)");
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2));
		lines.add(tocIndex, "## Table Of Contents");
		
		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
		
		waitOnMaps();
	}
	
	private static GriddedGeoDataSet getGeo(GriddedRegion gridReg, List<Double> vals) {
		Preconditions.checkState(gridReg.getNodeCount() == vals.size());
		
		GriddedGeoDataSet xyz = new GriddedGeoDataSet(gridReg, false);
		for (int i=0; i<gridReg.getNodeCount(); i++)
			xyz.set(i, vals.get(i));
		
		return xyz;
	}
	
	static void plotMaps(File outputDir, String prefix, GeoDataSet data, Region region, double spacing,
			Double customMin, Double customMax, String label, CPT cpt, boolean rescaleCPT, boolean googleEarth)
					throws GMT_MapException, IOException {
		
		System.out.println("Creating map instance...");
		GMT_Map map = new GMT_Map(region, data, spacing, cpt);
		
		map.setCustomLabel(label);
//		map.setTopoResolution(TopographicSlopeFile.US_SIX);
		map.setTopoResolution(null);
		map.setUseGMTSmoothing(false);
		map.setCoast(new CoastAttributes(Color.GRAY, 1));
		map.setLogPlot(false);
		map.setDpi(72);
		map.setCustomScaleMin(customMin);
		map.setCustomScaleMax(customMax);
		map.setRescaleCPT(rescaleCPT);
		map.setBlackBackground(false);
		map.setJPGFileName(null);
		
		if (googleEarth) {
			map.setGenerateKML(true);
			GMT_MapGenerator.JAVA_PATH = "java";
			GMT_MapGenerator.JAVA_CLASSPATH = "/home/kevin/workspace/opensha-dev/build/libs/opensha-dev-all.jar";
		}
		
		System.out.println("Making map...");
		
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
