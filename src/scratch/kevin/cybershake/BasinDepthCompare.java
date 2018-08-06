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
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;

public class BasinDepthCompare {
	
	static boolean map_parallel = true;
	
	public static void main(String[] args) throws IOException, GMT_MapException {
		File mainOutputDir = new File("/home/kevin/git/misc-research/basin_depth_compare");
		File dataDir = new File("/home/kevin/CyberShake/basin");
		
		FaultBasedMapGen.LOCAL_MAPGEN = true;
		boolean generateKML = true;
		
		List<SiteData<Double>> dataProvs = new ArrayList<>();
		List<String> modelNames = new ArrayList<>();
		List<String> modelPrefixes = new ArrayList<>();
		
//		String type = CVM4i26BasinDepth.TYPE_DEPTH_TO_2_5;
//		
//		File outputDir = new File(mainOutputDir, "cca_3d_z25");
//		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
//		File resourcesDir = new File(outputDir, "resources");
//		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
//		
//		String title = "CCA-06 Z2.5";
//		double dataMin = 0d;
//		double dataMax = 10d;
//		
//		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca06_z25_map_full.bin")));
//		modelNames.add("CCA UCVM-Py, Scott");
//		modelPrefixes.add("cca_25_ucvm_py_scott");
//		
//		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca_z2.5_nGTL.firstOrSecond")));
//		modelNames.add("CCA UCVMC, First Or Second Crossing");
//		modelPrefixes.add("cca_25_ucvmc_first_or_second");
//		
//		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca_z2.5_nGTL.last")));
//		modelNames.add("CCA UCVMC, Last Crossing");
//		modelPrefixes.add("cca_25_ucvmc_last");
//		
//		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca_z2.5_nGTL.first")));
//		modelNames.add("CCA UCVMC, First Crossing");
//		modelPrefixes.add("cca_25_ucvmc_first");
		
		String type = CVM4i26BasinDepth.TYPE_DEPTH_TO_1_0;
		
		File outputDir = new File(mainOutputDir, "cca_3d_z10");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		String title = "CCA-06 Z1.0";
		double dataMin = 0d;
		double dataMax = 2d;
		
		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca06_z10_map_full.bin")));
		modelNames.add("CCA UCVM-Py, Scott");
		modelPrefixes.add("cca_10_ucvm_py_scott");
		
		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca_z1.0_nGTL.firstOrSecond")));
		modelNames.add("CCA UCVMC, First Or Second Crossing");
		modelPrefixes.add("cca_10_ucvmc_first_or_second");
		
		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca_z1.0_nGTL.last")));
		modelNames.add("CCA UCVMC, Last Crossing");
		modelPrefixes.add("cca_10_ucvmc_last");
		
		dataProvs.add(new CVM_CCAi6BasinDepth(type, new File(dataDir, "cca_z1.0_nGTL.first")));
		modelNames.add("CCA UCVMC, First Crossing");
		modelPrefixes.add("cca_10_ucvmc_first");
		
//		String type = CVM4i26BasinDepth.TYPE_DEPTH_TO_1_0;
////		File dataDir = new File("/home/kevin/CyberShake/basin");
////		String outFilePrefix = "cvm426_z10";
////		String mapLabelPrefix = "CVM-S4.26 Z1.0";
////		File newFile = new File(dataDir, "cvms426_z10_map_full.bin");
////		String outFilePrefix = "cvm426m01_z10";
////		String mapLabelPrefix = "CVM-S4.26-01 Z1.0";
////		File newFile = new File(dataDir, "cvms426m01_z10_map_full.bin");
////		File oldFile = new File(dataDir, "depth_1.0.bin");
//		File dataDir = new File("/home/kevin/CyberShake/basin");
//		String outFilePrefix = "cca06_z10_final";
//		String mapLabelPrefix = "CCA-06 Z1.0";
//		File newFile = new File(dataDir, "cca_z1_final.firstOrSecond");
//		File oldFile = new File(dataDir, "cca06_z10_map_full.bin");
////		File newFile = new File(dataDir, "cca06_z10_map_full.bin");
////		File oldFile = new File(dataDir, "cca_depth_1.0.bin");
//		double dataMin = 0d;
//		double dataMax = 2d;
		
		double fullDiscr = 0.005;
		double zoomDiscr = dataProvs.get(0).getResolution();
		
//		String willsPrefix = "wills2015_vs30_cca";
		
		WillsMap2015 vs30Fetch = new WillsMap2015();
		
		Region fullReg = dataProvs.get(0).getApplicableRegion();
		GriddedRegion fullGridReg = new GriddedRegion(fullReg, fullDiscr, null);
		
		List<Region> zoomRegs = new ArrayList<>();
		if (dataProvs.get(0) instanceof CVM4i26BasinDepth) {
			zoomRegs.add(new Region(new Location(34.3, -117.5), new Location(33.9, -116.9)));
		} else if (dataProvs.get(0) instanceof CVM_CCAi6BasinDepth) {
			zoomRegs.add(new Region(new Location(34.8, -118.4), new Location(33.8, -121)));
			zoomRegs.add(new Region(new Location(34.65, -118.6), new Location(34.35, -119.5)));
		}
		List<GriddedRegion> zoomGridRegs = new ArrayList<>();
		for (Region reg : zoomRegs)
			zoomGridRegs.add(new GriddedRegion(reg, zoomDiscr, null));
		
		System.out.println("Fetching full datas");
		List<GriddedGeoDataSet> fullDatas = new ArrayList<>();
		List<List<GriddedGeoDataSet>> zoomDatas = new ArrayList<>();
		for (SiteData<Double> prov : dataProvs) {
			fullDatas.add(getGeo(fullGridReg, prov.getValues(fullGridReg.getNodeList())));
			List<GriddedGeoDataSet> myZoomDatas = new ArrayList<>();
			zoomDatas.add(myZoomDatas);
			for (GriddedRegion gridReg : zoomGridRegs)
				myZoomDatas.add(getGeo(gridReg, prov.getValues(gridReg.getNodeList())));
		}
		
		System.out.println("Fetching Vs30");
		GriddedGeoDataSet vs30FullData = getGeo(fullGridReg, vs30Fetch.getValues(fullGridReg.getNodeList()));
		List<GriddedGeoDataSet> vs30ZoomDatas = new ArrayList<>();
		for (GriddedRegion gridReg : zoomGridRegs)
			vs30ZoomDatas.add(getGeo(gridReg, vs30Fetch.getValues(gridReg.getNodeList())));
		
		CPT dataCPT = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(dataMin, dataMax);
		dataCPT.setBelowMinColor(dataCPT.getMinColor());
		dataCPT.setAboveMaxColor(dataCPT.getMaxColor());
		dataCPT.setNanColor(Color.GRAY);
		
		List<String> lines = new ArrayList<>();
		lines.add("# "+title);
		lines.add("");
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		
		lines.add("## Full Maps");
		lines.add(topLink); lines.add("");
		
		for (int i=0; i<dataProvs.size(); i++) {
			String name = modelNames.get(i);
			String prefix = modelPrefixes.get(i);
			System.out.println("Mapping full, "+name);
			plotMaps(resourcesDir, prefix+"_full", fullDatas.get(i), fullReg, dataMin, dataMax,
					title+", "+name, dataCPT, false, generateKML);
			
			table.initNewLine();
			table.addColumn("**"+name+"**");
			table.addColumn("![Map]("+resourcesDir.getName()+"/"+prefix+"_full.png)");
			table.finalizeLine();
		}
		lines.addAll(table.build());
		
		lines.add("## Zoomed Maps");
		lines.add(topLink); lines.add("");
		
		for (int z=0; z<zoomGridRegs.size(); z++) {
			
			if (zoomGridRegs.size() > 0) {
				lines.add("### Zoom "+(z+1));
				lines.add(topLink); lines.add("");
			}
			
			table = MarkdownUtils.tableBuilder();
			
			for (int i=0; i<dataProvs.size(); i++) {
				String name = modelNames.get(i);
				String prefix = modelPrefixes.get(i);
				System.out.println("Mapping full, "+name);
				plotMaps(resourcesDir, prefix+"_zoom_"+z, zoomDatas.get(i).get(z), zoomRegs.get(z), dataMin, dataMax,
						title+", "+name, dataCPT, false, generateKML);
				
				table.initNewLine();
				table.addColumn("**"+name+"**");
				table.addColumn("![Map]("+resourcesDir.getName()+"/"+prefix+"_zoom_"+z+".png)");
				table.finalizeLine();
			}
			lines.addAll(table.build());
		}
		
		if (dataProvs.size() > 1) {
			lines.add("## Scatter Comparisons");
			lines.add(topLink); lines.add("");
			
			for (int i=0; i<dataProvs.size(); i++) {
				String name1 = modelNames.get(i);
				String prefix1 = modelPrefixes.get(i);
				GriddedGeoDataSet data1 = fullDatas.get(i);
				for (int j=i+1; j<dataProvs.size(); j++) {
					String name2 = modelNames.get(j);
					String prefix2 = modelPrefixes.get(j);
					GriddedGeoDataSet data2 = fullDatas.get(j);
					
					// now scatter
					DefaultXY_DataSet scatter = new DefaultXY_DataSet();
					double minNonZero = Double.POSITIVE_INFINITY;
					double max = 0d;
					for (int k=0; k<data1.size(); k++) {
						double v1 = data1.get(k);
						double v2 = data2.get(k);
						
						if (Double.isNaN(v1) || Double.isNaN(v2)
								|| (dataProvs.get(i) instanceof CVM_CCAi6BasinDepth && v1 == 15d)
								|| (dataProvs.get(j) instanceof CVM_CCAi6BasinDepth && v2 == 15d))
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
					
					lines.add("### "+name1+" vs "+name2);
					lines.add(topLink); lines.add("");
					
					String prefix = prefix1+"_vs_"+prefix2+"_scatter";
					gp.saveAsPNG(new File(resourcesDir, prefix).getAbsolutePath()+".png");
					gp.saveAsPDF(new File(resourcesDir, prefix).getAbsolutePath()+".pdf");
					lines.add("![Scatter]("+resourcesDir.getName()+"/"+prefix+".png)");
				}
			}
		}
		
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
	
	static void plotMaps(File outputDir, String prefix, GriddedGeoDataSet data, Region region,
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
