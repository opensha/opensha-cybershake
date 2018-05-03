package org.opensha.sha.cybershake.constants;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.dom4j.DocumentException;
import org.jfree.data.Range;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.ComparablePairing;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileNameComparator;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.UCERF2_AleatoryMagVarRemovalMod;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.gui.util.ERFSaver;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.magdist.GutenbergRichterMagFreqDist;
import org.opensha.sha.magdist.IncrementalMagFreqDist;
import org.opensha.sha.simulators.SimulatorElement;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.primitives.Doubles;

import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare;
import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.Vs30_Source;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.plots.AbstractPlot;
import scratch.kevin.simulators.plots.MFDPlot;
import scratch.kevin.simulators.plots.MagAreaScalingPlot;
import scratch.kevin.simulators.plots.RuptureVelocityPlot;
import scratch.kevin.util.MarkdownUtils;
import scratch.kevin.util.MarkdownUtils.TableBuilder;

public enum CyberShakeStudy {
	
	STUDY_15_4(cal(2015, 4), 57, "Study 15.4", "study_15_4",
			"Los Angeles region with CVM-S4.26 Velocity Model, 1hz", 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME) {
		@Override
		AbstractERF buildERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
	},
	STUDY_17_3_1D(cal(2017, 3), 80, "Study 17.3 1-D",
			"study_17_3_1d", "Central California with CCA-1D Velocity Model, 1hz", 9,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		AbstractERF buildERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
	},
	STUDY_17_3_3D(cal(2017, 3), 81, "Study 17.3 3-D",
			"study_17_3_3d", "Central California with CCA-06 Velocity Model, 1hz", 10,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		AbstractERF buildERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
	},
	STUDY_18_4_RSQSIM_PROTOTYPE_2457(cal(2018, 4), 82, "RSQSim 2457",
			"study_18_4_rsqsim_prototype_2457", "RSQSim prototype with catalog 2457", 5,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		AbstractERF buildERF() {
			return getRSQSimERF("rundir2457");
		}
	},
	STUDY_18_4_RSQSIM_2585(cal(2018, 4), 83, "RSQSim 2585",
			"study_18_4_rsqsim_2585", "RSQSim prototype with catalog 2585 (1myr)", 5,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		AbstractERF buildERF() {
			return getRSQSimERF("rundir2585_1myr");
		}
	};
	
	private static AbstractERF getRSQSimERF(String catalogDirName) {
		File catsDir = new File("/home/scec-02/kmilner/simulators/catalogs/");
		if (!catsDir.exists())
			catsDir = new File("/data/kevin/simulators/catalogs");
		Preconditions.checkState(catsDir.exists(), "No known catalog dirs exist");
		File catDir = new File(catsDir, catalogDirName);
		if (!catDir.exists() && new File(catsDir, "bruce").exists())
			catDir = new File(new File(catsDir, "bruce"), catalogDirName);
		Preconditions.checkState(catDir.exists(), "Could not find catalog dir for "+catalogDirName);
		File xmlFile = new File(catDir, "erf_params.xml");
		Preconditions.checkState(xmlFile.exists(), "Catalog dir found, but no 'erf_params.xml' in %s", catDir.getAbsolutePath());
		AbstractERF erf;
		try {
			erf = ERFSaver.LOAD_ERF_FROM_FILE(xmlFile.getAbsolutePath());
		} catch (Exception e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		erf.updateForecast();
		return erf;
	}
	
	private int datasetID;
	private String name;
	private String dirName;
	private String description;
	private int velocityModelID;
	private Region region;
	private String dbHost;
	private GregorianCalendar date;
	
	private DBAccess db;
	private Runs2DB runs2db;
	
	private CyberShakeStudy(GregorianCalendar date, int datasetID, String name, String dirName, String description,
			int velocityModelID, Region region, String dbHost) {
		this.date = date;
		this.datasetID = datasetID;
		this.name = name;
		this.dirName = dirName;
		this.description = description;
		this.velocityModelID = velocityModelID;
		this.region = region;
		this.dbHost = dbHost;
	}
	
	private static GregorianCalendar cal(int year, int month) {
		return new GregorianCalendar(year, month-1, 1);
	}
	
	private static DateFormat dateFormat = new SimpleDateFormat("MMM yyyy");

	public int getDatasetID() {
		return datasetID;
	}

	public String getName() {
		return name;
	}

	public String getDirName() {
		return dirName;
	}

	public String getDescription() {
		return description;
	}

	public int getVelocityModelID() {
		return velocityModelID;
	}

	public Region getRegion() {
		return region;
	}

	public String getDBHost() {
		return dbHost;
	}
	
	public synchronized DBAccess getDB() {
		if (db == null)
			db = Cybershake_OpenSHA_DBApplication.getDB(dbHost);
		return db;
	}
	
	private synchronized Runs2DB getRunsDB() {
		if (runs2db == null)
			runs2db = new Runs2DB(getDB());
		return runs2db;
	}
	
	private int getERF_ID(DBAccess db) {
		String sql = "SELECT ERF_ID FROM Hazard_Datasets WHERE Hazard_Dataset_ID="+getDatasetID();
		
		try {
			ResultSet rs = db.selectData(sql);
			rs.first();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
	
	private int getRupVarScenarioID(DBAccess db) {
		String sql = "SELECT Rup_Var_Scenario_ID FROM Hazard_Datasets WHERE Hazard_Dataset_ID="+getDatasetID();
		
		try {
			ResultSet rs = db.selectData(sql);
			rs.first();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public CybershakeVelocityModel getVelocityModel() {
		return getRunsDB().getVelocityModel(velocityModelID);
	}
	
	public List<String> getMarkdownMetadataTable() {
		TableBuilder builder = MarkdownUtils.tableBuilder();
		builder.addLine("**Name**", getName());
		builder.addLine("**Date**", dateFormat.format(date.getTime()));
		String regStr;
		if (getRegion() instanceof CaliforniaRegions.CYBERSHAKE_MAP_REGION)
			regStr = "Los Angeles Box";
		else if (getRegion() instanceof CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION)
			regStr = "Central California Box";
		else
			regStr = getRegion().getName();
		builder.addLine("**Region**", regStr);
		builder.addLine("**Description**", getDescription());
		CybershakeVelocityModel vm = getVelocityModel();
		builder.addLine("**Velocity Model**", vm.getName()+", "+vm.getVersion());
		return builder.build();
	}
	
	public void writeMarkdownSummary(File dir) throws IOException {
		writeMarkdownSummary(dir, false);
	}
	
	public void writeMarkdownSummary(File dir, boolean replot) throws IOException {
		List<String> lines = new LinkedList<>();
		String topLink = "*[(top)](#"+MarkdownUtils.getAnchorName(getName())+")*";
		lines.add("# "+getName());
		lines.add("## Metadata");
		lines.addAll(getMarkdownMetadataTable());
		lines.add("");
		int tocIndex = lines.size();
		
		Map<Vs30_Source, List<String>> gmpeLinksMap = new HashMap<>();
		Map<Vs30_Source, List<String>> gmpeNamesMap = new HashMap<>();
		
		Table<String, Vs30_Source, List<String>> siteHazardLinksTable = HashBasedTable.create();
		Table<String, Vs30_Source, List<String>> siteHazardNamesTable = HashBasedTable.create();
		
		String rotDDLink = null;
		
		File[] dirList = dir.listFiles();
		Arrays.sort(dirList, new FileNameComparator());
		for (File subDir : dirList) {
			if (!subDir.isDirectory())
				continue;
			File mdFile = new File(subDir, "README.md");
			if (!mdFile.exists())
				continue;
			String name = subDir.getName();
			if (name.startsWith("gmpe_comparisons_") && name.contains("_Vs30")) {
				String gmpeName = name.substring("gmpe_comparisons_".length());
				gmpeName = gmpeName.substring(0, gmpeName.indexOf("_Vs30"));
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				if (!gmpeLinksMap.containsKey(vs30)) {
					gmpeLinksMap.put(vs30, new ArrayList<>());
					gmpeNamesMap.put(vs30, new ArrayList<>());
				}
				
				gmpeLinksMap.get(vs30).add(name);
				gmpeNamesMap.get(vs30).add(gmpeName);
			} else if (name.equals("rotd_ratio_comparisons")) {
				Preconditions.checkState(rotDDLink == null, "Duplicate RotDD dirs! %s and %s", name, rotDDLink);
				rotDDLink = name;
			} else if (name.startsWith("site_hazard_")) {
				String siteName = name.substring("site_hazard_".length());
				siteName = siteName.substring(0, siteName.indexOf("_"));
				String gmpeName = name.substring("site_hazard_".length()+siteName.length()+1);
				gmpeName = gmpeName.substring(0, gmpeName.indexOf("_Vs30"));
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				if (!siteHazardNamesTable.contains(gmpeName, vs30)) {
					siteHazardLinksTable.put(gmpeName, vs30, new ArrayList<>());
					siteHazardNamesTable.put(gmpeName, vs30, new ArrayList<>());
				}
				
				siteHazardLinksTable.get(gmpeName, vs30).add(name);
				siteHazardNamesTable.get(gmpeName, vs30).add(siteName);
			}
		}
		
		if (!gmpeLinksMap.isEmpty()) {
			lines.add("");
			lines.add("## GMPE Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Vs30_Source vs30 : gmpeLinksMap.keySet()) {
				if (gmpeLinksMap.keySet().size() > 1) {
					lines.add("### Vs30 model for GMPE comparisons: "+vs30);
					lines.add("");
				}
				List<String> gmpeNames = gmpeNamesMap.get(vs30);
				List<String> gmpeLinks = gmpeLinksMap.get(vs30);
				
				for (int i=0; i<gmpeNames.size(); i++)
					lines.add("* ["+gmpeNames.get(i)+"]("+gmpeLinks.get(i)+"/)");
			}
		}
		if (!siteHazardLinksTable.isEmpty()) {
			lines.add("");
			lines.add("## Site Hazard Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Cell<String, Vs30_Source, List<String>> cell : siteHazardLinksTable.cellSet()) {
				String gmpe = cell.getRowKey();
				Vs30_Source vs30 = cell.getColumnKey();
				lines.add("### GMPE: "+cell.getRowKey()+", Vs30 model: "+cell.getColumnKey());
				lines.add("");
				List<String> siteNames = siteHazardNamesTable.get(gmpe, vs30);
				List<String> siteLinks = siteHazardLinksTable.get(gmpe, vs30);
				
				for (int i=0; i<siteNames.size(); i++)
					lines.add("* ["+siteNames.get(i)+"]("+siteLinks.get(i)+"/)");
			}
		}
		if (rotDDLink != null) {
			lines.add("");
			lines.add("## RotD100/RotD50 Ratios");
			lines.add(topLink);
			lines.add("");
			lines.add("[RotD100/RotD50 Ratios Plotted Here]("+rotDDLink+"/)");
		}
		
		File resourcesDir = new File(dir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		lines.add("");
		lines.addAll(writeStandardDiagnosticPlots(resourcesDir, 5000, 6d, replot, topLink));
		
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2));
		
		MarkdownUtils.writeReadmeAndHTML(lines, dir);
	}
	
	public List<String> writeStandardDiagnosticPlots(File outputDir, int skipYears, double minMag, boolean replot, String topLink)
			throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("## Plots");
		
		TableBuilder table;
		
		if (replot || !new File(outputDir, "mfd.png").exists()) {
			writeMFD(outputDir, "mfd", null);
		}
		lines.add("### Magnitude-Frequency Plot");
		lines.add(topLink);
		lines.add("");
		lines.add("![MFD]("+outputDir.getName()+"/mfd.png)");
		
		if (replot || !new File(outputDir, "rv_count.png").exists()) {
			writeRVCount(outputDir, "rv_count", null);
		}
		lines.add("### Rupture Variation Count Plot");
		lines.add(topLink);
		lines.add("");
		lines.add("![RV Count]("+outputDir.getName()+"/rv_count.png)");
		
		if (replot || !new File(outputDir, "mag_area_hist2D.png").exists()) {
			writeMagAreaPlot(outputDir, "mag_area", null);
		}
		lines.add("### Magnitude-Area Plots");
		lines.add(topLink);
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.addLine("Scatter", "2-D Hist");
		table.initNewLine();
		table.addColumn("![MFD Scatter]("+outputDir.getName()+"/mag_area.png)");
		table.addColumn("![MFD Hist]("+outputDir.getName()+"/mag_area_hist2D.png)");
		table.finalizeLine();
		lines.addAll(table.build());
		
		if (new File(outputDir, "mfd_no_aleatory.png").exists()
				|| erf != null && (erf instanceof MeanUCERF2 || erf.getName().startsWith(MeanUCERF2.NAME))) {
			UCERF2_AleatoryMagVarRemovalMod probMod = new UCERF2_AleatoryMagVarRemovalMod(erf);
			
			lines.add("### Plots Without Aleatory Magnitude Variability");
			
			if (replot || !new File(outputDir, "mfd_no_aleatory.png").exists()) {
				writeMFD(outputDir, "mfd_no_aleatory", probMod);
			}
			lines.add("#### Magnitude-Frequency Plot");
			lines.add(topLink);
			lines.add("");
			lines.add("![MFD]("+outputDir.getName()+"/mfd_no_aleatory.png)");
			
			if (replot || !new File(outputDir, "rv_count_no_aleatory.png").exists()) {
				writeRVCount(outputDir, "rv_count_no_aleatory", probMod);
			}
			lines.add("#### Rupture Variation Count Plot");
			lines.add(topLink);
			lines.add("");
			lines.add("![RV Count]("+outputDir.getName()+"/rv_count_no_aleatory.png)");
			
			if (replot || !new File(outputDir, "mag_area_no_aleatory_hist2D.png").exists()) {
				writeMagAreaPlot(outputDir, "mag_area_no_aleatory", probMod);
			}
			lines.add("#### Magnitude-Area Plots");
			lines.add(topLink);
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Scatter", "2-D Hist");
			table.initNewLine();
			table.addColumn("![MFD Scatter]("+outputDir.getName()+"/mag_area_no_aleatory.png)");
			table.addColumn("![MFD Hist]("+outputDir.getName()+"/mag_area_no_aleatory_hist2D.png)");
			table.finalizeLine();
			lines.addAll(table.build());
		}
		
		return lines;
	}
	
	private void writeMagAreaPlot(File outputDir, String prefix, RuptureProbabilityModifier probMod) throws IOException {
		DefaultXY_DataSet scatter = new DefaultXY_DataSet();
		AbstractERF erf = getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				if (rup.getProbability() == 0d
						|| probMod != null && probMod.getModifiedProb(sourceID, rupID, rup.getProbability()) == 0d)
					continue;
				double area;
				if (rup instanceof RSQSimProbEqkRup) {
					area = 0;
					for (SimulatorElement elem : ((RSQSimProbEqkRup)rup).getElements())
						area += elem.getArea();
					area /= 1e6; // m^2 to km^2
				} else {
					area = rup.getRuptureSurface().getArea();
				}
				scatter.set(area, rup.getMag());
			}
		}
		MagAreaScalingPlot.writeScatterPlots(scatter, getName(), outputDir, prefix, 650, 600);
	}
	
	private void writeMFD(File outputDir, String prefix, RuptureProbabilityModifier probMod) throws IOException {
		IncrementalMagFreqDist mfd = buildMagFunc();
		Range xRange = new Range(mfd.getMinX() - 0.5*mfd.getDelta(), mfd.getMaxX()+0.5*mfd.getDelta());
		AbstractERF erf = getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double mag = rup.getMag();
				if (xRange.contains(mag)) {
					double rate;
					if (probMod != null) {
						double origProb = rup.getProbability();
						double newProb = probMod.getModifiedProb(sourceID, rupID, origProb);
						if (newProb == 0d)
							continue;
						rate = -Math.log(1d - newProb);
					} else {
						rate = rup.getMeanAnnualRate(1d);
					}
					mfd.add(mfd.getClosestXIndex(mag), rate);
				}
			}
		}
		EvenlyDiscretizedFunc cumulative = mfd.getCumRateDistWithOffset();
		
		writeMagPlot(outputDir, prefix, "Annual Rate", "Magnitude-Frequency Distribution", mfd, cumulative, true, xRange);
	}
	
	private void writeRVCount(File outputDir, String prefix, RuptureProbabilityModifier probMod) throws IOException {
		IncrementalMagFreqDist mnd = buildMagFunc();
		Range xRange = new Range(mnd.getMinX() - 0.5*mnd.getDelta(), mnd.getMaxX()+0.5*mnd.getDelta());
		DBAccess db = getDB();
		int erfID = getERF_ID(db);
		int rvScenID = getRupVarScenarioID(db);
		AbstractERF erf = getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double mag = rup.getMag();
				if (rup.getProbability() == 0d
						|| probMod != null && probMod.getModifiedProb(sourceID, rupID, rup.getProbability()) == 0d)
					continue;
				if (xRange.contains(mag)) {
					String sql = "SELECT count(*) FROM Rupture_Variations WHERE ERF_ID="+erfID+" AND Rup_Var_Scenario_ID="+rvScenID
							+" AND Source_ID="+sourceID+" AND Rupture_ID="+rupID;
					try {
						ResultSet rs = db.selectData(sql);
						rs.first();
						int count = rs.getInt(1);
						if (count > 0)
							mnd.add(mnd.getClosestXIndex(mag), (double)count);
					} catch (SQLException e) {
						throw new IllegalStateException(e);
					}
					
				}
			}
		}
		EvenlyDiscretizedFunc cumulative = mnd.getCumRateDistWithOffset();
		
		writeMagPlot(outputDir, prefix, "Number", "Rupture Variation Count", mnd, cumulative, false, xRange);
	}
	
	private void writeMagPlot(File outputDir, String prefix, String yAxisLabel, String title, EvenlyDiscretizedFunc incremental,
			EvenlyDiscretizedFunc cumulative, boolean doGR, Range xRange) throws IOException {
		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		funcs.add(incremental);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.GRAY));
		incremental.setName(getName()+" Incremental");

		funcs.add(cumulative);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLACK));
		cumulative.setName("Cumulative");
		
		if (doGR) {
			double totCmlRate = cumulative.getY(0);
			GutenbergRichterMagFreqDist grMFD = new GutenbergRichterMagFreqDist(1d, 1d,
					xRange.getLowerBound(), xRange.getUpperBound(), 100);
			grMFD.scaleToIncrRate(0, totCmlRate);
			grMFD.setName("G-R B=1");
			funcs.add(grMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 1f, Color.GRAY));
		}
		
		String xAxisLabel = "Magnitude";
		
		PlotSpec plot = new PlotSpec(funcs, chars, title, xAxisLabel, yAxisLabel);
		plot.setLegendVisible(true);
		
		double minY = Double.POSITIVE_INFINITY;
		double maxY = 0d;
		for (DiscretizedFunc func : funcs) {
			minY = Math.min(minY, AbstractPlot.minNonZero(func));
			maxY = Math.max(maxY, func.getMaxY());
		}
		Range yRange;
		if (!Doubles.isFinite(minY))
			yRange = new Range(1d, 10d);
		else
			yRange = AbstractPlot.calcEncompassingLog10Range(minY, maxY);
		
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(20);
		plotPrefs.setAxisLabelFontSize(22);
		plotPrefs.setPlotLabelFontSize(24);
		plotPrefs.setLegendFontSize(22);
		plotPrefs.setBackgroundColor(Color.WHITE);
		HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
		
		gp.drawGraphPanel(plot, false, true, xRange, yRange);
		gp.getChartPanel().setSize(650, 600);
		gp.saveAsPNG(new File(outputDir, prefix+".png").getAbsolutePath());
		gp.saveAsPDF(new File(outputDir, prefix+".pdf").getAbsolutePath());
		gp.saveAsTXT(new File(outputDir, prefix+".txt").getAbsolutePath());
	}
	
	private static IncrementalMagFreqDist buildMagFunc() {
		double minMag = 6;
		double delta = 0.1;
		int num = (int)((9d - minMag)/delta);
		
		IncrementalMagFreqDist mfd = new IncrementalMagFreqDist(minMag+0.5*delta, num, delta);
		
		return mfd;
	}
	
	public static void writeStudiesIndex(File dir) throws IOException {
		// sort by date, newest first
		List<Long> times = new ArrayList<>();
		List<CyberShakeStudy> studies = new ArrayList<>();
		for (File subDir : dir.listFiles()) {
			if (!subDir.isDirectory())
				continue;
			CyberShakeStudy study = null;
			for (CyberShakeStudy s : values()) {
				if (subDir.getName().equals(s.getDirName())) {
					study = s;
					break;
				}
			}
			if (study == null)
				continue;
			studies.add(study);
			times.add(study.date.getTimeInMillis());
		}
		studies = ComparablePairing.getSortedData(times, studies);
		Collections.reverse(studies);
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Date", "Name", "Description");
		for (CyberShakeStudy study : studies) {
			table.initNewLine();
			
			table.addColumn(dateFormat.format(study.date.getTime()));
			table.addColumn("["+study.getName()+"]("+study.getDirName()+"#"+MarkdownUtils.getAnchorName(study.getName())+")");
			table.addColumn(study.getDescription());
			
			table.finalizeLine();
		}
		
		List<String> lines = new LinkedList<>();
		lines.add("# CyberShake Study Analysis");
		lines.add("");
		lines.addAll(table.build());
		
		MarkdownUtils.writeReadmeAndHTML(lines, dir);
	}
	
	abstract AbstractERF buildERF();
	
	private AbstractERF erf;
	public synchronized AbstractERF getERF() {
		if (erf == null)
			erf = buildERF();
		return erf;
	}
	
	public static void main(String[] args) throws IOException {
		File gitDir = new File("/home/kevin/git/cybershake-analysis");
		
		boolean replot = true;
		
		for (CyberShakeStudy study : CyberShakeStudy.values()) {
			File studyDir = new File(gitDir, study.getDirName());
			if (studyDir.exists())
				study.writeMarkdownSummary(studyDir, replot);
		}
		
		writeStudiesIndex(gitDir);
		
		for (CyberShakeStudy study : CyberShakeStudy.values())
			study.getDB().destroy();
	}

}
