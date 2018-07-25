package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.xyz.EvenlyDiscrXYZ_DataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZGraphPanel;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZPlotSpec;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;

import scratch.kevin.bbp.BBP_Site;
import scratch.kevin.bbp.SpectraPlotter;
import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.Vs30_Source;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.ruptures.BBP_CatalogSimZipLoader;
import scratch.kevin.util.MarkdownUtils;
import scratch.kevin.util.MarkdownUtils.TableBuilder;

public class Study3Dvs1DCompare {
	
	private SimulationRotDProvider<CSRupture> prov3D;
	private SimulationRotDProvider<CSRupture> prov1D;
	private List<Site> sites;
	
	private static boolean DIST_JB = false; // else dist rup

	public Study3Dvs1DCompare(SimulationRotDProvider<CSRupture> prov3D, SimulationRotDProvider<CSRupture> prov1D,
			List<Site> sites) {
		this.prov3D = prov3D;
		this.prov1D = prov1D;
		this.sites = sites;
	}
	
	public void generatePage(File outputDir, List<String> headerLines, double[] periods, Collection<Site> highlightSites) throws IOException {
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		List<String> lines = new LinkedList<>();
		if (headerLines != null && !headerLines.isEmpty()) {
			lines.addAll(headerLines);
			if (!lines.get(lines.size()-1).isEmpty())
				lines.add("");
		}
		
		lines.add("## 3-D vs 1-D Comparisons");
		lines.add("");
		lines.add("* 3-D Model: "+prov3D.getName());
		lines.add("* 1-D Model: "+prov1D.getName());
		lines.add("");
		
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		List<Site> mySites = new ArrayList<>();
		if (sites.size() > 1) {
			mySites.add(null); // all sites
			if (highlightSites != null)
				mySites.addAll(highlightSites);
		} else {
			mySites = sites;
		}
		
		System.out.println("Calculating all gains...");
		Table<Site, CSRupture, AmpComparison> gains = calcGains();
		System.out.println("DONE.");

		System.out.println("Calculating all distances...");
		Table<Site, CSRupture, Double> dists = calcDistances(DIST_JB);
		System.out.println("DONE.");
		
		for (Site site : mySites) {
			Collection<AmpComparison> comps;
			String siteName;
			String sitePrefix;
			if (site == null) {
				comps = gains.values();
				siteName = "All Sites";
				sitePrefix = "all_sites";
			} else {
				comps = gains.row(site).values();
				siteName = site.getName();
				sitePrefix = site.getName().replaceAll(" ", "_");
			}
			lines.add("## "+siteName);
			lines.add(topLink); lines.add("");
			
			lines.add("### "+siteName+" 3-D Gain Spectra");
			lines.add(topLink); lines.add("");
			
			System.out.println("Plotting Gain Spectra for "+siteName);
			String ampPrefix = sitePrefix+"_amp_spectra";
			plotAmpSpectra(comps, resourcesDir, ampPrefix, siteName);
			lines.add("![Gain Spectra]("+resourcesDir.getName()+"/"+ampPrefix+".png)");
			
			lines.add("### "+siteName+" 3-D Mag/Distance Gain Plots");
			lines.add(topLink); lines.add("");
			System.out.println("Plotting Mag/Dist gains for "+siteName);
			File[] magDistPots = plotMagDist(resourcesDir, siteName+"_mag_dist_gains", site, periods, gains, dists);
			
			TableBuilder table = MarkdownUtils.tableBuilder().initNewLine();
			for (double period : periods)
				table.addColumn("**"+optionalDigitDF.format(period)+"s**");
			table.finalizeLine().initNewLine();
			for (File magDistPot : magDistPots)
				table.addColumn("![Mag Dist XYZ]("+resourcesDir.getName()+"/"+magDistPot.getName()+")");
			table.finalizeLine();
			lines.addAll(table.build());
			lines.add("");
		}
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
		lines.add(tocIndex, "## Table Of Contents");
		
		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
	}
	
	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.##");
	
	private Table<Site, CSRupture, AmpComparison> calcGains() throws IOException {
		Table<Site, CSRupture, AmpComparison> ret = HashBasedTable.create();
		
		int numSkipped = 0;
		
		for (Site site : sites) {
			for (CSRupture rup : prov3D.getRupturesForSite(site)) {
				if (prov1D.getNumSimulations(site, rup) < 1)
					continue;
//				System.out.println("3D has "+prov3D.getNumSimulations(site, rup)+"\t1D has "+prov1D.getNumSimulations(site, rup));
				List<DiscretizedFunc> vals3D = prov3D.getRotD50s(site, rup);
				List<DiscretizedFunc> vals1D;
				try {
					vals1D = prov1D.getRotD50s(site, rup);
				} catch (Exception e) {
					numSkipped++;
					continue;
				}
				
				ret.put(site, rup, new AmpComparison(site, rup, vals3D, vals1D));
			}
		}
		if (numSkipped > 0)
			System.out.println("WARNING: skipped "+numSkipped+"/"+(numSkipped+ret.size())+" ruptures");
		
		return ret;
	}
	
	private Table<Site, CSRupture, Double> calcDistances(boolean distJB) throws IOException {
		Table<Site, CSRupture, Double> ret = HashBasedTable.create();
		
		for (Site site : sites) {
			Location siteLoc = site.getLocation();
			for (CSRupture rup : prov3D.getRupturesForSite(site)) {
				RuptureSurface surf = rup.getRup().getRuptureSurface();
				if (distJB)
					ret.put(site, rup, surf.getDistanceJB(siteLoc));
				else
					ret.put(site, rup, surf.getDistanceRup(siteLoc));
			}
		}
		
		return ret;
	}
	
	private class AmpComparison {
		private Site site;
		private CSRupture rupture;
		private List<DiscretizedFunc> vals3D;
		private List<DiscretizedFunc> vals1D;
		private List<DiscretizedFunc> gains3D;
		
		public AmpComparison(Site site, CSRupture rupture, List<DiscretizedFunc> vals3D, List<DiscretizedFunc> vals1D) {
			super();
			this.site = site;
			this.rupture = rupture;
			this.vals3D = vals3D;
			this.vals1D = vals1D;
			Preconditions.checkState(vals3D.size() == vals1D.size());
			
			gains3D = new ArrayList<>();
			
			for (int i=0; i<vals3D.size(); i++) {
				DiscretizedFunc func3D = vals3D.get(i);
				DiscretizedFunc func1D = vals1D.get(i);
				ArbitrarilyDiscretizedFunc gainsFunc = new ArbitrarilyDiscretizedFunc();
				
				for (Point2D pt3D : func3D) {
					if (func1D.hasX(pt3D.getX()))
						gainsFunc.set(pt3D.getX(), pt3D.getY()/func1D.getY(pt3D.getX()));
				}
				
				Preconditions.checkState(gainsFunc.size() > 0, "No common periods between 3-D and 1-D");
				
				gains3D.add(gainsFunc);
			}
		}

		public Site getSite() {
			return site;
		}

		public CSRupture getRupture() {
			return rupture;
		}

		public List<DiscretizedFunc> getVals3D() {
			return vals3D;
		}

		public List<DiscretizedFunc> getVals1D() {
			return vals1D;
		}

		public List<DiscretizedFunc> getGains3D() {
			return gains3D;
		}
	}
	
	private void plotAmpSpectra(Collection<AmpComparison> comps, File resourcesDir, String prefix, String siteName) throws IOException {
		List<DiscretizedFunc> allGainFuncs = new ArrayList<>();
		
		for (AmpComparison comp : comps)
			allGainFuncs.addAll(comp.getGains3D());
		
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		Range gainRange = SpectraPlotter.populateRefRangeFuncs(allGainFuncs, "3-D Gains", funcs, chars);
		
		Range xRange = new Range(allGainFuncs.get(0).getMinX(), allGainFuncs.get(0).getMaxX());
		Range yRange = new Range(Math.max(0.333, Math.min(gainRange.getLowerBound()*0.9, 0.9)),
				Math.min(3, Math.max(gainRange.getUpperBound()*1.1, 1.d)));
		
		XY_DataSet horzLine = new DefaultXY_DataSet();
		horzLine.set(xRange.getLowerBound(), 1d);
		horzLine.set(xRange.getUpperBound(), 1d);
		funcs.add(horzLine);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.BLACK));
		
		PlotSpec spec = new PlotSpec(funcs, chars, siteName+" 3-D Gain Factors", "Period (s)", "3-D Gain (vs 1-D)");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(24);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(20);
		gp.setBackgroundColor(Color.WHITE);
		gp.drawGraphPanel(spec, true, true, xRange, yRange);
		
		File file = new File(resourcesDir, prefix);
		gp.getChartPanel().setSize(800, 800);
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
	}
	
	private File[] plotMagDist(File resourcesDir, String prefix, Site site, double[] periods,
			Table<Site, CSRupture, AmpComparison> comps, Table<Site, CSRupture, Double> dists) throws IOException {
		SummaryStatistics magStats = new SummaryStatistics();
		for (CSRupture rup : comps.columnKeySet())
			magStats.addValue(rup.getRup().getMag());
		
		double distThreshold = 1;
		
		SummaryStatistics distStats = new SummaryStatistics();
		for (Double dist : dists.values())
			if (dist >= distThreshold)
				distStats.addValue(dist);
		
		double minMag = Math.floor(magStats.getMin()*10d)/10d;
		double maxMag = Math.ceil(magStats.getMax()*10d)/10d;
		int numDist = 20;
		int numMag = 15;
		double distSpacing = (Math.log10(distStats.getMax()) - Math.log10(distThreshold)) / (double)numDist;
		double magSpacing = (maxMag - minMag) / (double)numMag;
		EvenlyDiscrXYZ_DataSet[] xyzs = new EvenlyDiscrXYZ_DataSet[periods.length];
		for (int p=0; p<periods.length; p++)
			xyzs[p] = new EvenlyDiscrXYZ_DataSet(numDist, numMag, Math.log10(distThreshold)+0.5*distSpacing,
					minMag+0.5*magSpacing, distSpacing, magSpacing);
		EvenlyDiscrXYZ_DataSet counts = new EvenlyDiscrXYZ_DataSet(numDist, numMag, Math.log10(distThreshold)+0.5*distSpacing,
				minMag+0.5*magSpacing, distSpacing, magSpacing);
		
		List<Site> mySites;
		if (site == null) {
			mySites = new ArrayList<>(comps.rowKeySet());
		} else {
			mySites = new ArrayList<>();
			mySites.add(site);
		}
		
		for (Site compSite : mySites) {
			Map<CSRupture, AmpComparison> compsMap = comps.row(compSite);
			for (CSRupture rup : compsMap.keySet()) {
				double dist = dists.get(site, rup);
				if (dist < distThreshold)
					continue;
				double mag = rup.getRup().getMag();
				int magIndex = counts.getYIndex(mag);
				if (magIndex < 0)
					magIndex = 0;
				if (magIndex >= numMag)
					magIndex = numMag-1;
				int distIndex = counts.getXIndex(Math.log10(dist));
				if (distIndex < 0)
					distIndex = 0;
				if (distIndex >= numDist)
					distIndex = numDist-1;
				AmpComparison comp = compsMap.get(rup);
				List<DiscretizedFunc> gains = comp.getGains3D();
				counts.set(distIndex, magIndex, counts.get(distIndex, magIndex)+gains.size());
				for (int p=0; p<periods.length; p++) {
					double sumGains = 0d;
					for (DiscretizedFunc gain : gains)
						sumGains += Math.log10(gain.getY(periods[p]));
					xyzs[p].set(distIndex, magIndex, xyzs[p].get(distIndex, magIndex)+sumGains);
				}
			}
		}
		
		// convert from sum of gains to average gain
		for (int i=0; i<counts.size(); i++) {
			double count = counts.get(i);
			if (count > 0)
				for (int p=0; p<periods.length; p++)
					xyzs[p].set(i, xyzs[p].get(i)/count);
		}
		
		double maxGain = 0d;
		for (EvenlyDiscrXYZ_DataSet xyz : xyzs)
			maxGain = Math.max(maxGain, Math.max(Math.abs(xyz.getMinZ()), Math.abs(xyz.getMaxZ())));
		CPT gainCPT = GMT_CPT_Files.GMT_POLAR.instance().rescale(-maxGain, maxGain);
		
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(18);
		plotPrefs.setAxisLabelFontSize(24);
		plotPrefs.setPlotLabelFontSize(24);
		plotPrefs.setLegendFontSize(20);
		plotPrefs.setBackgroundColor(Color.WHITE);
		
		File[] plotFiles = new File[periods.length];
		
		double maxDist = Math.pow(10, counts.getMaxX()+0.5*counts.getGridSpacingX());
		
		for (int p=0; p<periods.length; p++) {
			String periodStr;
			if (periods[p] == Math.round(periods[p]))
				periodStr = (int)periods[p]+"s";
			else
				periodStr = (float)periods[p]+"s";
			String title = periodStr+" 3-D Gains";
			XYZPlotSpec xyzSpec = new XYZPlotSpec(xyzs[p], gainCPT, title, "Log10 Distance Rup", "Magnitude", "Log10(3-D Gain)");
			XYZGraphPanel xyzGP = new XYZGraphPanel(plotPrefs);
			xyzGP.drawPlot(xyzSpec, false, false, new Range(Math.log10(distThreshold), Math.log10(maxDist)), new Range(minMag, maxMag));
			xyzGP.getChartPanel().getChart().setBackgroundPaint(Color.WHITE);
			xyzGP.getChartPanel().setSize(700, 550);
			plotFiles[p] = new File(resourcesDir, prefix+"_"+periodStr+".png");
			xyzGP.saveAsPNG(plotFiles[p].getAbsolutePath());
		}
		
		return plotFiles;
	}
	
	static StudyRotDProvider getSimProv(CyberShakeStudy study, String[] siteNames, File ampsCacheDir, double[] periods,
			CybershakeIM[] rd50_ims, Vs30_Source vs30Source) throws SQLException, IOException {
		DBAccess db = study.getDB();
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		
		Map<Site, Integer> runIDsMap = new HashMap<>();
		
		AbstractERF erf = study.getERF();
		CSRupture[][] csRups = new CSRupture[erf.getNumSources()][];
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, ampsCacheDir, erf);
		
		Map<Site, List<CSRupture>> siteRupsMap = new HashMap<>();
		
		for (String siteName : siteNames) {
			CybershakeSite csSite = sites2db.getSiteFromDB(siteName);
			
			System.out.println("Finding Run_ID for study "+study+", site "+siteName);
			String sql = "SELECT C.Run_ID FROM Hazard_Curves C JOIN CyberShake_Runs R ON R.Run_ID=C.Run_ID\n" + 
					"WHERE R.Site_ID="+csSite.id+" AND C.Hazard_Dataset_ID="+study.getDatasetID()+" ORDER BY C.Curve_Date DESC LIMIT 1";
			System.out.println(sql);
			ResultSet rs = db.selectData(sql);
			Preconditions.checkState(rs.first());
			int runID = rs.getInt(1);
			
			System.out.println("Detected Run_ID="+runID);
			
			CybershakeRun run = new Runs2DB(db).getRun(runID);
			
			System.out.println("Building site");
			Site site = StudyGMPE_Compare.buildSite(csSite, runID, study, vs30Source, db);
			
			runIDsMap.put(site, runID);
			
			List<CSRupture> siteRups = StudyGMPE_Compare.getSiteRuptures(site, amps2db, run.getERFID(), run.getRupVarScenID(),
					csRups, erf, runID, rd50_ims[0]);
			siteRupsMap.put(site, siteRups);
		}
		
		return new StudyRotDProvider(amps2db, runIDsMap, siteRupsMap, periods, rd50_ims, null, study.getName());
	}
	
//	private void plotCheckerboard()

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		File rsqsimCatalogBaseDir = new File("/data/kevin/simulators/catalogs");
		
		CyberShakeStudy study3D = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
		CyberShakeStudy study1D = null;
		RSQSimCatalog catalog = Catalogs.BRUCE_2585_1MYR.instance(rsqsimCatalogBaseDir);
		double catalogMinMag = 6.5;
		File bbpFile1D = new File("/data/kevin/bbp/parallel/2018_04_13-rundir2585_1myrs-all-m6.5-skipYears5000-noHF-csLASites/results_rotD.zip");
		String[] siteNames = { "PAS" };
		double[] periods = {3d, 5d, 7.5, 10d};
		
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		CybershakeIM[] rd50_ims = new PeakAmplitudesFromDB(study3D.getDB()).getIMs(Doubles.asList(periods),
				IMType.SA, CyberShakeComponent.RotD50).toArray(new CybershakeIM[0]);
		
		StudyRotDProvider prov3D = getSimProv(study3D, siteNames, ampsCacheDir, periods, rd50_ims, vs30Source);
		List<Site> sites = new ArrayList<>(prov3D.getAvailableSites());
		SimulationRotDProvider<CSRupture> prov1D;
		if (study1D == null) {
			// BBP for 1D
			Preconditions.checkNotNull(bbpFile1D);
			Preconditions.checkNotNull(catalog);
			
			System.out.println("Loading RSQSim Catalog");
			List<RSQSimEvent> events = catalog.loader().minMag(catalogMinMag).load();
			Map<Integer, RSQSimEvent> eventsMap = new HashMap<>();
			for (RSQSimEvent e : events)
				eventsMap.put(e.getID(), e);
			System.out.println("Loaded "+events.size()+" events");
			
			List<BBP_Site> bbpSites = BBP_Site.readFile(bbpFile1D.getParentFile());
			BiMap<BBP_Site, Site> bbpToSiteMap = HashBiMap.create();
			for (int i=bbpSites.size(); --i>=0; ) {
				BBP_Site bbpSite = bbpSites.get(i);
				Site match = null;
				for (Site site : sites) {
					if (site.getName().startsWith(bbpSite.getName())) {
						match = site;
						break;
					}
				}
				if (match == null)
					bbpSites.remove(i);
				else
					bbpToSiteMap.put(bbpSite, match);
			}
			
			BBP_CatalogSimZipLoader bbpLoader = new BBP_CatalogSimZipLoader(bbpFile1D, bbpSites, bbpToSiteMap, eventsMap);
			Map<Site, List<CSRupture>> siteRups = new HashMap<>();
			for (Site site : sites)
				siteRups.put(site, new ArrayList<>(prov3D.getRupturesForSite(site)));
			prov1D = new CSRuptureBBPWrapperRotDProvider((RSQSimSectBundledERF)study3D.getERF(), eventsMap, bbpLoader, siteRups);
		} else {
			throw new IllegalStateException("not yet implemented"); //TODO
		}
		
		Study3Dvs1DCompare comp = new Study3Dvs1DCompare(prov3D, prov1D, sites);
		
		File studyDir = new File(mainOutputDir, study3D.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		File outputDir = new File(studyDir, "3d_1d_comparison");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		comp.generatePage(outputDir, null, periods, null);
		
		study3D.writeMarkdownSummary(studyDir);
		CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		
		study3D.getDB().destroy();
		if (study1D != null)
			study1D.getDB().destroy();
		
		System.exit(0);
	}

}
