package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
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
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZGraphPanel;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZPlotSpec;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.impl.DoubleParameter;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;
import org.opensha.sha.imr.param.SiteParams.Vs30_TypeParam;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import scratch.kevin.bbp.BBP_Module.VelocityModel;
import scratch.kevin.bbp.BBP_Site;
import scratch.kevin.bbp.SpectraPlotter;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.ruptures.BBP_CatalogSimZipLoader;

public class Study3Dvs1DCompare {
	
	private SimulationRotDProvider<CSRupture> prov3D;
	private SimulationRotDProvider<CSRupture> prov1D;
	private List<Site> sites;
	private double vs30_1d;
	private double similarDeltaVs30;
	
	private static boolean DIST_JB = false; // else dist rup

	public Study3Dvs1DCompare(SimulationRotDProvider<CSRupture> prov3D, SimulationRotDProvider<CSRupture> prov1D,
			List<Site> sites, double vs30_1d, double similarDeltaVs30) {
		this.prov3D = prov3D;
		this.prov1D = prov1D;
		this.sites = sites;
		this.vs30_1d = vs30_1d;
		this.similarDeltaVs30 = similarDeltaVs30;
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
		lines.add("* 1-D Model: "+prov1D.getName()+" (Vs30="+optionalDigitDF.format(vs30_1d)+" m/s)");
		lines.add("");
		
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		List<Site> mySites = new ArrayList<>();
		if (sites.size() > 1) {
			mySites.add(null); // all sites
			if (highlightSites != null)
				mySites.addAll(highlightSites);
			else if (sites.size() <= 10)
				mySites.addAll(sites);
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
			List<Site> siteBundle;
			Collection<AmpComparison> comps;
			String siteName;
			String sitePrefix;
			int numSimilarSites = 0;
			if (site == null) {
				comps = new ArrayList<>();
				siteBundle = new ArrayList<>();
				for (Site s2 : sites) {
					double vs30 = s2.getParameter(Double.class, Vs30_Param.NAME).getValue();
					double diff = Math.abs(vs30 - vs30_1d);
					if (diff < similarDeltaVs30) {
						comps.addAll(gains.row(s2).values());
						siteBundle.add(s2);
						numSimilarSites++;
					}
				}
				if (comps.isEmpty())
					continue;
				siteName = numSimilarSites+" Similar Vs Sites";
				sitePrefix = "similar_sites";
			} else {
				comps = gains.row(site).values();
				siteBundle = new ArrayList<>();
				siteBundle.add(site);
				siteName = site.getName();
				sitePrefix = site.getName().replaceAll(" ", "_");
			}
			lines.add("## "+siteName);
			lines.add(topLink); lines.add("");
			
			if (site == null) {
				lines.add("Results for all "+numSimilarSites+" with Vs30 within "+optionalDigitDF.format(similarDeltaVs30)
					+" m/s of 1-D Vs30="+optionalDigitDF.format(vs30_1d)+" m/s");
				lines.add("");
				TableBuilder table = MarkdownUtils.tableBuilder();
				table.addLine("**Name**", "**Vs30**");
				for (Site s2 : siteBundle) {
					double vs30 = s2.getParameter(Double.class, Vs30_Param.NAME).getValue();
					table.addLine(s2.getName(), optionalDigitDF.format(vs30)+" m/s");
				}
				lines.addAll(table.build());
				lines.add("");
			} else {
				TableBuilder table = MarkdownUtils.tableBuilder();
				table.addLine("**Name**", site.getName());
				table.addLine("**Latitude**", (float)site.getLocation().getLatitude()+"");
				table.addLine("**Longitude**", (float)site.getLocation().getLongitude()+"");
				table.addLine("**Site Parameters**", "");
				for (Parameter<?> param : site) {
					if (param.getName().equals(Vs30_TypeParam.NAME))
						continue;
					String name = "**"+param.getName()+"**";
					String units = param.getUnits();
					if (units != null && !units.isEmpty())
						name += " (*"+units+"*)";
					if (param instanceof DoubleParameter)
						table.addLine(name, ((Double)param.getValue()).floatValue()+"");
					else
						table.addLine(name, param.getValue().toString());
				}
				lines.addAll(table.build());
				lines.add("");
			}
			
			lines.add("### "+siteName+" 3-D Gain Spectra");
			lines.add(topLink); lines.add("");
			
			System.out.println("Plotting Gain Spectra for "+siteName);
			String ampPrefix = sitePrefix+"_amp_spectra";
			plotAmpSpectra(comps, resourcesDir, ampPrefix, siteName);
			lines.add("![Gain Spectra]("+resourcesDir.getName()+"/"+ampPrefix+".png)");
			
			lines.add("### "+siteName+" Scatter Plots");
			lines.add(topLink); lines.add("");
			
			File[][] scatters = plotScatters(resourcesDir, sitePrefix+"_scatter", siteBundle, periods, gains);
			TableBuilder table = MarkdownUtils.tableBuilder().initNewLine();
			for (double period : periods)
				table.addColumn("**"+optionalDigitDF.format(period)+"s**");
			table.finalizeLine().initNewLine();
			for (File[] plots : scatters)
				table.addColumn("![plot]("+resourcesDir.getName()+"/"+plots[0].getName()+")");
			table.finalizeLine().initNewLine();
			for (File[] plots : scatters)
				table.addColumn("![plot]("+resourcesDir.getName()+"/"+plots[1].getName()+")");
			table.finalizeLine();
			lines.addAll(table.build());
			lines.add("");
			
			lines.add("### "+siteName+" 3-D Mag/Distance Gain Plots");
			lines.add(topLink); lines.add("");
			System.out.println("Plotting Mag/Dist gains for "+siteName);
			File[] magDistPots = plotMagDist(resourcesDir, sitePrefix+"_mag_dist_gains", siteBundle, periods, gains, dists);
			
			table = MarkdownUtils.tableBuilder().initNewLine();
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
	
	private File[][] plotScatters(File resourcesDir, String prefix, List<Site> sites, double[] periods,
			Table<Site, CSRupture, AmpComparison> comps) throws IOException {
		
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(18);
		plotPrefs.setAxisLabelFontSize(24);
		plotPrefs.setPlotLabelFontSize(24);
		plotPrefs.setLegendFontSize(20);
		plotPrefs.setBackgroundColor(Color.WHITE);
		
		File[][] ret = new File[periods.length][2];
		
		for (int p=0; p<periods.length; p++) {
			XY_DataSet xy = new DefaultXY_DataSet();
			
			for (Site compSite : sites) {
				Map<CSRupture, AmpComparison> compsMap = comps.row(compSite);
				for (CSRupture rup : compsMap.keySet()) {
					AmpComparison comp = compsMap.get(rup);
					List<DiscretizedFunc> vals3d = comp.getVals3D();
					List<DiscretizedFunc> vals1d = comp.getVals1D();
					
					for (int i=0; i<vals3d.size(); i++) {
						double val3d = vals3d.get(i).getY(periods[p]);
						double val1d = vals1d.get(i).getY(periods[p]);
						
						xy.set(val1d, val3d);
					}
				}
			}
			
			List<XY_DataSet> funcs = Lists.newArrayList();
			List<PlotCurveCharacterstics> chars = Lists.newArrayList();
			
			funcs.add(xy);
			chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 3f, Color.BLACK));
			
			double minVal = Math.min(xy.getMinX(), xy.getMinY());
			double maxVal = Math.max(xy.getMaxX(), xy.getMaxY());
			minVal = Math.pow(10, Math.floor(Math.log10(minVal)));
			maxVal = Math.pow(10, Math.ceil(Math.log10(maxVal)));
			
			Range range = new Range(minVal, maxVal);
			
			DefaultXY_DataSet oneToOne = new DefaultXY_DataSet();
			oneToOne.set(minVal, minVal);
			oneToOne.set(maxVal, maxVal);
			
			funcs.add(oneToOne);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
			
			PlotSpec plot = new PlotSpec(funcs, chars, " ", "3-D", "1-D");
			
			HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
			gp.drawGraphPanel(plot, true, true, range, range);
			gp.getChartPanel().setSize(800, 800);
			
			ret[p][0] = new File(resourcesDir, prefix+"_"+optionalDigitDF.format(periods[p])+".png");
			gp.saveAsPNG(ret[p][0].getAbsolutePath());
			
			double minX = Math.log10(minVal);
			double maxX = Math.log10(maxVal);
			double minY = Math.log10(minVal);
			double maxY = Math.log10(maxVal);
			int nx = 51;
			int ny = 51;
			double gridSpacingX = (maxX - minX)/(nx-1);
			double gridSpacingY = (maxY - minY)/(ny-1);
			
			// XYZ plot (2D hist)
			EvenlyDiscrXYZ_DataSet xyz = new EvenlyDiscrXYZ_DataSet(nx, ny, minX, minY, gridSpacingX, gridSpacingY);
			
			for (Point2D pt : xy) {
				int index = xyz.indexOf(Math.log10(pt.getX()), Math.log10(pt.getY()));
				if (index < 0 || index >= xyz.size())
					throw new IllegalStateException("Scatter point not in XYZ range. x: "
								+pt.getX()+" ["+xyz.getMinX()+" "+xyz.getMaxX()
							+"], y: "+pt.getY()+" ["+xyz.getMinY()+" "+xyz.getMaxY()+"]");
				xyz.set(index, xyz.get(index)+1);
			}
			// convert to density
			for (int i=0; i<xyz.size(); i++) {
				// convert to density
				Point2D pt = xyz.getPoint(i);
				double x = pt.getX();
				double y = pt.getY();
				double binWidth = Math.pow(10, x + 0.5*gridSpacingX) - Math.pow(10, x - 0.5*gridSpacingX);
				double binHeight = Math.pow(10, y + 0.5*gridSpacingY) - Math.pow(10, y - 0.5*gridSpacingY);
				double area = binWidth * binHeight;
				xyz.set(i, xyz.get(i)*area);
			}
			xyz.scale(1d/xyz.getSumZ());
			
			// set all zero to NaN so that it will plot white
			for (int i=0; i<xyz.size(); i++) {
				if (xyz.get(i) == 0)
					xyz.set(i, Double.NaN);
			}
			xyz.log10();
			
			double minZ = Double.POSITIVE_INFINITY;
			double maxZ = Double.NEGATIVE_INFINITY;
			for (int i=0; i<xyz.size(); i++) {
				double val = xyz.get(i);
				if (!Double.isFinite(val))
					continue;
				if (val < minZ)
					minZ = val;
				if (val > maxZ)
					maxZ = val;
			}
			
			System.out.println("MinZ: "+minZ);
			System.out.println("MaxZ: "+maxZ);
			
			CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance();
			if ((float)minZ == (float)maxZ)
				cpt = cpt.rescale(minZ, minZ*2);
			else if (!Double.isFinite(minZ))
				cpt = cpt.rescale(0d, 1d);
			else
				cpt = cpt.rescale(minZ, maxZ);
			cpt.setNanColor(new Color(255, 255, 255, 0));
			
			String zAxisLabel = "Log10(Density)";
			XYZPlotSpec xyzSpec = new XYZPlotSpec(xyz, cpt, " ", "Log10 1-D", "Log10 3-D", zAxisLabel);
			
			funcs = new ArrayList<>();
			chars = new ArrayList<>();
			
			oneToOne = new DefaultXY_DataSet();
			oneToOne.set(minX, minY);
			oneToOne.set(maxX, maxY);
			
			funcs.add(oneToOne);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GRAY));
			
			xyzSpec.setXYElems(funcs);
			xyzSpec.setXYChars(chars);
			
			XYZGraphPanel xyzGP = new XYZGraphPanel(plotPrefs);
			xyzGP.drawPlot(xyzSpec, false, false, new Range(minX-0.5*gridSpacingX, maxX+0.5*gridSpacingX),
					new Range(minY-0.5*gridSpacingY, maxY+0.5*gridSpacingY));
			// write plot
			xyzGP.getChartPanel().setSize(800, 800);
			ret[p][1] = new File(resourcesDir, prefix+"_"+optionalDigitDF.format(periods[p])+"s_hist2D.png");
			xyzGP.saveAsPNG(ret[p][1].getAbsolutePath());
		}
		
		return ret;
	}
	
	private File[] plotMagDist(File resourcesDir, String prefix, List<Site> sites, double[] periods,
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
		
		for (Site compSite : sites) {
			Map<CSRupture, AmpComparison> compsMap = comps.row(compSite);
			for (CSRupture rup : compsMap.keySet()) {
				Preconditions.checkNotNull(rup, "Null rupture?");
				Preconditions.checkNotNull(compSite, "Null site?");
				Double dist = dists.get(compSite, rup);
				Preconditions.checkNotNull(dist, "no distance for site "+compSite.getName()+", rupture "+rup.getSourceID()+","+rup.getRupID());
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
		Preconditions.checkState(maxGain > 0, "Bad max gain: %s", maxGain);
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
//	private void plotCheckerboard()

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
//		CyberShakeStudy study3D = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
//		CyberShakeStudy study1D = null;
//		RSQSimCatalog catalog = Catalogs.BRUCE_2585_1MYR.instance();
//		double catalogMinMag = 6.5;
//		File bbpFile1D = new File("/data/kevin/bbp/parallel/2019_11_11-rundir2585_1myrs-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites/results_rotD.zip");
//		double vs30_1d = VelocityModel.LA_BASIN_500.getVs30();
//		String[] siteNames = { "USC", "STNI", "SBSM", "WNGC" };
//		double[] periods = {3d, 5d, 7.5, 10d};
		
//		CyberShakeStudy study3D = CyberShakeStudy.STUDY_18_9_RSQSIM_2740;
//		CyberShakeStudy study1D = null;
//		RSQSimCatalog catalog = Catalogs.BRUCE_2740.instance();
//		double catalogMinMag = 6.5;
//		File bbpFile1D = new File("/data/kevin/bbp/parallel/2018_09_10-rundir2740-all-m6.5-skipYears5000-noHF-csLASites/results_rotD.zip");
//		double vs30_1d = VelocityModel.LA_BASIN_863.getVs30(); // TODO CHANGE WHEN NEW RUNS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
////		String[] siteNames = { "PAS", "s279", "s119", "s480" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC", "s119", "s279", "s480" };
//		double[] periods = {3d, 5d, 7.5, 10d};
		
		CyberShakeStudy study3D = CyberShakeStudy.STUDY_20_2_RSQSIM_4841;
		CyberShakeStudy study1D = null;
		RSQSimCatalog catalog = Catalogs.BRUCE_4841.instance();
		double catalogMinMag = 6.5;
		File bbpFile1D = new File("/data/kevin/bbp/parallel/2020_02_03-rundir4841-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites/results_rotD.zip");
		double vs30_1d = VelocityModel.LA_BASIN_500.getVs30();
//		String[] siteNames = { "PAS", "s279", "s119", "s480" };
		String[] siteNames = { "USC", "WNGC", "OSI", "PDE", "s022" };
		double[] periods = {3d, 5d, 7.5, 10d};
		
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		
//		StudyRotDProvider prov3D = getSimProv(study3D, siteNames, ampsCacheDir, periods, rd50_ims, vs30Source);
		
		List<CybershakeRun> runs3D = study3D.runFetcher().forSiteNames(siteNames).fetch();
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study3D, vs30Source, runs3D);
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study3D.getDB(), ampsCacheDir, study3D.getERF());
		StudyRotDProvider prov3D = new StudyRotDProvider(study3D, amps2db, periods, study3D.getName());
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
		
		Study3Dvs1DCompare comp = new Study3Dvs1DCompare(prov3D, prov1D, sites, vs30_1d, 150);
		
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
