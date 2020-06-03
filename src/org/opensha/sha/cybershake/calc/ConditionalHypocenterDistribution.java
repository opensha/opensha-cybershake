package org.opensha.sha.cybershake.calc;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSetMath;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;

public class ConditionalHypocenterDistribution {
	
	private static boolean debug_plots = false;
	private static boolean add_random_noise = false;
	
	private static boolean bundle_hypos_by_name = false;
	
	private ERF erf;
	private DBAccess db;
	private RuptureVariationProbabilityModifier mod;
	
	private RVHypoCache hypoCache;
	
	/*
	 * Table organized by sourceID, ruptureID to already cached variation probabilities
	 */
	private Table<Integer, Integer, List<Double>> varProbsCache = HashBasedTable.create();
	private String distName;
	
	public ConditionalHypocenterDistribution(ERF erf, DBAccess db, RealDistribution dist, String distName) {
		init(erf, db, new StandardMod(dist), distName);
	}
	
	public ConditionalHypocenterDistribution(ERF erf, DBAccess db,
			DiscretizedFunc func, String distName) {
		init(erf, db, new StandardMod(func), distName);
	}
	
	public ConditionalHypocenterDistribution(ERF erf, DBAccess db,
			RuptureVariationProbabilityModifier mod, String distName) {
		init(erf, db, mod, distName);
	}
	
	private void init(ERF erf, DBAccess db, RuptureVariationProbabilityModifier mod, String distName) {
		this.erf = erf;
		this.db = db;
		this.mod = mod;
		this.distName = distName;
	}
	
	private class StandardMod implements RuptureVariationProbabilityModifier {
		
		private RealDistribution dist;
		private DiscretizedFunc func;

		public StandardMod(RealDistribution dist) {
			this.dist = dist;
		}
		
		public StandardMod(DiscretizedFunc func) {
			this.func = func;
			Preconditions.checkArgument((float)func.getMinX() == 0f);
			Preconditions.checkArgument((float)func.getMaxX() == 1f);
		}
		
		@Override
		public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run, CybershakeIM im) {
			if (varProbsCache.contains(sourceID, rupID))
				return varProbsCache.get(sourceID, rupID);
			RuptureSurface surf = erf.getSource(sourceID).getRupture(rupID).getRuptureSurface();
			Preconditions.checkState(surf instanceof EvenlyGriddedSurface, "Must be evenly gridded surface");
			EvenlyGriddedSurface gridSurf = (EvenlyGriddedSurface)surf;
			
			// list of hypocenters ordered by RV ID
			List<Location> hypos = loadRVHypos(run, sourceID, rupID);
			
			// calculate DAS for each hypocenter
			List<Double> dasVals = calcDAS(hypos, gridSurf);
			
			if (debug_plots && sourceID == 128 && rupID == 1296)
				debugPlotDAS(dasVals, hypos, true);
			
			List<Double> hypocenterProbs = Lists.newArrayList();
			double sumHypoProbs = 0d;
			for (int rvIndex=0; rvIndex<dasVals.size(); rvIndex++) {
				double das = dasVals.get(rvIndex);
				double hypocenterProb;
				if (dist != null)
					hypocenterProb = dist.density(das);
				else
					hypocenterProb = func.getInterpolatedY(das);
				if (add_random_noise
//						&& Math.random() < 0.1
						) {
					double r = Math.random()-0.5;
					r *= (hypocenterProb*0.0001);
					double newProb = hypocenterProb + r;
					Preconditions.checkState(hypocenterProb != newProb);
					Preconditions.checkState(DataUtils.getPercentDiff(newProb, hypocenterProb) < 2d);
					hypocenterProb = newProb;
				}
				hypocenterProbs.add(hypocenterProb);
				sumHypoProbs += hypocenterProb;
			}
			
			// normalize to original probability
			for (int i=0; i<hypocenterProbs.size(); i++)
				hypocenterProbs.set(i, originalProb*hypocenterProbs.get(i)/sumHypoProbs);
			
			if (debug_plots && sourceID == 128 && rupID == 1296)
				debugPlotProbVsDAS(dasVals, hypocenterProbs);
			
//			Map<Double, List<Integer>> ret = Maps.newHashMap();
//			for (int rvID=0; rvID<hypocenterProbs.size(); rvID++) {
//				double hypocenterProb = hypocenterProbs.get(rvID);
//				hypocenterProb *= originalProb;
//				// test that hypo prob is uniform, only applicable when alpha=beta=1
////				double expected = (originalProb/(double)hypos.size());
////				Preconditions.checkState((float)hypocenterProb == (float)expected, hypocenterProb+" != "+expected);
//				
//				List<Integer> idsAtProb = ret.get(hypocenterProb);
//				if (idsAtProb == null) {
//					idsAtProb = Lists.newArrayList();
//					ret.put(hypocenterProb, idsAtProb);
//				}
//				idsAtProb.add(rvID);
//			}
//			
//			Map<Double, List<Integer>> fixedRet = Maps.newHashMap();
//			for (double prob : ret.keySet()) {
//				List<Integer> ids = ret.get(prob);
//				fixedRet.put(prob*(double)ids.size(), ids);
//			}
//			ret = fixedRet;
			
			// make sure the sum of all probabilities equals origProb
			double runningProb = 0d;
			for (double hypoProb : hypocenterProbs)
				runningProb += hypoProb;
			Preconditions.checkState((float)runningProb == (float)originalProb,
					"total probability doesn't equal original: "+runningProb+" != "+originalProb);
			
			synchronized (this) {
				varProbsCache.put(sourceID, rupID, hypocenterProbs);
			}
			
			return hypocenterProbs;
		}
	}
	
	private List<Location> loadRVHypos(CybershakeRun run, int sourceID, int rupID) {
		if (hypoCache == null)
			hypoCache = new RVHypoCache(db, run.getERFID(), run.getRupVarScenID());
		return hypoCache.loadRVHypos(sourceID, rupID);
	}
	
	private static class RVHypoCache {
		Table<Integer, Integer, List<Location>> cache;
		private DBAccess db;
		private int erfID;
		private int rvScenID;
		
		public RVHypoCache(DBAccess db, int erfID, int rvScenID) {
			this.db = db;
			this.erfID = erfID;
			this.rvScenID = rvScenID;
			
			cache = HashBasedTable.create();
		}
		
		public synchronized List<Location> loadRVHypos(int sourceID, int rupID) {
			List<Location> locs = cache.get(sourceID, rupID);
			
			if (locs == null) {
				String sql = "SELECT Rup_Var_ID,Hypocenter_Lat,Hypocenter_Lon,Hypocenter_Depth,Rup_Var_LFN FROM Rupture_Variations " +
						"WHERE ERF_ID=" + erfID + " AND Rup_Var_Scenario_ID=" + rvScenID + " " +
						"AND Source_ID=" + sourceID + " AND Rupture_ID=" + rupID;
				
				locs = new ArrayList<>();
				List<String> lfns = null;
				if (bundle_hypos_by_name)
					lfns = Lists.newArrayList();
				
				try {
					ResultSet rs = db.selectData(sql);
					boolean success = rs.next();
					while (success) {
						int rvID = rs.getInt("Rup_Var_ID");
						// make sure the list is big enough
						while (locs.size() <= rvID)
							locs.add(null);
						double lat = rs.getDouble("Hypocenter_Lat");
						double lon = rs.getDouble("Hypocenter_Lon");
						double depth = rs.getDouble("Hypocenter_Depth");
						if (lfns != null)
							lfns.add(rs.getString("Rup_Var_LFN"));
						Location loc = new Location(lat, lon, depth);
						
						locs.set(rvID, loc);

						success = rs.next();
					}
				} catch (SQLException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
				
				for (int i=0; i<locs.size(); i++) {
					Location loc = locs.get(i);
					Preconditions.checkNotNull(loc, "RV "+i+" has no hypo for rup with "+locs.size()+" RVs. SQL:\n\t"+sql);
				}
				
				if (bundle_hypos_by_name) {
					Map<String, List<Integer>> hypoBundles = Maps.newHashMap();
					for (int i=0; i<lfns.size(); i++) {
						String lfn = lfns.get(i);
						lfn = lfn.substring(lfn.indexOf("-h"));
						Preconditions.checkState(!lfn.isEmpty());
						List<Integer> ids = hypoBundles.get(lfn);
						if (ids == null) {
							ids = Lists.newArrayList();
							hypoBundles.put(lfn, ids);
						}
						ids.add(i);
					}
					for (String lfn : hypoBundles.keySet()) {
						double lat = 0d;
						double lon = 0d;
						double depth = 0d;
						int cnt = 0;
						for (int id : hypoBundles.get(lfn)) {
							Location loc = locs.get(id);
							lat += loc.getLatitude();
							lon += loc.getLongitude();
							depth += loc.getDepth();
							cnt++;
						}
						lat /= (double)cnt;
						lon /= (double)cnt;
						depth /= (double)cnt;
						Location loc = new Location(lat, lon, depth);
						for (int id : hypoBundles.get(lfn))
							locs.set(id, loc);
					}
				}
				
				cache.put(sourceID, rupID, locs);
			}
			
			return locs;
		}
	}
	
	private List<Double> calcDAS(List<Location> hypos, EvenlyGriddedSurface gridSurf) {
		int cols = gridSurf.getNumCols();
		
		List<Double> dasVals = Lists.newArrayList();
		for (Location loc : hypos) {
			double minDist = Double.POSITIVE_INFINITY;
			int closestColIndex = -1;
			for (int col=0; col<gridSurf.getNumCols(); col++) {
				double dist = LocationUtils.horzDistanceFast(loc, gridSurf.get(0, col));
				if (dist < minDist) {
					minDist = dist;
					closestColIndex = col;
				}
			}
			Preconditions.checkState(Doubles.isFinite(minDist));
			
			// find second closest for interpolation
			int secondClosestIndex = -1;
			double secondClosestDist = Double.POSITIVE_INFINITY;
			// check column before
			if (closestColIndex > 0) {
				secondClosestDist = LocationUtils.horzDistanceFast(loc, gridSurf.get(0, closestColIndex-1));
				secondClosestIndex = closestColIndex - 1;
			}
			if (closestColIndex + 1 < cols) {
				double dist = LocationUtils.horzDistanceFast(loc, gridSurf.get(0, closestColIndex+1));
				if (dist < secondClosestDist) {
					secondClosestDist = dist;
					secondClosestIndex = closestColIndex + 1;
				}
			}
			Preconditions.checkState(Doubles.isFinite(secondClosestDist));
			Preconditions.checkState(secondClosestDist >= minDist);
			
			// first calculate without interpolation
			double das = (double)closestColIndex;
			
			// now do interpolation
			double fractInterpolation = (minDist)/(minDist+secondClosestDist);
			
			if (closestColIndex < secondClosestIndex)
				das += fractInterpolation;
			else
				das -= fractInterpolation;
			
			// now normalize
			das /= (double)(cols-1);
			
			Preconditions.checkState(das >= 0 && das <= 1, "DAS outside of range [0 1]: "+das);
			
			dasVals.add(das);
		}
		return dasVals;
	}

	
	
	private void debugPlotProbVsDAS(List<Double> dasVals, List<Double> probs) {
		ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
		
		for (int i=0; i<dasVals.size(); i++) {
			double das = dasVals.get(i);
			double prob = probs.get(i);
			func.set(das, prob);
		}
		
		List<DiscretizedFunc> funcs = Lists.newArrayList();
		funcs.add(func);
		List<PlotCurveCharacterstics> chars = Lists.newArrayList(
				new PlotCurveCharacterstics(PlotLineType.SOLID, 1f, PlotSymbol.CROSS, 4f, Color.BLACK));
		PlotSpec spec = new PlotSpec(funcs, chars, "Prob vs DAS", "DAS", "Prob");
		new GraphWindow(spec);
	}
	
	private void debugPlotDAS(List<Double> dasVals, List<Location> hypos, boolean latitude) {
		ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
		
		for (int i=0; i<dasVals.size(); i++) {
			double das = dasVals.get(i);
			Location hypo = hypos.get(i);
			double x;
			if (latitude)
				x = hypo.getLatitude();
			else
				x = hypo.getLongitude();
			func.set(x, das);
		}
		
		List<DiscretizedFunc> funcs = Lists.newArrayList();
		funcs.add(func);
		List<PlotCurveCharacterstics> chars = Lists.newArrayList(
				new PlotCurveCharacterstics(PlotLineType.SOLID, 1f, PlotSymbol.CROSS, 4f, Color.BLACK));
		String xLabel;
		if (latitude)
			xLabel = "Latitude";
		else
			xLabel = "Longitude";
		PlotSpec spec = new PlotSpec(funcs, chars, "DAS vs Hypo Loc", xLabel, "DAS");
		new GraphWindow(spec);
	}
	
	public void plotDist(File outputDir, String prefix) throws IOException {
		Preconditions.checkState(mod instanceof StandardMod, "Can only plot starndard mods");
		StandardMod smod = (StandardMod)mod;
		DiscretizedFunc uniformFunc = new ArbitrarilyDiscretizedFunc();
		uniformFunc.setName("Uniform");
		uniformFunc.set(0d, 1d);
		uniformFunc.set(1d, 1d);
		
		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		funcs.add(uniformFunc);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 3f, Color.GRAY));
		
		DiscretizedFunc func;
		if (smod.func == null) {
			func = new EvenlyDiscretizedFunc(0d, 1d, 100);
			for (int i=0; i<func.size(); i++)
				func.set(i, smod.dist.density(func.getX(i)));
		} else {
			func = smod.func.deepClone();
		}
		func.setName(distName);
		
		funcs.add(func);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLACK));
		
		PlotSpec spec = new PlotSpec(funcs, chars, "Conditional Hypocenter Distribution",
				"Fractional Distance Along Strike", "Density");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		gp.setBackgroundColor(Color.WHITE);
		
		gp.setUserBounds(new Range(0d, 1d), new Range(0d, Math.max(2, 1.1*func.getMaxY())));
		gp.drawGraphPanel(spec, false, false);
		gp.getChartPanel().setSize(1000, 800);
		File outputFile = new File(outputDir, prefix);
		gp.saveAsPNG(outputFile.getAbsolutePath()+".png");
		gp.saveAsPDF(outputFile.getAbsolutePath()+".pdf");
	}
	
	private static class DistStepFuncMod implements RuptureVariationProbabilityModifier {
		
		private Map<Integer, Location> siteLocMap;
		
		private SiteInfo2DB sites2db;
		private RVHypoCache hypoCache;

		private DiscretizedFunc relDistProbFunc;
		
		private DistStepFuncMod(DiscretizedFunc relDistProbFunc, DBAccess db, RVHypoCache hypoCache) {
			this.relDistProbFunc = relDistProbFunc;
			this.hypoCache = hypoCache;
			sites2db = new SiteInfo2DB(db);
			
			siteLocMap = new HashMap<>();
		}

		@Override
		public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run,
				CybershakeIM im) {
			Location siteLoc = siteLocMap.get(run.getSiteID());
			if (siteLoc == null) {
				siteLoc = sites2db.getLocationForSiteID(run.getSiteID());
				siteLocMap.put(run.getSiteID(), siteLoc);
			}
			
			double minDist = Double.POSITIVE_INFINITY;
			double maxDist = 0d;
			
			List<Location> hypos = hypoCache.loadRVHypos(sourceID, rupID);
			List<Double> dists = new ArrayList<>();
			
			for (Location hypo : hypos) {
				double dist = LocationUtils.horzDistanceFast(hypo, siteLoc);
				dists.add(dist);
				minDist = Math.min(minDist, dist);
				maxDist = Math.max(maxDist, dist);
			}
			
			double probEach = originalProb/(double)dists.size();
			double totProb = 0d;
			List<Double> modProbs = new ArrayList<>();
			
			for (double dist : dists) {
				double relDist = (dist-minDist)/(maxDist-minDist);
				double prob = probEach*relDistProbFunc.getInterpolatedY(relDist);
				totProb += prob;
				modProbs.add(prob);
			}
			
			double scale = originalProb/totProb;
			for (int i=0; i<modProbs.size(); i++)
				modProbs.set(i, scale*modProbs.get(i));
			
			return modProbs;
		}
		
	}
	
	private static DiscretizedFunc getCustomFunc() {
		// from Jessica via e-mail "Re: CyberShake hazard curves" 6/2/15
		ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
		
		func.set(0d,0.638655462185526);
		func.set(0.0526315789473684,0.638655462185692);
		func.set(0.105263157894737,0.638655488376696);
		func.set(0.157894736842105,0.638672707923049);
		func.set(0.210526315789474,0.639805013114497);
		func.set(0.263157894736842,0.659570892181832);
		func.set(0.315789473684211,0.798958630903336);
		func.set(0.368421052631579,0.846212807747123);
		func.set(0.421052631578947,1.75155788103647);
		func.set(0.473684210526316,2.56858338543854);
		func.set(0.526315789473684,2.56858338543854);
		func.set(0.578947368421053,1.75155788103647);
		func.set(0.631578947368421,0.846212807747125);
		func.set(0.684210526315790,0.798958630903336);
		func.set(0.736842105263158,0.659570892181832);
		func.set(0.789473684210526,0.639805013114497);
		func.set(0.842105263157895,0.638672707923049);
		func.set(0.894736842105263,0.638655488376696);
		func.set(0.947368421052632,0.638655462185692);
		func.set(1d,0.638655462185526);
		
		return func;
	}

	public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException, IOException, GMT_MapException, SQLException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		HardCodedInterpDiffMapCreator.gmpe_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);;
		HardCodedInterpDiffMapCreator.normPlotTypes = new InterpDiffMapType[] {
				InterpDiffMapType.INTERP_NOMARKS
		};
		HardCodedInterpDiffMapCreator.gainPlotTypes = new InterpDiffMapType[] {
				InterpDiffMapType.INTERP_NOMARKS
		};
		File gitDir = new File("/home/kevin/markdown/cybershake-analysis");
		
		File studyDir = new File(gitDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		DBAccess db = study.getDB();
		ERF erf = study.getERF();
		
//		double alpha = 10.03;
//		double beta = 10.03;
//		BetaDistribution dist = new BetaDistribution(alpha, beta);
//		String distName = "Beta("+(float)alpha+","+(float)beta+")";
//		String distDesc = "Beta distribution with &alpha;="+(float)alpha+" and &beta;="+(float)beta;
//		String dirName = "chd_betadist_a"+(float)alpha+"_b"+(float)beta;
//		ConditionalHypocenterDistribution mod = new ConditionalHypocenterDistribution(erf, db, dist, distName);
		
		DiscretizedFunc stepFunc = new ArbitrarilyDiscretizedFunc();
		int factor = 5;
		stepFunc.set(0d, 1d/(double)factor);
		stepFunc.set(0.33d, 1d/(double)factor);
		stepFunc.set(0.34d, 1d);
		stepFunc.set(0.66d, 1d);
		stepFunc.set(0.67d, (double)factor);
		stepFunc.set(1d, (double)factor);
		CybershakeRun run0 = study.runFetcher().fetch().get(0);
		RVHypoCache hypoCache = new RVHypoCache(db, run0.getERFID(), run0.getRupVarScenID());
		DistStepFuncMod funcMod = new DistStepFuncMod(stepFunc, db, hypoCache);
		String distName = "Furthest "+factor+"x As Likely";
		String distDesc = "step function applied uniquely to each site/source pair where the furthest third of the hypocenters "
				+ "(from the site) are "+factor+"x more likely, closest third 1/"+factor+" as likely, and the center third are unchanged";
		String dirName = "chd_furthest_"+factor+"x";
		ConditionalHypocenterDistribution mod = new ConditionalHypocenterDistribution(erf, db, funcMod, distName);
		mod.hypoCache = hypoCache;

		boolean replotMaps = false;
		
//		CyberShakeComponent component = CyberShakeComponent.GEOM_MEAN;
//		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
		CyberShakeComponent component = CyberShakeComponent.RotD50;
		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		HardCodedInterpDiffMapCreator.setTruncation(baseMapGMPE, 3.0);
		
		List<String> lines = new ArrayList<>();
		
		lines.add("# Conditional Hypocenter Distribution Calculations");
		lines.add("");
		lines.add("This page modifies CyberShake "+study.getName()+" with a non-uniform conditional hypocenter distribution (CHD). "
				+ "That distribution is parameterized with a "+distDesc+".");
		lines.add("");
		
		File outputDir = new File(studyDir, dirName);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		List<String> curvePlotSites = Lists.newArrayList("USC", "SBSM", "STNI", "PACI2", "LAPD");
		
		CybershakeIM[] ims = {
				CybershakeIM.getSA(component, 3d),
				CybershakeIM.getSA(component, 5d),
				CybershakeIM.getSA(component, 10d)
		};
		
		List<Double> levels = new ArrayList<>();
		List<Boolean> isProbAtIMLs = new ArrayList<>();
		
		levels.add(4e-4);
		isProbAtIMLs.add(false);
		
		levels.add(0.1);
		isProbAtIMLs.add(true);
		
		levels.add(0.2);
		isProbAtIMLs.add(true);
		
		HazardCurveComputation calc = new HazardCurveComputation(db);
		// cached peak amps for quick calculation
		calc.setPeakAmpsAccessor(new CachedPeakAmplitudesFromDB(db,
				new File("/home/kevin/CyberShake/MCER/.amps_cache"), erf));
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		
		DiscretizedFunc xValsFunc = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		List<Double> xVals = Lists.newArrayList();
		for (Point2D pt : xValsFunc)
			xVals.add(pt.getX());
		
		if (mod.mod instanceof StandardMod) {
			mod.plotDist(resourcesDir, "chd_plot");
			lines.add("![CHD](resources/chd_plot.png)");
			lines.add("");
		}
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		List<List<GeoDataSet>> scatters = new ArrayList<>();
		List<List<GeoDataSet>> origScatters = new ArrayList<>();
		
		TableBuilder curveTable = MarkdownUtils.tableBuilder();
		curveTable.initNewLine();
		curveTable.addColumn("Site Name");
		for (CybershakeIM im : ims)
			curveTable.addColumn((int)im.getVal()+"s SA");
		curveTable.finalizeLine();
		
		for (int i=0; i<ims.length; i++) {
			scatters.add(new ArrayList<>());
			origScatters.add(new ArrayList<>());
			for (int j=0; j<levels.size(); j++) {
				scatters.get(i).add(new ArbDiscrGeoDataSet(true));
				origScatters.get(i).add(new ArbDiscrGeoDataSet(true));
			}
		}
		
		for (int i=0; i<sites.size(); i++) {
			CyberShakeSiteRun site = (CyberShakeSiteRun)sites.get(i);
			
			if (curvePlotSites.contains(site.getName())) {
				curveTable.initNewLine();
				curveTable.addColumn("**"+site.getName()+"**");
			}
			for (int j=0; j<ims.length; j++) {
				CybershakeIM im = ims[j];
				
				calc.setRupVarProbModifier(mod.mod);
				DiscretizedFunc curve = calc.computeHazardCurve(xVals, site.getCS_Run(), im);
				calc.setRupVarProbModifier(null);
				DiscretizedFunc origCurve = calc.computeHazardCurve(xVals, site.getCS_Run(), im);
				Location loc = site.getLocation();
				
				for (int k=0; k<levels.size(); k++) {
					double val = levels.get(k);
					boolean isProbAt_IML = isProbAtIMLs.get(k);
					double newVal = HazardDataSetLoader.getCurveVal(curve, isProbAt_IML, val);
					double origVal = HazardDataSetLoader.getCurveVal(origCurve, isProbAt_IML, val);
					
					scatters.get(j).get(k).set(loc, newVal);
					origScatters.get(j).get(k).set(loc, origVal);
				}
				
				String xAxisLabel = (int)im.getVal()+"s SA";
				
				if (curvePlotSites.contains(site.getName())) {
					System.out.println("Plotting curves for "+site.getName());
					// plot curves
					List<DiscretizedFunc> curves = Lists.newArrayList();
					List<PlotCurveCharacterstics> chars = Lists.newArrayList();
					
					curves.add(origCurve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, PlotSymbol.FILLED_SQUARE, 4f, Color.BLACK));
					origCurve.setName("Original");
					curves.add(curve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLUE));
					curve.setName("Cond. Hypo. Modified");
					
					PlotSpec spec = new PlotSpec(curves, chars, site.getName(), xAxisLabel, "Exceed. Prob");
					spec.setLegendVisible(true);
					
					HeadlessGraphPanel gp = new HeadlessGraphPanel();
					gp.setTickLabelFontSize(18);
					gp.setAxisLabelFontSize(20);
					gp.setPlotLabelFontSize(21);
					gp.setBackgroundColor(Color.WHITE);
					
					gp.setUserBounds(new Range(1e-3, 1e1), new Range(1e-8, 1));
					gp.drawGraphPanel(spec, true, true);
//					gp.setUserBounds(new Range(0, 2), new Range(1.0E-6, 1));
//					gp.drawGraphPanel(spec, false, true);
					gp.getChartPanel().setSize(1000, 800);
					File outputFile = new File(resourcesDir, site.getName()+"_comparison_"+(int)im.getVal()+"s");
					gp.saveAsPNG(outputFile.getAbsolutePath()+".png");
					gp.saveAsPDF(outputFile.getAbsolutePath()+".pdf");
					curveTable.addColumn("![curve]("+resourcesDir.getName()+"/"+outputFile.getName()+".png)");
				}
			}
			if (curvePlotSites.contains(site.getName()))
				curveTable.finalizeLine();
		}
		
		boolean logPlot = true;
		
		// maps
		lines.add("# Maps");
		lines.add(topLink); lines.add("");
		
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		
		for (int l=0; l<levels.size(); l++) {
			double level = levels.get(l);
			boolean isProbAtIML = isProbAtIMLs.get(l);
			
			String label, prefix;
			if (isProbAtIML) {
				label = "POE "+(float)level+"g";
				prefix = "map_poe_"+(float)level;
			} else {
				if (level == 4e-4) {
					label = "2% in 50yr";
					prefix = "map_2p_in_50";
				} else {
					label = "IML @ P="+(float)level+"";
					prefix = "map_iml_at_"+(float)level;
				}
			}
			
			lines.add("## "+label+" Maps");
			lines.add(topLink); lines.add("");
			
			TableBuilder mapTable = MarkdownUtils.tableBuilder();
			mapTable.initNewLine();
			mapTable.addColumn(" ");
			for (CybershakeIM im : ims)
				mapTable.addColumn((int)im.getVal()+"s SA");
			mapTable.finalizeLine();
			
			String[][] plots = new String[4][ims.length];
			
			for (int j=0; j<ims.length; j++) {
				GeoDataSet scatter = scatters.get(j).get(l);
				GeoDataSet origScatter = origScatters.get(j).get(l);
				
				CybershakeIM im = ims[j];
				String imLabel = label+", "+(int)im.getVal()+"s SA";
				String imPrefix = prefix+"_"+(int)im.getVal()+"s";

				double minVal = Double.POSITIVE_INFINITY;
				double maxVal = 0d;
				
				// now deal with infinities
				if (logPlot) {
					for (int i=0; i<scatter.size(); i++) {
						double val = scatter.get(i);
						if (val < 1e-10 || !Double.isFinite(val))
							scatter.set(i, 1e-10);
						else
							minVal = Math.min(minVal, val);
						maxVal = Math.max(maxVal, val);
					}
					for (int i=0; i<origScatter.size(); i++) {
						double val = origScatter.get(i);
						if (val < 1e-10 || !Double.isFinite(val))
							origScatter.set(i, 1e-10);
						else
							minVal = Math.min(minVal, val);
						maxVal = Math.max(maxVal, val);
					}
					minVal = Math.floor(Math.log10(minVal));
					maxVal = Math.ceil(Math.log10(maxVal));
				} else {
					minVal = 0d;
					maxVal = Math.max(scatter.getMaxZ(), origScatter.getMaxZ());
				}
				
				String addr;
				plots[0][j] = imPrefix+"_orig_chd.png";
				if (replotMaps || !new File(resourcesDir, plots[0][j]).exists()) {
					System.out.println("Orig:");
					try {
						addr = HardCodedInterpDiffMapCreator.getMap(region, origScatter, logPlot, study.getVelocityModelID(), im.getID(),
								minVal, maxVal, isProbAtIML, level, baseMapGMPE, false, "Original Map, "+imLabel);
					} catch (Exception e) {
						e.printStackTrace();
						System.err.flush();
						System.out.println("Disabling GMPE for "+im);
						addr = HardCodedInterpDiffMapCreator.getMap(region, origScatter, logPlot, study.getVelocityModelID(), im.getID(),
								minVal, maxVal, isProbAtIML, level, null, false, "Original Map, "+imLabel);
					}
					FileUtils.downloadURL(addr+"/interpolated.150.png", new File(resourcesDir, imPrefix+"_orig_chd.png"));
				}
				
				plots[1][j] = imPrefix+"_mod_chd.png";
				if (replotMaps || !new File(resourcesDir, plots[1][j]).exists()) {
					System.out.println("Modified:");
					try {
						addr = HardCodedInterpDiffMapCreator.getMap(region, scatter, logPlot, study.getVelocityModelID(), im.getID(),
								minVal, maxVal, isProbAtIML, level, baseMapGMPE, false, "Cond Prob Modified, "+imLabel);
					} catch (Exception e) {
						addr = HardCodedInterpDiffMapCreator.getMap(region, scatter, logPlot, study.getVelocityModelID(), im.getID(),
								minVal, maxVal, isProbAtIML, level, null, false, "Cond Prob Modified, "+imLabel);
					}
					FileUtils.downloadURL(addr+"/interpolated.150.png", new File(resourcesDir, imPrefix+"_mod_chd.png"));
				}
				
				// now ratio
				plots[2][j] = imPrefix+"_diff.png";
				plots[3][j] = imPrefix+"_ratio.png";
				if (replotMaps || !new File(resourcesDir, plots[2][j]).exists() || !new File(resourcesDir, plots[3][j]).exists()) {
					String[] addrs = HardCodedInterpDiffMapCreator.getCompareMap(
							false, scatter, origScatter, "Cond. Hypo. Dist, "+imLabel, true, region);
					FileUtils.downloadURL(addrs[0]+"/interpolated.150.png", new File(resourcesDir, imPrefix+"_diff.png"));
					FileUtils.downloadURL(addrs[1]+"/interpolated.150.png", new File(resourcesDir, imPrefix+"_ratio.png"));
				}
			}
			
			mapTable.initNewLine();
			mapTable.addColumn("**Uniform CHD**");
			for (String plot : plots[0])
				mapTable.addColumn("![map]("+resourcesDir.getName()+"/"+plot+")");
			mapTable.finalizeLine();
			
			mapTable.initNewLine();
			mapTable.addColumn("**Modified CHD**");
			for (String plot : plots[1])
				mapTable.addColumn("![map]("+resourcesDir.getName()+"/"+plot+")");
			mapTable.finalizeLine();
			
			mapTable.initNewLine();
			mapTable.addColumn("**Difference Mod-Uni**");
			for (String plot : plots[2])
				mapTable.addColumn("![map]("+resourcesDir.getName()+"/"+plot+")");
			mapTable.finalizeLine();
			
			mapTable.initNewLine();
			mapTable.addColumn("**Ratio Mod/Uni**");
			for (String plot : plots[3])
				mapTable.addColumn("![map]("+resourcesDir.getName()+"/"+plot+")");
			mapTable.finalizeLine();
			
			lines.addAll(mapTable.build());
			lines.add("");
		}
		
		// maps
		lines.add("# Hazard Curves");
		lines.add(topLink); lines.add("");
		
		lines.addAll(curveTable.build());
		lines.add("");
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 1, 3));
		lines.add(tocIndex, "## Table Of Contents");
		
		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
		
		db.destroy();
		HardCodedInterpDiffMapCreator.gmpe_db.destroy();
		System.exit(0);
	}

}
