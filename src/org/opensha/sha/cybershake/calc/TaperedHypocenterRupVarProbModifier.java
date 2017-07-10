package org.opensha.sha.cybershake.calc;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.data.xyz.EvenlyDiscrXYZ_DataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotElement;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZPlotSpec;
import org.opensha.commons.gui.plot.jfreechart.xyzPlot.XYZPlotWindow;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.InterpolatedEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

public class TaperedHypocenterRupVarProbModifier implements
		RuptureVariationProbabilityModifier {
	
	// make sure that all hypocenters are within this distance of the closest surface point
	private static final double surface_distance_max = 1.5d;
	
	// if true, use the fast closest surf point algorithm
	private static final boolean loc_map_fast = true;
	
	public enum TaperType {
		ROB_PREFERRED(new Taper(0.2, 0.1, 0.8, 0.1), new Taper(0.4, 0.01, 0.8, 0.1)),
		TEST_HIGH_UPPER(null, new Taper(0.01, 1.0, 0.3, 0.01)),
		TEST_HIGH_LOWER(null, new Taper(0.7, 0.01, 0.99, 1.0));
		
		private Taper xTaper;
		private Taper yTaper;
		
		private TaperType(Taper xTaper, Taper yTaper) {
			this.xTaper = xTaper;
			this.yTaper = yTaper;
		}
	}
	
	public static class Taper {
		double beginLoc, beginVal, endLoc, endVal;
		public Taper(double beginLoc, double beginVal, double endLoc, double endVal) {
			this.beginLoc = beginLoc;
			this.beginVal = beginVal;
			this.endLoc = endLoc;
			this.endVal = endVal;
		}
	}
	
	private TaperType taperType;
	
	private ERF erf;
	private DBAccess db;
	
	private LoadingCache<PointCacheKey, List<Point2D>> pointCache;
	private LoadingCache<HypoCacheKey, List<List<Location>>> hypoCache;

	
	private static double maxLocDiscrepancyEncountered = 0d;
	
	public TaperedHypocenterRupVarProbModifier(TaperType taperType, ERF erf, DBAccess db) {
		this.taperType = taperType;
		this.erf = erf;
		this.db = db;
		this.pointCache = CacheBuilder.newBuilder().maximumSize(100000).build(new PointCacheLoader());
		this.hypoCache = CacheBuilder.newBuilder().maximumSize(500).build(new HypoCacheLoader());
	}

	@Override
	public List<Double> getVariationProbs(int sourceID, int rupID,
			double originalProb, CybershakeRun run, CybershakeIM im) {
		List<Point2D> points;
		try {
			points = pointCache.get(new PointCacheKey(sourceID, rupID, run));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		List<Double> probs = Lists.newArrayList();
		double sumProbs = 0d;
		for (Point2D point : points) {
			double prob = getRelativeProb(taperType, point.getX(), point.getY());
			sumProbs += prob;
			probs.add(prob);
		}
		// now rescale to original prob
		double scalar = originalProb/sumProbs;
		for (int i=0; i<probs.size(); i++)
			probs.set(i, probs.get(i)*scalar);
		return probs;
	}
	
	private static double getRelativeProb(TaperType taperType, double x, double y) {
		double xTaper = getTaper(x, taperType.xTaper);
		double yTaper = getTaper(y, taperType.yTaper);
		return xTaper * yTaper;
	}
	
	private static double getTaper(double val, Taper taper) {
		if (taper == null)
			return 1d;
		if (val < taper.beginLoc)
			return taper.beginVal + (val/taper.beginLoc)*(1d-taper.beginVal);
		else if (val > taper.endLoc)
			return taper.endVal + ((1d-val)/(1d-taper.endLoc))*(1d-taper.endVal);
		return 1d;
	}
	
	private class HypoCacheKey {
		private final int erfID, sourceID, rvID;

		public HypoCacheKey(int erfID, int sourceID, int rvID) {
			super();
			this.erfID = erfID;
			this.sourceID = sourceID;
			this.rvID = rvID;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + erfID;
			result = prime * result + rvID;
			result = prime * result + sourceID;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			HypoCacheKey other = (HypoCacheKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (erfID != other.erfID)
				return false;
			if (rvID != other.rvID)
				return false;
			if (sourceID != other.sourceID)
				return false;
			return true;
		}

		private TaperedHypocenterRupVarProbModifier getOuterType() {
			return TaperedHypocenterRupVarProbModifier.this;
		}
	}
	
	/**
	 * Key for accessing point on rupture surface cache
	 * @author kevin
	 *
	 */
	private class PointCacheKey {
		private final int erfID, sourceID, rupID, rvID;
		
		public PointCacheKey(int sourceID, int rupID, CybershakeRun run) {
			this.erfID = run.getERFID();
			this.sourceID = sourceID;
			this.rupID = rupID;
			this.rvID = run.getRupVarScenID();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + erfID;
			result = prime * result + rupID;
			result = prime * result + rvID;
			result = prime * result + sourceID;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PointCacheKey other = (PointCacheKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (erfID != other.erfID)
				return false;
			if (rupID != other.rupID)
				return false;
			if (rvID != other.rvID)
				return false;
			if (sourceID != other.sourceID)
				return false;
			return true;
		}

		private TaperedHypocenterRupVarProbModifier getOuterType() {
			return TaperedHypocenterRupVarProbModifier.this;
		}
	}
	
	private class PointCacheLoader extends CacheLoader<PointCacheKey, List<Point2D>> {

		@Override
		public List<Point2D> load(PointCacheKey key) throws Exception {
			RuptureSurface surf = getRuptureSurface(key.sourceID, key.rupID);
			List<Location> hypos = getHypocenters(key.sourceID, key.rupID, key.erfID, key.rvID);
			
			return getRelativeLocations(surf, hypos);
		}
	}
	
	private RuptureSurface getRuptureSurface(int sourceID, int rupID) {
		return erf.getRupture(sourceID, rupID).getRuptureSurface();
	}
	
	private class HypoCacheLoader extends CacheLoader<HypoCacheKey, List<List<Location>>> {

		@Override
		public List<List<Location>> load(HypoCacheKey key) throws Exception {
			String sql = "SELECT Rupture_ID,Rup_Var_ID,Hypocenter_Lat,Hypocenter_Lon,Hypocenter_Depth FROM Rupture_Variations " +
					"WHERE ERF_ID=" + key.erfID + " AND Rup_Var_Scenario_ID=" + key.rvID + " " +
					"AND Source_ID=" + key.sourceID + " ORDER BY Rupture_ID, Rup_Var_ID";

			List<List<Location>> ret = Lists.newArrayList();
			try {
				ResultSet rs = db.selectData(sql);

				boolean success = rs.first();
				while (success) {
					int rupID = rs.getInt("Rupture_ID");
					int rvID = rs.getInt("Rup_Var_ID");
					double lat = rs.getDouble("Hypocenter_Lat");
					double lon = rs.getDouble("Hypocenter_Lon");
					double depth = rs.getDouble("Hypocenter_Depth");
					Location loc = new Location(lat, lon, depth);
					
					while (ret.size() <= rupID)
						ret.add(new ArrayList<Location>());
					
					List<Location> locs = ret.get(rupID);
					Preconditions.checkState(locs.size() == rvID);

					locs.add(loc);

					success = rs.next();
				}
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			return ret;
		}
		
	}
	
	private synchronized List<Location> getHypocenters(int sourceID, int rupID, int erfID, int rvScenID) {
		try {
			return hypoCache.get(new HypoCacheKey(erfID, sourceID, rvScenID)).get(rupID);
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}
	
	private List<Point2D> getRelativeLocations(RuptureSurface surf, List<Location> locs) {
		Preconditions.checkState(surf instanceof EvenlyGriddedSurface,
				"Currently only implemented for EvenlyGirddedSurfaces. Compound (UCERF3) "
				+"surfaces will take more thought. Like do we taper the whole rupture, or each "
				+"individual fault?");
		
		EvenlyGriddedSurface gridSurf = (EvenlyGriddedSurface)surf;
		int numCols = gridSurf.getNumCols();
		int numRows = gridSurf.getNumRows();
		
		List<Point2D> points = Lists.newArrayList();
		
		for (Location loc : locs) {
			LocInSurfResult locInSurf;
			if (loc_map_fast)
				locInSurf = findLocInSurfFast(gridSurf, loc);
			else
				locInSurf = findLocInSurfSlow(gridSurf, loc);
			
			double x = indexToFractDist(locInSurf.boundingStartCol+locInSurf.getRelativeLocX(), numCols);
			double y = indexToFractDist(locInSurf.boundingStartRow+locInSurf.getRelativeLocY(), numRows);
			Preconditions.checkState(Doubles.isFinite(x) && x >= 0d && x <= 1d, "Invalid X: %s", x);
			Preconditions.checkState(Doubles.isFinite(y) && y >= 0d && y <= 1d, "Invalid Y: %s", y);
			
			points.add(new Point2D.Double(x, y));
		}
		
		// sanity test TODO
//		for (int i=0; i<locs.size(); i++) {
//			Location loc = locs.get(i);
//			Point2D pt = points.get(i);
//			EvenlyGriddedSurface hiResSurf = testSurfaces[testSurfaces.length-1];
//			int row = distToIndex(pt.getY(), hiResSurf.getNumRows());
//			int col = distToIndex(pt.getX(), hiResSurf.getNumCols());
//			Location test = hiResSurf.get(row, col);
//			double dist = LocationUtils.linearDistance(loc, test);
//			Preconditions.checkState(dist <= surface_distance_max,
//					"Hypocenter location mapping outside of tolerance. Dist=%s km\n\tHypo: %s\n\tClosest: %s",
//					dist, loc, test);
//			
//			maxLocDiscrepancyEncountered = Math.max(maxLocDiscrepancyEncountered, dist);
//		}
		
//		// now oversample the rupture surface in order to map locations
//		InterpolatedEvenlyGriddedSurface interpSurf = new InterpolatedEvenlyGriddedSurface(
//				(EvenlyGriddedSurface)surf, surface_oversampling_resolution);
//		
//		List<Point2D> points = Lists.newArrayList();
//		
//		for (Location loc : locs) {
//			if (loc_map_fast)
//				points.add(findClosestSurfPointFast(interpSurf, loc));
//			else
//				points.add(findClosestSurfPointSlow(interpSurf, loc));
//		}
		
		return points;
	}
	
	private static double indexToFractDist(double index, int num) {
		return index/(double)(num-1);
	}
	
	private static int distToIndex(double dist, int num) {
		return (int)(dist*(num - 1d) + 0.5);
	}
	
	private static class LocInSurfResult {
		
		EvenlyGriddedSurface surf;
		Location loc;
		
		int boundingStartRow;
		int boundingStartCol;
		
		public LocInSurfResult(int closestRow, int closestCol, EvenlyGriddedSurface surf, Location loc) {
			this.surf = surf;
			this.loc = loc;
			
			int numCols = surf.getNumCols();
			int numRows = surf.getNumRows();
			
			int colBefore;
			if (closestCol == 0) {
				colBefore = 0;
			} else if (closestCol == numCols-1) {
				colBefore = numCols-2;
			} else {
				double distBefore = LocationUtils.horzDistanceFast(loc, surf.get(closestRow, closestCol-1));
				double distAfter = LocationUtils.horzDistanceFast(loc, surf.get(closestRow, closestCol+1));
				if (distBefore > distAfter)
					colBefore = closestCol;
				else
					colBefore = closestCol-1;
			}
			int rowBefore;
			if (closestRow == 0) {
				rowBefore = 0;
			} else if (closestRow == numRows-1) {
				rowBefore = numRows-2;
			} else {
				double distBefore = LocationUtils.vertDistance(loc, surf.get(closestRow-1, closestCol));
				double distAfter = LocationUtils.vertDistance(loc, surf.get(closestRow+1, closestCol));
				if (distBefore > distAfter)
					rowBefore = closestRow;
				else
					rowBefore = closestRow-1;
			}
			
			this.boundingStartRow = rowBefore;
			this.boundingStartCol = colBefore;
		}
		
		public double getRelativeLocX() {
			double dBefore = LocationUtils.horzDistanceFast(loc, surf.get(boundingStartRow, boundingStartCol));
			double dAfter = LocationUtils.horzDistanceFast(loc, surf.get(boundingStartRow, boundingStartCol+1));
			double x = dBefore/(dBefore+dAfter);
			Preconditions.checkState(x >= 0d && x <= 1d);
			return x;
		}
		
		public double getRelativeLocY() {
			double dBefore = Math.abs(LocationUtils.vertDistance(loc, surf.get(boundingStartRow, boundingStartCol)));
			double dAfter = Math.abs(LocationUtils.vertDistance(loc, surf.get(boundingStartRow+1, boundingStartCol)));
			double y = dBefore/(dBefore+dAfter);
			Preconditions.checkState(y >= 0d && y <= 1d, "Bad Y calc. dBefore=%s, dAfter=%s, y=%s", dBefore, dAfter, y);
			return y;
		}
	}
	
	/**
	 * This finds the point on the rupture surface by first searching by row then by column
	 * @param surf
	 * @param loc
	 * @return
	 */
	private static LocInSurfResult findLocInSurfFast(EvenlyGriddedSurface surf, Location loc) {
		int numCols = surf.getNumCols();
		int numRows = surf.getNumRows();
		
		// first find the correct depth
		double hypoDepth = loc.getDepth();
		int closestRow = -1;
		double closestRowDepthDiff = Double.POSITIVE_INFINITY;
		for (int row=0; row<numRows; row++) {
			if (row < 0)
				continue;
			if (row == numRows)
				break;
			double rowDepth = surf.get(row, 0).getDepth();
			double diff = Math.abs(hypoDepth - rowDepth);
			if (diff < closestRowDepthDiff) {
				closestRow = row;
				closestRowDepthDiff = diff;
			}
		}

		// now find the column
		int closestCol = -1;
		double closestColDepthDist = Double.POSITIVE_INFINITY;
		for (int col=0; col<numCols; col++) {
			if (col < 0)
				continue;
			if (col == numCols)
				break;
			double dist = LocationUtils.linearDistanceFast(loc, surf.get(closestRow, col));
			if (dist < closestColDepthDist) {
				closestCol = col;
				closestColDepthDist = dist;
			}
		}
		
		double minDist = closestColDepthDist;
		
		Preconditions.checkState(minDist <= surface_distance_max,
				"Hypocenter location mapping outside of tolerance. Dist=%s km", minDist);
		
		maxLocDiscrepancyEncountered = Math.max(maxLocDiscrepancyEncountered, minDist);
		
		return new LocInSurfResult(closestRow, closestCol, surf, loc);
	}
	
	/**
	 * This finds the point on the rupture surface by brute force checking every surface loc
	 * @param surf
	 * @param loc
	 * @return
	 */
	private static LocInSurfResult findLocInSurfSlow(EvenlyGriddedSurface surf, Location loc) {
		int numCols = surf.getNumCols();
		int numRows = surf.getNumRows();
		
		double minDist = Double.POSITIVE_INFINITY;
		int closestCol = -1;
		int closestRow = -1;
		
		for (int row=0; row<numRows; row++) {
			for (int col=0; col<numCols; col++) {
				double dist = LocationUtils.linearDistanceFast(loc, surf.get(row, col));
				if (dist < minDist) {
					minDist = dist;
					closestRow = row;
					closestCol = col;
				}
			}
		}
		
		Preconditions.checkState(minDist <= surface_distance_max,
				"Hypocenter location mapping outside of tolerance. Dist=%s km", minDist);
		
		maxLocDiscrepancyEncountered = Math.max(maxLocDiscrepancyEncountered, minDist);
		
		return new LocInSurfResult(closestRow, closestCol, surf, loc);
	}
	
	private static List<Location> loadInputHypos(File inputHypoFile) throws IOException {
		List<Location> locs = Lists.newArrayList();
		for (String line : Files.readLines(inputHypoFile, Charset.defaultCharset())) {
			if (!line.startsWith("lat="))
				continue;
			line = line.trim().replaceAll(" ", "");
			String[] split = line.split(",");
			double lat = Double.parseDouble(split[0].substring(split[0].indexOf("=")+1));
			double lon = Double.parseDouble(split[1].substring(split[1].indexOf("=")+1));
			double dep = Double.parseDouble(split[2].substring(split[2].indexOf("=")+1));
			
			locs.add(new Location(lat, lon, dep));
		}
		
		return locs;
	}
	
	private static void plotRuptureHypos(TaperedHypocenterRupVarProbModifier mod, int sourceID, int rupID,
			CybershakeRun run, File outputDir, File inputHyposDir) throws IOException {
		double origProb = mod.erf.getRupture(sourceID, rupID).getProbability();
		RuptureSurface surf = mod.erf.getSource(sourceID).getRupture(rupID).getRuptureSurface();
		List<Double> rvProbs = mod.getVariationProbs(sourceID, rupID, origProb, run, null);
		List<Location> hypos = mod.getHypocenters(sourceID, rupID, run.getERFID(), run.getRupVarScenID());
		List<Point2D> points = mod.getRelativeLocations(surf, hypos);
		System.out.println("Calculated probs for "+rvProbs.size()+" RVs");
		Preconditions.checkState(hypos.size() == rvProbs.size());
		
		System.out.println("Depth to top of rupture: "+surf.getAveRupTopDepth());
		
		List<Location> inputHypos = null;
		List<Point2D> inputHypoPoints = null;
		if (inputHyposDir != null) {
			File inputHypoFile = new File(inputHyposDir, sourceID+"_"+rupID+"_hypos.txt");
			inputHypos = loadInputHypos(inputHypoFile);
			Preconditions.checkState(inputHypos.size() == hypos.size());
			inputHypoPoints = mod.getRelativeLocations(surf, inputHypos);
		}
		
		String sourceName = mod.erf.getSource(sourceID).getName();
		String prefix = "rupture_hypos_"+sourceID+"_"+rupID;
		float mag = (float)mod.erf.getRupture(sourceID, rupID).getMag();
		
		// now plot actual hypos as a function of lat
		List<PlotElement> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		double sumProbs = 0;
		double maxRelative = 0;
		List<Double> relativeProbs = Lists.newArrayList();
		for (int i=0; i<rvProbs.size(); i++) {
			double prob = rvProbs.get(i);
			sumProbs += prob;
			double relative = prob/origProb;
			maxRelative = Math.max(maxRelative, relative);
			relativeProbs.add(relative);
		}
		
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(0d, maxRelative);
		
		DefaultXY_DataSet inputHyposFunc = null;
		List<DiscretizedFunc> inputHypoLines = null;
		if (inputHypos != null) {
			inputHyposFunc = new DefaultXY_DataSet();
			inputHypoLines = Lists.newArrayList();
		}
		
		for (int i=0; i<rvProbs.size(); i++) {
			Location loc = hypos.get(i);
			double x = loc.getLatitude();
			double y = -loc.getDepth();
			double prob = relativeProbs.get(i);
//			double prob = maxRelative*(double)i/(double)(rvProbs.size()-1);
			Color c = cpt.getColor((float)prob);
			funcs.add(new LightFixedXFunc(new double[] {x}, new double[] {y}));
			chars.add(new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 4f, c));
			
			// now input hypos
			if (inputHypos != null) {
				Location inputLoc = inputHypos.get(i);
				double x1 = inputLoc.getLatitude();
				double y1 = -inputLoc.getDepth();
				inputHyposFunc.set(x1, y1);
				inputHypoLines.add(new LightFixedXFunc(
						new LightFixedXFunc(new double[] {x, x1}, new double[] {y, y1})));
				
				if (i == 0) {
					System.out.println("RV Hypo: "+loc);
					System.out.println("Input hypo: "+inputLoc);
				}
			}
		}
		
		if (inputHypos != null) {
			for (DiscretizedFunc func : inputHypoLines) {
				funcs.add(0, func);
				chars.add(0, new PlotCurveCharacterstics(PlotLineType.SOLID, 1f, Color.GRAY));
			}
			
			funcs.add(0, inputHyposFunc);
			chars.add(0, new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 2f, Color.GRAY));
		}
		
		Preconditions.checkState((float)sumProbs == (float)origProb, sumProbs+" != "+origProb);
		
		String title = "M"+mag+" "+sourceName+" Rupture";
		
		int width = 1000;
		// rough calculation for just the chart window
		double calc_height = (double)(width-100) * surf.getAveWidth() / surf.getAveLength();
		int height = (int)calc_height + 350;
		System.out.println("Calc height: "+calc_height+" => "+width+"x"+height);
		
		PlotSpec spec = new PlotSpec(funcs, chars, title, "Latitude", "Altitude");
		GraphWindow gw = new GraphWindow(spec);
		gw.setSize(width, height);
		gw.saveAsPNG(new File(outputDir, prefix+"_latitude.png").getAbsolutePath());
		gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
		
		// now plot as DAS
		
		funcs = Lists.newArrayList();
		chars = Lists.newArrayList();
		
		CSVFile<String> hyposCSV = null;
		double[] distances = null;
		if (inputHypos != null) {
			inputHyposFunc = new DefaultXY_DataSet();
			inputHypoLines = Lists.newArrayList();
			hyposCSV = new CSVFile<String>(true);
			hyposCSV.addLine("RV ID", "Input DAS (km)", "Input DDW (km)",
					"Actual DAS (km)", "Actual DDW (km)", "Perturbation (km)");
			distances = new double[rvProbs.size()];
		}
		
		double surfLen = surf.getAveLength();
		double surfDDW = surf.getAveWidth();
		System.out.println("Surface len: "+surfLen+", DDW: "+surfDDW);
		for (int i=0; i<rvProbs.size(); i++) {
			Point2D pt = points.get(i);
			double x = pt.getX()*surfLen;
			double y = -pt.getY()*surfDDW;
			Color c = cpt.getColor(relativeProbs.get(i).floatValue());
			funcs.add(new LightFixedXFunc(new double[] {x}, new double[] {y}));
			chars.add(new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 4f, c));
			
			// now input hypos
			if (inputHypos != null) {
				Point2D pt1 = inputHypoPoints.get(i);
				double x1 = pt1.getX()*surfLen;
				double y1 = -pt1.getY()*surfDDW;
				inputHyposFunc.set(x1, y1);
				inputHypoLines.add(new LightFixedXFunc(
						new LightFixedXFunc(new double[] {x, x1}, new double[] {y, y1})));

				if (i == 0) {
					System.out.println("RV Hypo: das="+x+", ddw="+y);
					System.out.println("Input Hypo: das="+x1+", ddw="+y1);
				}
				double dist = LocationUtils.linearDistanceFast(hypos.get(i), inputHypos.get(i));
				distances[i] = dist;
				hyposCSV.addLine(i+"", (float)x1+"", (float)y1+"", (float)x+"", (float)y+"", (float)dist+"");
			}
		}
		
		if (inputHypos != null) {
			for (DiscretizedFunc func : inputHypoLines) {
				funcs.add(0, func);
				chars.add(0, new PlotCurveCharacterstics(PlotLineType.SOLID, 1f, Color.GRAY));
			}
			
			funcs.add(0, inputHyposFunc);
			chars.add(0, new PlotCurveCharacterstics(PlotSymbol.FILLED_CIRCLE, 2f, Color.GRAY));
			
			hyposCSV.addLine("", "", "", "", "", "");
			hyposCSV.addLine("", "", "", "", "mean:", (float)StatUtils.mean(distances)+"");
			hyposCSV.addLine("", "", "", "", "median:", (float)DataUtils.median(distances)+"");
			hyposCSV.addLine("", "", "", "", "min:", (float)StatUtils.min(distances)+"");
			hyposCSV.addLine("", "", "", "", "max:", (float)StatUtils.max(distances)+"");
			hyposCSV.writeToFile(new File(outputDir, prefix+"_distances.csv"));
		}
		
		Preconditions.checkState((float)sumProbs == (float)origProb, sumProbs+" != "+origProb);
		
		spec = new PlotSpec(funcs, chars, title, "Distance Along Strike (km)",
				"Distance Down Dip (km)");
		gw = new GraphWindow(spec);
		gw.setSize(width, height);
		gw.saveAsPNG(new File(outputDir, prefix+"_das_ddw.png").getAbsolutePath());
		gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
	}
	
	public static void main(String[] args) throws IOException {
		TaperType taperType = TaperType.ROB_PREFERRED;
		File outputDir = new File("/home/kevin/CyberShake/tapered_hypocenter_dist/"+taperType.name());
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		File inputHyposDir = new File("/home/kevin/CyberShake/tapered_hypocenter_dist/intended_hypos");
		
		// first plot regular taper
		EvenlyDiscrXYZ_DataSet xyz = new EvenlyDiscrXYZ_DataSet(101, 101, 0d, -1d, 0.01);
		for (int xInd=0; xInd<xyz.getNumX(); xInd++) {
			for (int yInd=0; yInd<xyz.getNumY(); yInd++) {
				Point2D point = xyz.getPoint(xInd + xyz.getNumX()*yInd);
				double val = getRelativeProb(taperType, point.getX(), -point.getY());
				xyz.set(xInd, yInd, val);
			}
		}
		xyz.scale(1d/xyz.getSumZ());
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(0d, xyz.getMaxZ());
		XYZPlotSpec xyzSpec = new XYZPlotSpec(xyz, cpt,
				"Hypocenter PDF", "Fraction along Strike", "Fraction Down Dip", "Probability");
		
		XYZPlotWindow xyzGW = new XYZPlotWindow(xyzSpec);
		xyzGW.getXYZPanel().saveAsPNG(new File(outputDir, "hypocenter_pdf_theoretical.png").getAbsolutePath());
		xyzGW.setDefaultCloseOperation(XYZPlotWindow.EXIT_ON_CLOSE);
		
		// now lets plot an actual RV
//		ERF erf = MeanUCERF2_ToDB.createUCERF2_200mERF();
		ERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		TaperedHypocenterRupVarProbModifier mod = new TaperedHypocenterRupVarProbModifier(taperType, erf, db);
		
		List<Integer> runIDs = Lists.newArrayList();
		List<String> siteNames = Lists.newArrayList();
		
		int datasetID = 57;
		int imTypeID = 21;
		
		runIDs.add(3870);
		siteNames.add("LADT");
		
		runIDs.add(3970);
		siteNames.add("USC");
		
		runIDs.add(3873);
		siteNames.add("STNI");
		
		runIDs.add(3880);
		siteNames.add("SBSM");
		
		runIDs.add(3878);
		siteNames.add("PAS");
		
		Runs2DB runs2db = new Runs2DB(db);
		List<CybershakeRun> runs = Lists.newArrayList();
		for (int runID : runIDs)
			runs.add(runs2db.getRun(runID));
		CybershakeIM imType = new HazardCurve2DB(db).getIMFromID(imTypeID);
		
		plotRuptureHypos(mod, 90, 0, runs.get(0), outputDir, inputHyposDir);
		plotRuptureHypos(mod, 90, 3, runs.get(0), outputDir, inputHyposDir);
		plotRuptureHypos(mod, 90, 6, runs.get(0), outputDir, inputHyposDir);
//		plotRuptureHypos(mod, 242, 29, runs.get(0), outputDir, inputHyposDir);
		
		System.out.println("Max discrepancy: "+maxLocDiscrepancyEncountered+" km");
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(
				db, new File("/home/kevin/CyberShake/MCER/.amps_cache"), erf);
		
		for (int i=0; i<runs.size(); i++) {
			CybershakeRun run = runs.get(i);
			String siteName = siteNames.get(i);
			System.out.println("Now doing curve test for "+run.getRunID()+" ("+siteName+")");
			HazardCurveComputation calc = new HazardCurveComputation(db);
			calc.setPeakAmpsAccessor(amps2db);
			List<Double> xVals = Lists.newArrayList();
			for (Point2D pt : new IMT_Info().getDefaultHazardCurve(SA_Param.NAME))
				xVals.add(pt.getX());
			System.out.println("Calculating Original Curve");
			DiscretizedFunc origCurve = calc.computeHazardCurve(xVals, run, imType);
			origCurve.setName("Original");
			calc.setRupVarProbModifier(mod);
			System.out.println("Calculating Modified Curve");
			Stopwatch watch = Stopwatch.createStarted();
			DiscretizedFunc modCurve = calc.computeHazardCurve(xVals, run, imType);
			watch.stop();
			System.out.println("Mod curve took "+watch.elapsed(TimeUnit.SECONDS)+" s");
			System.out.println("Point cache size: "+mod.pointCache.size());
			modCurve.setName("Hypcoenter Prob Tapered");
			System.out.println("Done calculating");
			
			List<DiscretizedFunc> funcs = Lists.newArrayList();
			List<PlotCurveCharacterstics> chars = Lists.newArrayList();
			
			funcs.add(origCurve);
			chars.add(new PlotCurveCharacterstics(
					PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLACK));
			funcs.add(modCurve);
			chars.add(new PlotCurveCharacterstics(
					PlotLineType.SOLID, 2f, PlotSymbol.FILLED_CIRCLE, 4f, Color.BLUE));
			
			PlotSpec spec = new PlotSpec(funcs, chars, siteName+" Comparison", "3sec SA (g)", "1-year POE");
			spec.setLegendVisible(true);
			GraphWindow gw = new GraphWindow(spec);
			gw.setXLog(true);
			gw.setYLog(true);
			gw.setX_AxisRange(1e-2, 3d);
			gw.setY_AxisRange(1e-8, 1e-1);
			gw.saveAsPNG(new File(outputDir, "curve_comparison_"+siteName+".png").getAbsolutePath());
			gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
			
			System.out.println("Max discrepancy: "+maxLocDiscrepancyEncountered+" km");
		}
		
		System.out.println("Fetching original curves");
		HazardCurveFetcher fetch = new HazardCurveFetcher(db, datasetID, imTypeID);
		
		// now maps
		HazardCurveComputation calc = new HazardCurveComputation(db);
		calc.setPeakAmpsAccessor(amps2db);
		calc.setRupVarProbModifier(mod);
		ModifiedMapCalc mapCalc = new ModifiedMapCalc(db, fetch, calc);
		
		try {
			System.out.println("Calculating map");
			mapCalc.generateMap(outputDir, "map", "Tapered Hypocenter Dist", false, 0d, 1d, false, 4e-4);
		} catch (Exception e) {
			ExceptionUtils.throwAsRuntimeException(e);
		}
	}

}
