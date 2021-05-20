package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.io.File;
import java.util.Map;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.xyz.EvenlyDiscrXYZ_DataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;

import com.google.common.base.Preconditions;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class GriddedSpectrumInterpolator {
	
	/*
	 * Organized by lon,lat
	 */
//	private DiscretizedFunc[][] gridded;
	private double[] periods;
	private EvenlyDiscrXYZ_DataSet[] xyzs;
	private double latSpacing;
	private double lonSpacing;
	private double minLat;
	private double minLon;
	private int numLat;
	private int numLon;
	
	public GriddedSpectrumInterpolator(Map<Location, ? extends DiscretizedFunc> map, double spacing) {
		this(map, spacing, spacing);
	}
	
	public GriddedSpectrumInterpolator(Map<Location, ? extends DiscretizedFunc> map, double latSpacing, double lonSpacing) {
		this.latSpacing = latSpacing;
		this.lonSpacing = lonSpacing;
		
		MinMaxAveTracker latTrack = new MinMaxAveTracker();
		MinMaxAveTracker lonTrack = new MinMaxAveTracker();
		for (Location loc : map.keySet()) {
			latTrack.addValue(loc.getLatitude());
			lonTrack.addValue(loc.getLongitude());
		}
		minLat = latTrack.getMin();
		double maxLat = latTrack.getMax();
		minLon = lonTrack.getMin();
		double maxLon = lonTrack.getMax();
		
		numLat = (int)Math.round((maxLat - minLat)/latSpacing)+1;
		numLon = (int)Math.round((maxLon - minLon)/lonSpacing)+1;
		
//		System.out.println("MinLat: "+minLat);
//		System.out.println((maxLat - minLat)/latSpacing);
		
		checkGridding("Latitude", minLat, maxLat, numLat, latSpacing);
		checkGridding("Longitude", minLon, maxLon, numLon, lonSpacing);
		
		DiscretizedFunc example = map.values().iterator().next();
		periods = new double[example.size()];
		for (int p=0; p<periods.length; p++)
			periods[p] = example.getX(p);
		xyzs = new EvenlyDiscrXYZ_DataSet[periods.length];
		for (int p=0; p<periods.length; p++) {
			xyzs[p] = new EvenlyDiscrXYZ_DataSet(numLon, numLat, minLon, minLat, lonSpacing, latSpacing);
			// initialize to NaNs
			for (int i=0; i<xyzs[p].size(); i++)
				xyzs[p].set(i, Double.NaN);
		}
		
		System.out.println("Gridded to new grid with corner "+minLat+","+minLon+" and size "+numLat+","+numLon);
		
		double maxDiffLat = latSpacing*0.01;
		double maxDiffLon = lonSpacing*0.01;
		// now associate to nodes
		for (Location loc : map.keySet()) {
			int latIndex = xyzs[0].getYIndex(loc.getLatitude());
			Preconditions.checkState(latIndex >= 0 && latIndex < numLat,
					"Bad input latitude. latRage=[%s %s], myLat=%s, numLat=%s, myIndex=%s",
					minLat, maxLat, loc.getLatitude(), numLat, latIndex);
			int lonIndex = xyzs[0].getXIndex(loc.getLongitude());
			Preconditions.checkState(lonIndex >= 0 && lonIndex < numLon,
					"Bad input longitude. lonRage=[%s %s], myLon=%s, numLon=%s, myIndex=%s",
					minLon, maxLon, loc.getLongitude(), numLon, lonIndex);
			Location gridLoc = getGridLocation(latIndex, lonIndex);
			Preconditions.checkState(Math.abs(gridLoc.getLatitude() - loc.getLatitude())<=maxDiffLat,
					"Couldn't snap to grid, too far!\n\tOrig: %s\n\tGrid: %s", loc, gridLoc);
			Preconditions.checkState(Math.abs(gridLoc.getLongitude() - loc.getLongitude())<=maxDiffLon,
					"Couldn't snap to grid, too far!\n\tOrig: %s\n\tGrid: %s", loc, gridLoc);
			Preconditions.checkState(Double.isNaN(xyzs[0].get(lonIndex, latIndex)), "Duplicate found at %s, %s!", lonIndex, latIndex);
			DiscretizedFunc func = map.get(loc);
			Preconditions.checkState(func.size() == periods.length, "Spectrum periods inconsistent!");
			for (int p=0; p<periods.length; p++) {
				Preconditions.checkState((float)func.getX(p) == (float)periods[p], "Spectrum periods inconsistent!");
				xyzs[p].set(lonIndex, latIndex, func.getY(p));
			}
		}
	}
	
	public Location getGridLocation(int latIndex, int lonIndex) {
		double lat = minLat + latIndex*latSpacing;
		double lon = minLon + lonIndex*lonSpacing;
		return new Location(lat, lon);
	}
	
	public DiscretizedFunc getInterpolated(Location loc) {
		DiscretizedFunc ret = new ArbitrarilyDiscretizedFunc();
		
		double lat = loc.getLatitude();
		double lon = loc.getLongitude();
		
		for (int i=0; i<periods.length; i++) {
			double y = xyzs[i].bilinearInterpolation(lon, lat);
			Preconditions.checkState(!Double.isNaN(y),
					"Bad interpolation, one or more of the surrounding points was undefined! loc: %s", loc);
			ret.set(periods[i], y);
		}
		
		return ret;
	}
	
	public DiscretizedFunc getClosest(Location loc) {
		DiscretizedFunc ret = new ArbitrarilyDiscretizedFunc();
		
		double lat = loc.getLatitude();
		double lon = loc.getLongitude();
		
		int xInd = xyzs[0].getXIndex(lon);
		int yInd = xyzs[0].getYIndex(lat);
		
		for (int i=0; i<periods.length; i++) {
			double y = xyzs[i].get(xInd, yInd);
			ret.set(periods[i], y);
		}
		
		return ret;
	}
	
	public Location getClosestGridLoc(Location loc) {
		double lat = loc.getLatitude();
		double lon = loc.getLongitude();
		
		int xInd = xyzs[0].getXIndex(lon);
		int yInd = xyzs[0].getYIndex(lat);
		
		return getGridLocation(yInd, xInd);
	}
	
	public Location getClosestDefinedGridLoc(Location loc) {
		double lat = loc.getLatitude();
		double lon = loc.getLongitude();
		
		int xInd = xyzs[0].getXIndex(lon);
		int yInd = xyzs[0].getYIndex(lat);
		
		double minDist = Double.POSITIVE_INFINITY;
		Location closestLoc = null;
		
		for (int x=xInd-1; x<=xInd+1; x++) {
			if (xInd < 0 || xInd >= xyzs[0].getNumX())
				continue;
			for (int y=yInd-1; y<=yInd+1; y++) {
				if (yInd < 0 || yInd >= xyzs[0].getNumY())
					continue;
				if (!isDefined(x, y))
					continue;
				Location gridLoc = getGridLocation(y, x);
				double dist = LocationUtils.horzDistanceFast(loc, gridLoc);
				if (dist < minDist) {
					minDist = dist;
					closestLoc = gridLoc;
				}
			}
		}
		
		Preconditions.checkNotNull(closestLoc,
				"Neither the closest grid loc, nor any direct neighbors of the closest grid loc are defined");
		
		return closestLoc;
	}
	
	/**
	 * 
	 * @param xInd
	 * @param yInd
	 * @return true if at least one spectrum value defined
	 */
	private boolean isDefined(int xInd, int yInd) {
		for (int i=0; i<periods.length; i++) {
			double y = xyzs[i].get(xInd, yInd);
			if (!Double.isNaN(y))
				return true;
		}
		return false;
	}
	
	private static void checkGridding(String name, double min, double max, int num, double spacing) {
		double calcMax = min + spacing*(num-1);
		Preconditions.checkState((float)calcMax == (float)max,
				"%s gridding mismatch. Calculated max=%s, actual=%s", name, calcMax, max);
	}

	public static void main(String[] args) throws Exception {
		double spacing = 0.02;
//		File dataFile = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen/mcer_binary_results/UCERF3/Wills.bin");
		File dataFile = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/mcer_spectrum.bin");
		BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dataFile.getAbsolutePath());
		Map<Location, ArbitrarilyDiscretizedFunc> curves = reader.getCurveMap();
		
		GriddedSpectrumInterpolator interp = new GriddedSpectrumInterpolator(curves, spacing);
		interp.getInterpolated(new Location(34.020581, -118.305007));
		
		Location ll = new Location(34.2, -118.8);
		double size = 0.5;
		// now test graphically
		Region testReg = new Region(ll, new Location(ll.getLatitude()+size, ll.getLongitude()+size));
		GriddedRegion nativeReg = new GriddedRegion(testReg, spacing, null);
		GriddedGeoDataSet nativeData = new GriddedGeoDataSet(nativeReg, false);
		for (int i=0; i<nativeData.size(); i++)
			nativeData.set(i, Double.NaN);
		int numNativeSet = 0;
		for (Location loc : curves.keySet()) {
			ArbitrarilyDiscretizedFunc func = curves.get(loc);
			if (nativeReg.indexForLocation(loc) >= 0) {
				nativeData.set(loc, func.getY(0));
				numNativeSet++;
			}
		}
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(nativeData.getMinZ(), nativeData.getMaxZ());
		cpt.setNanColor(Color.GRAY);
		System.out.println("Set "+numNativeSet+"/"+nativeData.size()+" native locs");
		Preconditions.checkState(numNativeSet == nativeData.size());
		GMT_Map map = new GMT_Map(nativeReg, nativeData, spacing, cpt);
		map.setRescaleCPT(false);
		map.setUseGMTSmoothing(false);
		FaultBasedMapGen.plotMap(null, null, true, map);
		GriddedRegion highReg = new GriddedRegion(testReg, spacing/4, null);
		GriddedGeoDataSet highData = new GriddedGeoDataSet(highReg, false);
		for (Location loc : highReg.getNodeList())
			highData.set(loc, interp.getInterpolated(loc).getY(0));
		map = new GMT_Map(highReg, highData, highReg.getSpacing(), cpt);
		map.setRescaleCPT(false);
		map.setUseGMTSmoothing(false);
		FaultBasedMapGen.plotMap(null, null, true, map);
	}

}
