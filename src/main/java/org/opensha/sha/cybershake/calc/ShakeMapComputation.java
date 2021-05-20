package org.opensha.sha.cybershake.calc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.ArbDiscrXYZ_DataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.XYZ_DataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;

import com.google.common.collect.Lists;

public class ShakeMapComputation {
	
	private DBAccess db;
	private Runs2DB runs2db;
	private HazardCurve2DB hc2db;
	private PeakAmplitudesFromDB amps2db;
	private ERF2DB erf2db;
	private HazardCurve2DB curves2db;
	
	private Double valForMissing = null;
	
	public ShakeMapComputation(DBAccess db) {
		this.db = db;
		runs2db = new Runs2DB(db);
		hc2db = new HazardCurve2DB(db);
		amps2db = new PeakAmplitudesFromDB(db);
		erf2db = new ERF2DB(db);
		curves2db = new HazardCurve2DB(db);
	}
	
	public void setValForMissing(double valForMissing) {
		this.valForMissing = valForMissing;
	}
	
	public GeoDataSet getShakeMap(int datasetID, int erfID, int rupVarScenID, int imTypeID,
			int sourceID, int rupID, Location hypo) {
		return getShakeMap(datasetID, erfID, rupVarScenID, imTypeID, sourceID, rupID, Lists.newArrayList(hypo));
	}
	
	public GeoDataSet getShakeMap(int datasetID, int erfID, int rupVarScenID, int imTypeID,
			int sourceID, int rupID, List<Location> hypos) {
		CybershakeIM im = hc2db.getIMFromID(imTypeID);
		return getShakeMap(datasetID, erfID, rupVarScenID, im, sourceID, rupID, hypos);
	}
	
	public GeoDataSet getShakeMap(int datasetID, int erfID, int rupVarScenID, CybershakeIM im,
			int sourceID, int rupID, Location hypo) {
		return getShakeMap(datasetID, erfID, rupVarScenID, im, sourceID, rupID, Lists.newArrayList(hypo));
	}
	
	public GeoDataSet getShakeMap(int datasetID, int erfID, int rupVarScenID, CybershakeIM im,
			int sourceID, int rupID, List<Location> hypos) {
		ArrayList<Integer> rvsToInclude;
		if (hypos == null || hypos.isEmpty()) {
			rvsToInclude = null;
		} else {
			HashSet<Integer> rvsToIncludeSet = new HashSet<Integer>();
			HashMap<Integer, Location> rvHypos = erf2db.getHypocenters(erfID, sourceID, rupID, rupVarScenID);
			
			for (Location hypo : hypos) {
				// find closest to this hypocenter
				Location closestRV = null;
				double closestDist = Double.MAX_VALUE;
				for (Location rvHypo : rvHypos.values()) {
					double dist = LocationUtils.linearDistance(hypo, rvHypo);
					if (dist < closestDist) {
						closestDist = dist;
						closestRV = rvHypo;
					}
				}
				
				for (int rvID : rvHypos.keySet()) {
					Location rvHypo = rvHypos.get(rvID);
					if (closestRV.equals(rvHypo))
						rvsToIncludeSet.add(rvID);
				}
				
				System.out.println("Matched hypocenter with RV hypo "+closestDist+" KM away. "+rvsToIncludeSet.size()+" RVs");
			}
			rvsToInclude = Lists.newArrayList(rvsToIncludeSet);
		}
		
		return getShakeMap(datasetID, erfID, rupVarScenID, im, sourceID, rupID, rvsToInclude);
	}
	
	public GeoDataSet getShakeMap(int datasetID, int erfID, int rupVarScenID, int imTypeID,
			int sourceID, int rupID, ArrayList<Integer> rvsToInclude) {
		CybershakeIM im = hc2db.getIMFromID(imTypeID);
		return getShakeMap(datasetID, erfID, rupVarScenID, im, sourceID, rupID, rvsToInclude);
	}
	
	public GeoDataSet getShakeMap(int datasetID, int erfID, int rupVarScenID, CybershakeIM im,
			int sourceID, int rupID, ArrayList<Integer> rvsToInclude) {
		
		GeoDataSet xyz = new ArbDiscrGeoDataSet(true);
		
		HazardCurveFetcher fetcher = new HazardCurveFetcher(db, datasetID, im.getID());
		
		List<CybershakeSite> sites = fetcher.getCurveSites();
		MinMaxAveTracker numAmpsTrack = new MinMaxAveTracker();
		int sitesLoaded = 0;
		for (int i=0; i<sites.size(); i++) {
			CybershakeSite site = sites.get(i);
			int curveID = fetcher.getCurveIDs().get(i);
			int runID = curves2db.getRunIDForCurve(curveID);
			CybershakeRun run = runs2db.getRun(runID);
			List<Double> imVals;
			try {
				imVals = amps2db.getIM_Values(run.getRunID(), sourceID, rupID, im);
			} catch (SQLException e) {
//				e.printStackTrace();
				if (valForMissing != null)
					xyz.set(new Location(site.lat, site.lon), valForMissing);
				continue;
			}

//			System.out.println("loaded " + imVals.size() + " amps for site: " + site);

			if (rvsToInclude != null) {
				ArrayList<Double> newIMVals = new ArrayList<Double>();
				for (int rvID=0; rvID<imVals.size(); rvID++) {
					if (rvsToInclude.contains(rvID))
						newIMVals.add(imVals.get(rvID));
				}
				imVals = newIMVals;
//				System.out.println("new size: " + imVals.size());
			}
			numAmpsTrack.addValue(imVals.size());
			sitesLoaded++;

			//				if (rvsToInclude != null) {
			//					if (imVals.size() != rvHypos.size())
			//						throw new RuntimeException("rv count mismatch!");
			//					ArrayList<Double> newIMVals = new ArrayList<Double>();
			//					for (int rvID=0; rvID<imVals.size(); rvID++) {
			//						Location rvHypo = rvHypos.get(rvID);
			//						if (hypo.equals(rvHypo))
			//							newIMVals.add(imVals.get(rvID));
			//					}
			//					imVals = newIMVals;
			//					System.out.println("RV's with correct hypo: " + imVals.size());
			//				}

			double logTotal = 0;
			for (int j=0; j<imVals.size(); j++) {
				double logIM = Math.log(imVals.get(j) / HazardCurveComputation.CONVERSION_TO_G);
				logTotal += logIM;
			}
//			System.out.println("logTotal: " + logTotal);
			double logMean = logTotal / (double)imVals.size();
//			System.out.println("logMean: " + logMean);
			double mean = Math.exp(logMean);

//			System.out.println("Mean: " + mean);

			xyz.set(new Location(site.lat, site.lon), mean);

			//				try {
			//					System.out.println("Getting amps for site: " + site);
			//					ArrayList<Double> vals = amps2db.getIM_Values(runID, sourceID, rupID, im);
			//					if (vals != null && vals.size() > 0) {
			//						for (double val : vals) {
			//							val /= HazardCurveComputation.CONVERSION_TO_G;
			//						}
			//						xyz.addValue(site.lat, site.lon, val);
			//					}
			//				} catch (Exception e) {
			//					System.out.println("No amp for site: " + site);
			//				}
		}
		
		System.out.println("Loaded "+sitesLoaded+" sites, im vals loaded per: "+numAmpsTrack.getAverage());
		
		return xyz;
	}

}
