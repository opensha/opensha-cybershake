package org.opensha.sha.cybershake.eew;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.bombay.BombayBeachHazardCurveCalc;
import org.opensha.sha.cybershake.bombay.RupHyposWithinCutoff;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;
import org.opensha.sha.cybershake.calc.ShakeMapComputation;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DBAPI;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import scratch.kevin.cybershake.BulkCSCurveReplacer;

public class EEWShakeMapCalc {
	
	private RupHyposWithinCutoff rupsWithinCutoff;
	
	private double totalProb;
	private ShakeMapComputation smCalc;
	
	private HashMap<String, GeoDataSet> shakeMaps = new HashMap<String, GeoDataSet>();
	private HashMap<String, Double> unscaledProbs = new HashMap<String, Double>();
	
	private static final int datasetID = 1;
	private static final int erfID = 35;
	private static final int rupVarScenID = 3;
	private static final int imTypeID = 21;
	
	private HazardCurveFetcher fetcher;
	private Runs2DB runs2db;
	private PeakAmplitudesFromDB amps2db;
	private SiteInfo2DB siteDB;
	
	private CybershakeIM im;
	
	private IMT_Info imtInfo = new IMT_Info();
	
	public EEWShakeMapCalc(DBAccess db, RupHyposWithinCutoff rupsWithinCutoff) {
		this.rupsWithinCutoff = rupsWithinCutoff;
		
		loadProbs();
		
		smCalc = new ShakeMapComputation(db);
		fetcher = new HazardCurveFetcher(db, datasetID, imTypeID);
		runs2db = new Runs2DB(db);
		amps2db = new PeakAmplitudesFromDB(db);
		siteDB = new SiteInfo2DB(db);
		
		im = new HazardCurve2DB(db).getIMFromID(imTypeID);
	}
	
	private void loadProbs() {
		totalProb = 0;
		
		AbstractERF erf = rupsWithinCutoff.getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double rupProb = rup.getProbability();
				
				ArrayList<Integer> rvs = rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID);
				if (rvs != null && rvs.size() > 0) {
					System.out.println("INCLUDING SOURCE: " + sourceID + " " + rupID + " mag: " + rup.getMag());
					int numExcluded = rupsWithinCutoff.getNumExcluded(sourceID, rupID);
					if (numExcluded < 0)
						throw new RuntimeException("Unexpected numEcluded!");
					double includedToTotal = (double)rvs.size() / (double)(numExcluded+rvs.size());
					double includedProb = rupProb * includedToTotal;
					unscaledProbs.put(RupHyposWithinCutoff.getKey(sourceID, rupID), includedProb);
					totalProb += includedProb;
				}
			}
		}
	}
	
	public GeoDataSet doCalc(String dir) throws IOException {
		ArbDiscrGeoDataSet xyz = new ArbDiscrGeoDataSet(true);
		for (CybershakeSite site : fetcher.getCurveSites()) {
			ArrayList<CybershakeRun> runs = runs2db.getRuns(site.id);
			if (runs.size() > 0) {
				CybershakeRun run = runs.get(0);
				if (run.getERFID() != erfID || run.getRupVarScenID() != rupVarScenID)
					continue;
				
				String fname = dir + File.separator + "run_"+run.getRunID()+".txt";
				File file = new File(fname);
				DiscretizedFunc curve;
				if (file.exists()) {
					curve = ArbitrarilyDiscretizedFunc.loadFuncFromSimpleFile(fname);
				} else {
					curve = getCurve(run);
					if (curve == null)
						continue;
					ArbitrarilyDiscretizedFunc.writeSimpleFuncFile(curve, fname);
				}
				
				double val = HazardDataSetLoader.getCurveVal(curve, false, 0.5);
				xyz.set(new Location(site.lat, site.lon), val);
			}
		}
		return xyz;
	}
	
	private ArbitrarilyDiscretizedFunc getXVals() {
		return imtInfo.getDefaultHazardCurve(SA_Param.NAME);
	}
	
	private DiscretizedFunc getCurve(CybershakeRun run) {
		System.out.println("calculating curve for run " + run.getRunID());
		ArrayList<Double> xVals = new ArrayList<Double>();
		ArbitrarilyDiscretizedFunc curve = getXVals();
		for (int i=0; i<curve.size(); i++)
			xVals.add(curve.getX(i));
		
		double myProbs = 0;
		
		List<Integer> srcIdList = siteDB.getSrcIdsForSite(run.getSiteID(), erfID);
		for (int sourceID : srcIdList) {
			List<Integer> rupIdList = siteDB.getRupIdsForSite(run.getSiteID(), erfID, sourceID);
			for (int rupID : rupIdList) {
				String key = RupHyposWithinCutoff.getKey(sourceID, rupID);
				if (unscaledProbs.containsKey(key)) {
					double unscaledProb = unscaledProbs.get(key);
					double scaledProb = unscaledProb / totalProb;
					
					myProbs += scaledProb;
					
					List<Double> allIMVals;
					try {
						allIMVals = amps2db.getIM_Values(run.getRunID(), sourceID, rupID, im);
					} catch (SQLException e) {
						e.printStackTrace();
						return null;
					}
					ArrayList<Integer> rvIDs = rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID);
					ArrayList<Double> imVals = new ArrayList<Double>();
					for (int rvID=0; rvID<allIMVals.size(); rvID++) {
						if (rvIDs.contains(rvID))
							imVals.add(allIMVals.get(rvID));
					}
					HazardCurveComputation.handleRupture(xVals, imVals, curve, scaledProb);
				}
			}
		}
//		System.out.println("Curve before inverse: " + curve);
		for(int j=0; j<curve.size(); ++j) 
			curve.set(curve.getX(j),(1-curve.getY(j)));
		System.out.println("Curve after inverse: " + curve);
		System.out.println("Scaled curve probs total: " + myProbs);
		return curve;
	}
	
//	private void calcShakeMaps() {
//		EqkRupForecast erf = rupsWithinCutoff.getERF();
//		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
//			ProbEqkSource source = erf.getSource(sourceID);
//			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
//				String key = RupHyposWithinCutoff.getKey(sourceID, rupID);
//				if (unscaledProbs.containsKey(key)) {
//					double unscaledProb = unscaledProbs.get(key);
//					double scaledProb = unscaledProb / totalProb;
//					
//					XYZ_DataSetAPI sm = smCalc.getShakeMap(datasetID, erfID, rupVarScenID, imTypeID, sourceID, rupID,
//							rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID));
//					
//					shakeMaps.put(key, sm);
//				}
//			}
//		}
//	}
//	
//	private static void addValueToSM(double x, double y, double val, XYZ_DataSetAPI sm) {
//		ArrayList<Double> xVals = sm.getX_DataSet();
//		ArrayList<Double> yVals = sm.getY_DataSet();
//		ArrayList<Double> zVals = sm.getZ_DataSet();
//		for (int i=0; i<xVals.size(); i++) {
//			if (xVals.get(i) == x && yVals.get(i) == y) {
//				zVals.set(i, zVals.get(i)+val);
//				return;
//			}
//		}
//		xVals.add(x);
//		yVals.add(y);
//		zVals.add(val);
//	}
//	
//	private XYZ_DataSetAPI calcMasterShakeMap() {
//		ArbDiscretizedXYZ_DataSet xyz = new ArbDiscretizedXYZ_DataSet();
//		EqkRupForecast erf = rupsWithinCutoff.getERF();
//		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
//			ProbEqkSource source = erf.getSource(sourceID);
//			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
//				String key = RupHyposWithinCutoff.getKey(sourceID, rupID);
//				if (unscaledProbs.containsKey(key)) {
//					double unscaledProb = unscaledProbs.get(key);
//					double scaledProb = unscaledProb / totalProb;
//					
//					XYZ_DataSetAPI sm = shakeMaps.get(key);
//					ArrayList<Double> xVals = sm.getX_DataSet();
//					ArrayList<Double> yVals = sm.getY_DataSet();
//					ArrayList<Double> zVals = sm.getZ_DataSet();
//					for (int i=0; i<xVals.size(); i++) {
//						
//					}
//				}
//			}
//		}
//		return xyz;
//	}
	
	public static void main(String[] args) {
		try {
			String dir = "/home/kevin/CyberShake/eew/parkfield";
			Location hypoLocation = BombayBeachHazardCurveCalc.PARKFIELD_LOC;
			double minMag = 7.0;
			dir += "/min"+(float)minMag;
			double maxDistance = 10d;
			String sourceNameConstr = null;
			boolean useDepth = false;
			
			DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
			
			RupHyposWithinCutoff rups =
				new RupHyposWithinCutoff(db, hypoLocation, maxDistance, sourceNameConstr, useDepth, minMag);
			
			EEWShakeMapCalc calc = new EEWShakeMapCalc(db, rups);
			
			GeoDataSet sm = calc.doCalc(dir);
			
			ArbDiscrGeoDataSet.writeXYZFile(sm, dir+File.separator+"shakemap.txt");
			
//			calc.calc(dir);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}

}
