package org.opensha.sha.cybershake.bombay;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.opensha.commons.data.function.ArbDiscrEmpiricalDistFunc;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BombayBeachHazardCurveCalc implements RuptureVariationProbabilityModifier {
	
	/** The location of the M4.8 event */
	public static final Location BOMBAY_LOC = new Location(33.318333, -115.728333);
	public static final Location PARKFIELD_LOC = new Location(35.815, -120.374);
	public static final Location PICO_RIVERA_LOC = new Location(33.99, -118.08, 18.9);
	public static final Location YUCAIPA_LOC = new Location(34.058, -117.011, 11.8);
	public static final Location COYOTE_CREEK = new Location(33.4205, -116.4887, 14.0);
//	public static double MAX_DIST_KM = 10;
	
	private double increaseMultFactor;
	
	private HashMap<String, ArrayList<Integer>> rvMap = new HashMap<String, ArrayList<Integer>>();
	
	private RupHyposWithinCutoff rupsWithinCutoff;
	
	public BombayBeachHazardCurveCalc(DBAccess db, double increaseMultFactor, Location hypoLocation,
			double maxDistance, String sourceNameConstr, boolean useDepth) {
		this.increaseMultFactor = increaseMultFactor;
		
		rupsWithinCutoff = new RupHyposWithinCutoff(db, hypoLocation, maxDistance, sourceNameConstr, useDepth, 0.0);
	}
	
//	/**
//	 * Computes the Hazard Curve at the given site 
//	 * @param imlVals
//	 * @param site
//	 * @param erfName
//	 * @param imType
//	 */
//	public DiscretizedFuncAPI computeHazardCurve(ArrayList<Double> imlVals, CybershakeRun run, CybershakeIM imType){
//		DiscretizedFuncAPI hazardFunc = new ArbitrarilyDiscretizedFunc();
//		int siteID = run.getSiteID();
//		int erfID = run.getERFID();
//		int runID = run.getRunID();
//		int numIMLs  = imlVals.size();
//		for(int i=0; i<numIMLs; ++i) hazardFunc.set((imlVals.get(i)).doubleValue(), 1.0);
//		
//		ArrayList<Integer> srcIdList = site2db.getSrcIdsForSite(siteID, erfID);
//		int numSrcs = srcIdList.size();
//		for(int srcIndex =0;srcIndex<numSrcs;++srcIndex){
////			updateProgress(srcIndex, numSrcs);
//			System.out.println("Source " + srcIndex + " of " + numSrcs + ".");
//			int srcId = srcIdList.get(srcIndex);
//			ArrayList<Integer> rupIdList = site2db.getRupIdsForSite(siteID, erfID, srcId);
//			int numRupSize = rupIdList.size();
//			for(int rupIndex = 0;rupIndex<numRupSize;++rupIndex){
//				int rupId = rupIdList.get(rupIndex);
//				double qkProb = erf2db.getRuptureProb(erfID, srcId, rupId);
//				ArbDiscrEmpiricalDistFunc function = new ArbDiscrEmpiricalDistFunc();
//				ArrayList<Double> imVals;
//				try {
//					imVals = amps2db.getIM_Values(runID, srcId, rupId, imType);
//				} catch (SQLException e) {
//					return null;
//				}
//				for (double val : imVals) {
//					function.set(val/HazardCurveComputation.CONVERSION_TO_G,1);
//				}
////				ArrayList<Integer> rupVariations = peakAmplitudes.getRupVarationsForRupture(erfId, srcId, rupId);
////				int size = rupVariations.size();
////				for(int i=0;i<size;++i){
////					int rupVarId =  rupVariations.get(i);
////					double imVal = peakAmplitudes.getIM_Value(siteId, erfId, sgtVariation, rvid, srcId, rupId, rupVarId, imType);
////					function.set(imVal/CONVERSION_TO_G,1);
////				}
//				HazardCurveComputation.setIMLProbs(imlVals,hazardFunc, function.getNormalizedCumDist(), qkProb);
//			}
//		}
//	     
//	    for(int j=0; j<numIMLs; ++j) 
//	    	hazardFunc.set(hazardFunc.getX(j),(1-hazardFunc.getY(j)));
//
//		return hazardFunc;
//	}
	
	public static void main(String args[]) {
		try {
			DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
			
//			String sourceNameConstr = "andreas";
//			BombayBeachHazardCurveCalc calc = new BombayBeachHazardCurveCalc(db, 1000d, BOMBAY_LOC,
//					10d, sourceNameConstr, false);
//			BombayBeachHazardCurveCalc calc = new BombayBeachHazardCurveCalc(db, 1000d, PARKFIELD_LOC,
//					10d, sourceNameConstr, false);
//			String sourceNameConstr = "puente";
			String sourceNameConstr = null;
//			BombayBeachHazardCurveCalc calc = new BombayBeachHazardCurveCalc(db, 1000d, PICO_RIVERA_LOC,
//					15d, sourceNameConstr, true);
//			BombayBeachHazardCurveCalc calc = new BombayBeachHazardCurveCalc(db, 1000d, YUCAIPA_LOC,
//					10d, sourceNameConstr, true);
			BombayBeachHazardCurveCalc calc = new BombayBeachHazardCurveCalc(db, 1000d, COYOTE_CREEK,
					10d, sourceNameConstr, true);
			
//			try {
//				calc.writeSourceRupInfoFile("/home/kevin/CyberShake/bombay/rv_info.txt");
//				calc.writeSourceRupInfoFile("/home/kevin/CyberShake/parkfield/rv_info.txt");
//				calc.writeSourceRupInfoFile("/home/kevin/CyberShake/picoRivera/rv_info.txt");
//				calc.writeSourceRupInfoFile("/home/kevin/CyberShake/yucaipa/rv_info.txt");
//				calc.writeSourceRupInfoFile("/home/kevin/CyberShake/coyote/rv_info.txt");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
//			calc.testProbRanges();
			
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
//	private void testProbRanges() {
//		double minOrig = 1;
//		double minNew = 1;
//		double maxOrig = 0;
//		double maxNew = 0;
//		for (int i=0; i<sources.size(); i++) {
//			int sourceID = sources.get(i);
//			ArrayList<Integer> rupIDs = rups.get(i);
//			for (int rupID : rupIDs) {
//				double origProb = erf2db.getRuptureProb(ERFID, sourceID, rupID);
//				ArrayList<Integer> rvs = rvMap.get(getKey(sourceID, rupID));
//				if (rvs == null)
//					continue;
//				for (int rupVarID : rvs) {
////					double newProb = getModifiedProb(sourceID, rupID, rupVarID, origProb);
////					System.out.println("orig: " + origProb + ", mod: " + newProb);
////					if (origProb < minOrig)
////						minOrig = origProb;
////					if (origProb > maxOrig)
////						maxOrig = origProb;
////					if (newProb < maxNew)
////						minNew = newProb;
////					if (newProb > maxNew)
////						maxNew = newProb;
//					break;
//				}
//			}
//		}
//		System.out.println("1 yr probability ranges:");
//		System.out.println("Orig range: " + minOrig + " => " + maxOrig);
//		System.out.println("New range: " + minNew + " => " + maxNew);
//		System.out.println("1 day probability ranges:");
//		System.out.println("Orig range: " + minOrig/365d + " => " + maxOrig/365d);
//		System.out.println("New range: " + minNew/365d + " => " + maxNew/365d);
//	}
	
//	public ArrayList<Integer> getModVariations(int sourceID, int rupID) {
//		return rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID);
//	}
//	
//	public double getModifiedProb(int sourceID, int rupID, double originalProb) {
//		return originalProb * increaseMultFactor;
////		return originalProb;
//	}

	@Override
	public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb,
			CybershakeRun run, CybershakeIM im) {
		HashSet<Integer> modIDs = new HashSet<Integer>(rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID));
		if (modIDs == null || modIDs.isEmpty())
			return null;
		int numExcluded = rupsWithinCutoff.getNumExcluded(sourceID, rupID);
		int totRVs = modIDs.size() + numExcluded;
		double origProbPer = originalProb / (double)totRVs;
		double modProbPer = origProbPer*increaseMultFactor;

		List<Double> ret = Lists.newArrayList();
		for (int rvID=0; rvID<totRVs; rvID++) {
			if (modIDs.contains(rvID))
				ret.add(modProbPer);
			else
				ret.add(origProbPer);
		}
		
		return ret;
	}

}

