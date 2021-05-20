package org.opensha.sha.cybershake.eew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.bombay.BombayBeachHazardCurveCalc;
import org.opensha.sha.cybershake.bombay.RupHyposWithinCutoff;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scratch.kevin.cybershake.BulkCSCurveReplacer;

public class EEWCalc implements RuptureVariationProbabilityModifier {
	
	private RupHyposWithinCutoff rupsWithinCutoff;
	
	private double totalProb;
	private BulkCSCurveReplacer calc;
	
	public EEWCalc(DBAccess db, ArrayList<Integer> ims, RupHyposWithinCutoff rupsWithinCutoff) {
		this.rupsWithinCutoff = rupsWithinCutoff;
		
		loadProbs();
		
		calc = new BulkCSCurveReplacer(db);
		calc.setRecalcIMs(ims);
		calc.setRupVarProbModifier(this);
		calc.setRupProbModifier(new ZeroProbMod());
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
					int numExcluded = rupsWithinCutoff.getNumExcluded(sourceID, rupID);
					if (numExcluded < 0)
						throw new RuntimeException("Unexpected numEcluded!");
					double includedToTotal = (double)rvs.size() / (double)(numExcluded+rvs.size());
					double includedProb = rupProb * includedToTotal;
					totalProb += includedProb;
				}
			}
		}
	}

//	@Override
//	public ArrayList<Integer> getModVariations(int sourceID, int rupID) {
//		return rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID);
//	}
//
//	@Override
//	public double getModifiedProb(int sourceID, int rupID, double origProb) {
//		double rupProb = rupsWithinCutoff.getERF().getRupture(sourceID, rupID).getProbability();
//		ArrayList<Integer> inclIDs = rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID);
//		if (inclIDs == null || inclIDs.size() == 0)
//			return 0d;
//		double numIncluded = inclIDs.size();
//		double numExcluded = rupsWithinCutoff.getNumExcluded(sourceID, rupID);
//		double incToTotal = numIncluded / (numIncluded + numExcluded);
//		double inclRupsProb = rupProb * incToTotal;
//		double scaledUpProb = inclRupsProb / totalProb;
//		// this will get multiplied by numIncluded / total in the HC calc, but we don't want that
//		// so we multiply it by total / numIncluded to reverse the effects
//		return scaledUpProb * ((numIncluded + numExcluded) / numIncluded);
//	}

	@Override
	public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run, CybershakeIM im) {
		double rupProb = rupsWithinCutoff.getERF().getRupture(sourceID, rupID).getProbability();
		HashSet<Integer> inclIDs = new HashSet<Integer>(rupsWithinCutoff.getVariationsWithinCutoff(sourceID, rupID));
		if (inclIDs == null || inclIDs.size() == 0)
			return null;
		int numIncluded = inclIDs.size();
		int numExcluded = rupsWithinCutoff.getNumExcluded(sourceID, rupID);
		int totRVs = numIncluded+numExcluded;
		double probPer = rupProb/(double)totRVs;
		List<Double> ret = Lists.newArrayList();
		for (int i=0; i<totRVs; i++) {
			if (inclIDs.contains(i))
				ret.add(probPer);
			else
				ret.add(0d);
		}
		return ret;
	}
	
	public void calc(String dir) throws IOException {
		calc.recalculateAllCurves(dir);
	}
	
	public static void main(String[] args) {
		try {
			String dir = "/home/kevin/CyberShake/eew/parkfield";
			Location hypoLocation = BombayBeachHazardCurveCalc.PARKFIELD_LOC;
			double minMag = 6.5;
			double maxDistance = 10d;
			String sourceNameConstr = null;
			boolean useDepth = false;
			
			ArrayList<Integer> ims = new ArrayList<Integer>();
			ims.add(21);
			
			DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
			
			RupHyposWithinCutoff rups =
				new RupHyposWithinCutoff(db, hypoLocation, maxDistance, sourceNameConstr, useDepth, minMag);
			
			EEWCalc calc = new EEWCalc(db, ims, rups);
			
			calc.calc(dir);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}

}
