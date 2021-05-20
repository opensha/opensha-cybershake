package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.calc.mcer.AbstractMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.DeterministicResult;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.UCERF2_AleatoryMagVarRemovalMod;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;

public class CyberShakeMCErDeterministicCalc extends AbstractMCErDeterministicCalc {
	
	private CachedPeakAmplitudesFromDB amps2db;
	private HazardCurve2DB curves2db;
	private ERF erf;
	private CyberShakeComponent component;
	
//	static final double mag_range = 0.11; // to grab highest two mags
	static final double mag_range = 0.01; // don't grab neighbor magnitudes
	
	static final boolean stripUCERF2Aleatory = true;
	
	private RuptureProbabilityModifier probMod;
	
	public CyberShakeMCErDeterministicCalc(CachedPeakAmplitudesFromDB amps2db, ERF erf,
			CyberShakeComponent component) {
		this.amps2db = amps2db;
		this.curves2db = new HazardCurve2DB(amps2db.getDBAccess());
		this.erf = erf;
		this.component = component;
		
		probMod = getProbMod(erf);
	}
	
	static RuptureProbabilityModifier getProbMod(ERF erf) {
		if (stripUCERF2Aleatory && erf instanceof MeanUCERF2) {
			System.out.println("Stripping UCERF2 aleatory variability");
			return new UCERF2_AleatoryMagVarRemovalMod(erf);
		}
		return null;
	}
	
	public void setRupProbMod(RuptureProbabilityModifier probMod) {
		this.probMod = probMod;
	}
	
	public RuptureProbabilityModifier getRupProbMod() {
		return probMod;
	}
	
	static List<Integer> getRupIDsForDeterm(ERF erf, int sourceID, RuptureProbabilityModifier probMod) {
		ProbEqkSource source = erf.getSource(sourceID);
		
		List<Integer> rupIDs = Lists.newArrayList();
		// weed out zero prob ruptures, including any modifier if needed
		for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
			ProbEqkRupture rup = source.getRupture(rupID);
			double prob = rup.getProbability();
			if (probMod != null)
				prob = probMod.getModifiedProb(sourceID, rupID, prob);
			if (prob > 0)
				rupIDs.add(rupID);
		}
		
		return rupIDs;
	}

	@Override
	public Map<Double, DeterministicResult> calc(Site site, Collection<Double> periods) {
		Preconditions.checkArgument(site instanceof CyberShakeSiteRun,
				"CS MCEr calcs can only be called with CyberShakeSiteRun instances");
		CyberShakeSiteRun siteRun = (CyberShakeSiteRun)site;
		int runID = siteRun.getCS_Run().getRunID();
		

		List<CybershakeIM> ims = CyberShakeMCErProbabilisticCalc.getIMsForPeriods(
				amps2db.getDBAccess(), component, periods);
		CyberShakeMCErProbabilisticCalc.validateIMs(ims);
		
		Map<Double, DeterministicResult> result = Maps.newHashMap();
		
		for (CybershakeIM im : ims) {
			double period = PeakAmplitudesFromDB.getCleanedCS_Period(im.getVal());
			// make sure that we have amplitudes by checking for a hazard curve
			int curveID = curves2db.getHazardCurveID(runID, im.getID());
			if (curveID < 0) {
				System.out.println("Skipping period "+period+" for site "+site.getName()+", no amplitudes exist");
				result.put(period, null);
				continue;
			}
			
			try {
				result.put(period, calc(runID, im));
			} catch (SQLException e) {
				ExceptionUtils.throwAsRuntimeException(e);
			}
		}
		
		return result;
	}
	
	public DeterministicResult calc(int runID, CybershakeIM im) throws SQLException {
		double[][][] vals = amps2db.getAllIM_Values(runID, im);
		
		Preconditions.checkState(vals.length == erf.getNumSources(), "num sources inconsistant!");
		
		int maxRupsIncluded = -1;
		int maxNumAmps = -1;
		double[] boundingAmps = null;
		int numBelow = -1;
		int numAbove = -1;
		
		DeterministicResult maxVal = null;
		for (int sourceID=0; sourceID<vals.length; sourceID++) {
			// peak amp values for each rupture in this source
			double[][] rupVals = vals[sourceID];
			if (rupVals == null)
				// rup not applicable for site
				continue;
			
			ProbEqkSource source = erf.getSource(sourceID);
			Preconditions.checkState(rupVals.length == source.getNumRuptures(),
					"Rupture count inconsistant for source "+sourceID+": "+rupVals.length+" != "+source.getNumRuptures());
			
			List<Integer> rupIDs = getRupIDsForDeterm(erf, sourceID, probMod);
			
			// find the maximum magnitude of this source
			double maxMag = 0d;
			int maxRupID = -10;
			for (int rupID : rupIDs) {
				if (rupVals[rupID] == null)
					// this rup is more than cutoff dist away
					continue;
				ProbEqkRupture rup = source.getRupture(rupID);
				if (rup.getMag() > maxMag) {
					maxMag = rup.getMag();
					maxRupID = rupID;
				}
			}
			Preconditions.checkState(maxMag > 0);
			// we will consider all ruptures with M>=(maxMag - magRange)
			double minMag = maxMag - mag_range;
			
			// find all peak amplitudes that match this criteria
			// units are cm/sec^2
			List<Double> ampsToInclude = Lists.newArrayList();
			int rupsIncluded = 0;
			for (int rupID : rupIDs) {
				if (rupVals[rupID] == null)
					// this rup is more than cutoff dist away
					continue;
				ProbEqkRupture rup = source.getRupture(rupID);
				if (rup.getMag() >= minMag) {
					ampsToInclude.addAll(Doubles.asList(rupVals[rupID]));
					rupsIncluded++;
				}
			}
			Preconditions.checkState(!ampsToInclude.isEmpty());
			
			double val = percentile(ampsToInclude, percentile);
			if (maxVal == null || val > maxVal.getVal()) {
				maxVal = new DeterministicResult(sourceID, maxRupID, maxMag, source.getName(), val);
				maxRupsIncluded = rupsIncluded;
				maxNumAmps = ampsToInclude.size();
				Collections.sort(ampsToInclude);
				boundingAmps = new double[] { Double.NaN, Double.NaN };
				for (int i=0; i<ampsToInclude.size(); i++) {
					double amp = ampsToInclude.get(i);
					if (amp <= val)
						boundingAmps[0] = amp;
					if (amp >= val) {
						numAbove = ampsToInclude.size()-i;
						numBelow = ampsToInclude.size()-numAbove;
						boundingAmps[1] = amp;
						break;
					}
				}
			}
		}
		
		// convert to G
		double maxValCM = maxVal.getVal();
		maxVal.setVal(maxValCM / HazardCurveComputation.CONVERSION_TO_G);
		
//		System.out.println("RunID="+runID+", IM: "+im+", percentile="+percentile+", magRange="+mag_range);
//		System.out.println("CS Det Max: "+maxVal.getVal()+" g = "+maxValCM+" cm/s^2");
//		System.out.println("Source("+maxVal.getSourceID()+","+maxVal.getRupID()+"): "+maxVal.getSourceName()
//				+" (Mmax="+(float)+maxVal.getMag()+")");
//		System.out.println(maxRupsIncluded+" rups and "+maxNumAmps+" amps used. "
//				+ "Bounding amps: ["+boundingAmps[0]+","+boundingAmps[1]+"]. "+numBelow+" below, "+numAbove+" above.");
		
		return maxVal;
	}
	
	private static double percentile(List<Double> amps, double percentile) {
		// apache commons is apparently very efficient but gets weird for small sample sizes
//		return StatUtils.percentile(Doubles.toArray(amps), percentile);
		
		// linearly assign percentiles to each data value, then interpolate to find the value at the given percentile
		Collections.sort(amps);
		ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<amps.size(); i++) {
			double amp = amps.get(i);
			double x = 100d*(double)i/((double)amps.size()-1);
			func.set(x, amp);
		}
		
		return func.getInterpolatedY(percentile);
	}

	public static void main(String[] args) {
		File baseDir = new File("/home/kevin/CyberShake/MCER");
		File cacheDir = new File(baseDir, ".amps_cache");
		if (!cacheDir.exists())
			cacheDir.mkdir();
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		ERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, cacheDir, erf);
		
		int runID = 2657;
		CybershakeIM im = new CybershakeIM(146, IMType.SA, 3d, null, CyberShakeComponent.RotD100);
//		CybershakeIM im = new CybershakeIM(142, IMType.SA, 5d, null, CyberShakeComponent.RotD100);
//		CybershakeIM im = new CybershakeIM(138, IMType.SA, 7.5d, null, CyberShakeComponent.RotD100);
		
		CyberShakeMCErDeterministicCalc calc = new CyberShakeMCErDeterministicCalc(amps2db, erf, im.getComponent());
		
		try {
			calc.calc(runID, im);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			db.destroy();
		}
		
		System.exit(0);
	}

}
