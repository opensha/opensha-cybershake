package org.opensha.sha.cybershake.calc.mcer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.calc.mcer.CurveBasedMCErProbabilisitCalc;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.HazardDataset2DB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CyberShakeMCErProbabilisticCalc extends
	CurveBasedMCErProbabilisitCalc {
	
	private DBAccess db;
	private CyberShakeComponent component;
	
	private HazardCurve2DB curves2db;
	private HazardDataset2DB dataset2db;
	
	private static List<CybershakeIM> allIMs;
	
	public CyberShakeMCErProbabilisticCalc(DBAccess db, CyberShakeComponent component) {
		this.db = db;
		this.component = component;
		
		this.curves2db = new HazardCurve2DB(db);
		this.dataset2db = new HazardDataset2DB(db);
	}
	
	public static synchronized List<CybershakeIM> getIMsForPeriods(DBAccess db,
			CyberShakeComponent component, Collection<Double> periods) {
		Preconditions.checkNotNull(component, "Must supply component!");
		
		// map periods to IMs
		if (allIMs == null)
			allIMs = new PeakAmplitudesFromDB(db).getAllIMs();
		
		List<CybershakeIM> ims = Lists.newArrayList();
		
		for (double period : periods) {
			CybershakeIM match = null;
			
			// we multiply by 1000 and round for comparisons
			double periodCompare = PeakAmplitudesFromDB.getCleanedCS_Period(period);
			
			double closestPeriod = Double.NaN;
			double closestDiff = Double.POSITIVE_INFINITY;
			
			for (CybershakeIM im : allIMs) {
				if (im.getMeasure() != IMType.SA || im.getComponent() != component)
					// filter out IMs of the wrong type or component
					continue;
				
				double imPeriod = im.getVal();
				double delta = Math.abs(imPeriod - period);
				if (delta < closestDiff) {
					closestDiff = delta;
					closestPeriod = imPeriod;
				}
				
				// need to round CS periods
				double imPeriodCompare = PeakAmplitudesFromDB.getCleanedCS_Period(imPeriod);
				
				if ((float)periodCompare == (float)imPeriodCompare) {
					match = im;
					break;
				}
			}
			Preconditions.checkState(match != null,
					"No CyberShake IM match for %s. Closest: %s", period, closestPeriod);
			
			ims.add(match);
		}
		
		return ims;
	}
	
	/**
	 * validate that component is the same for each and no duplicate periods
	 * @param ims
	 */
	static void validateIMs(List<CybershakeIM> ims) {
		HashSet<Double> myPeriods = new HashSet<Double>();
		CyberShakeComponent comp = null;
		for (CybershakeIM im : ims) {
			Preconditions.checkNotNull(im.getComponent());
			if (comp == null)
				comp = im.getComponent();
			else
				Preconditions.checkState(comp == im.getComponent());
			Preconditions.checkState(!myPeriods.contains(im.getVal()));
			myPeriods.add(im.getVal());
		}
	}
	
	@Override
	public Map<Double, DiscretizedFunc> calcHazardCurves(Site site, Collection<Double> periods) {
		Preconditions.checkArgument(site instanceof CyberShakeSiteRun,
				"CS MCEr calcs can only be called with CyberShakeSiteRun instances");
		CyberShakeSiteRun siteRun = (CyberShakeSiteRun)site;
		
		// get IMs
		List<CybershakeIM> ims = getIMsForPeriods(db, component, periods);
		validateIMs(ims);
		
		List<Integer> curveIDs = curves2db.getAllHazardCurveIDs(siteRun.getCS_Run().getRunID(), -1);
		// filter out any oddball probability models
		int origSize = curveIDs.size();
		for (int i=curveIDs.size(); --i>=0;) {
			int datasetID = curves2db.getDatasetIDForCurve(curveIDs.get(i));
			int probModelID = dataset2db.getProbModelID(datasetID);
			if (probModelID != 1)
				// 1 is time independent poisson
				curveIDs.remove(i);
		}
		if (origSize != curveIDs.size())
			System.out.println((origSize-curveIDs.size())+"/"+origSize
					+" were filtered out due to Prob Model ID != 1");
		
		Map<CybershakeIM, Integer> imToCurveIDMap = Maps.newHashMap();
		
		for (int curveID : curveIDs)
			imToCurveIDMap.put(curves2db.getIMForCurve(curveID), curveID);
		
		Map<Double, DiscretizedFunc> curves = Maps.newHashMap();
		
		for (CybershakeIM im : ims) {
			Integer curveID = imToCurveIDMap.get(im);
			double period = PeakAmplitudesFromDB.getCleanedCS_Period(im.getVal());
			// chop of weirdness at, for example 3.00003
			if (curveID == null) {
				System.out.println("Skipping period "+period+" for site "+site.getName()+", no curve exists");
				continue;
			}
			DiscretizedFunc curve = curves2db.getHazardCurve(curveID);
			curves.put(period, curve);
		}
		
		return curves;
	}

	@Override
	public void setXVals(DiscretizedFunc xVals) {
		throw new UnsupportedOperationException("Cannot set X values for CyberShake calculation");
	}

}
