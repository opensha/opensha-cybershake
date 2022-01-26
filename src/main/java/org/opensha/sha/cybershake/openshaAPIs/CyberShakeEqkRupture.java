package org.opensha.sha.cybershake.openshaAPIs;

import org.opensha.commons.exceptions.InvalidRangeException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.FaultUtils;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.RuptureSurface;

public class CyberShakeEqkRupture extends ProbEqkRupture {
	
	private int srcID = 0;
	private int rupID = 0;
	private int erfID = 0;
	
	private ERF2DB erf2db = null;

	public CyberShakeEqkRupture(
			double mag, double prob,
			CyberShakeEvenlyGriddedSurface ruptureSurface,
			Location hypocenterLocation, int srcID, int rupID,
			int erfID) throws InvalidRangeException{
		this.mag = mag;
		this.probability = prob;
		FaultUtils.assertValidRake(aveRake);
		this.hypocenterLocation = hypocenterLocation;
		this.aveRake = 0;
		this.ruptureSurface = ruptureSurface;
		
		this.srcID = srcID;
		this.rupID = rupID;
		this.erfID = erfID;
	}
	
	public CyberShakeEqkRupture(
			double mag, double prob,
			Location hypocenterLocation, int srcID, int rupID,
			int erfID, ERF2DB erf2db) throws InvalidRangeException{
		this(mag, prob, null, hypocenterLocation, srcID, rupID, erfID);
		
		this.erf2db = erf2db;
	}
	
	public CyberShakeEqkRupture(ProbEqkRupture rup, int srcID, int rupID, int erfID) {
		this.mag = rup.getMag();
		this.probability = rup.getProbability();
		this.hypocenterLocation = rup.getHypocenterLocation();
		this.aveRake = rup.getAveRake();
		this.ruptureSurface = rup.getRuptureSurface();
		
		this.srcID = srcID;
		this.rupID = rupID;
		this.erfID = erfID;
	}
	
	

	@Override
	public RuptureSurface getRuptureSurface() {
		if (ruptureSurface == null && erf2db != null) {
			ruptureSurface = erf2db.getRuptureSurface(erfID, srcID, rupID);
		}
		return ruptureSurface;
	}

	public int getSrcID() {
		return srcID;
	}

	public int getRupID() {
		return rupID;
	}

	public int getErfID() {
		return erfID;
	}

}
