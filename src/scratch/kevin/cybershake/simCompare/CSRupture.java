package scratch.kevin.cybershake.simCompare;

import java.util.Map;

import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.earthquake.ProbEqkRupture;

import com.google.common.base.Preconditions;

public class CSRupture {
	private int erfID;
	private int rvScenID;
	private int sourceID;
	private int rupID;
	private ProbEqkRupture rup;
	private int numRVs;
	private double rate;
	private double timeYears;
	
	private Location[] hypos;
	
	public CSRupture(int erfID, int rvScenID, int sourceID, int rupID, ProbEqkRupture rup, int numRVs) {
		this(erfID, rvScenID, sourceID, rupID, rup, numRVs, Double.NaN);
	}
	
	public CSRupture(int erfID, int rvScenID, int sourceID, int rupID, ProbEqkRupture rup, int numRVs, double timeYears) {
		this.erfID = erfID;
		this.rvScenID = rvScenID;
		this.sourceID = sourceID;
		this.rupID = rupID;
		this.rup = rup;
		this.numRVs = numRVs;
		this.timeYears = timeYears;
		rate = rup.getMeanAnnualRate(1d);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + rupID;
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
		CSRupture other = (CSRupture) obj;
		if (rupID != other.rupID)
			return false;
		if (sourceID != other.sourceID)
			return false;
		return true;
	}

	public int getSourceID() {
		return sourceID;
	}

	public int getRupID() {
		return rupID;
	}

	public ProbEqkRupture getRup() {
		return rup;
	}

	public int getNumRVs() {
		return numRVs;
	}

	public double getRate() {
		return rate;
	}
	
	public Location getHypocenter(int rv, ERF2DB erf2db) {
		if (hypos == null) {
			synchronized (this) {
				if (hypos != null)
					return hypos[rv];
				Map<Integer, Location> hypoMap = erf2db.getHypocenters(erfID, sourceID, rupID, rvScenID);
				Preconditions.checkState(hypoMap != null && !hypoMap.isEmpty(),
						"No hypos for %s, %s, %s, %s", erfID, rvScenID, sourceID, rupID);
				Preconditions.checkState(hypoMap.size() == numRVs,
						"Hypo size inconsistent. Have %s, expected %s", hypoMap.size(), numRVs);
				hypos = new Location[numRVs];
				for (int i=0; i<numRVs; i++)
					hypos[i] = hypoMap.get(i);
			}
		}
		return hypos[rv];
	}
	
	public double getRuptureTimeYears() {
		return timeYears;
	}
}
