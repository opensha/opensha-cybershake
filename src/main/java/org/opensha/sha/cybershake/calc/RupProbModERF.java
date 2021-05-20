package org.opensha.sha.cybershake.calc;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.TimeSpan;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.base.Preconditions;

/**
 * Wrapper ERF that allows us to apply a CyberShake RuptureProbabilityModifier to an ERF
 * for use in GMPE calculations.
 * 
 * @author kevin
 *
 */
public class RupProbModERF extends AbstractERF {
	
	private ERF erf;
	private RuptureProbabilityModifier probMod;
	
	public RupProbModERF(ERF erf, RuptureProbabilityModifier probMod) {
		Preconditions.checkNotNull(erf);
		Preconditions.checkNotNull(probMod);
		
		this.erf = erf;
		this.probMod = probMod;
	}

	@Override
	public int getNumSources() {
		return erf.getNumSources();
	}

	@Override
	public ProbEqkSource getSource(int idx) {
		return new ModSource(idx, erf.getSource(idx));
	}
	
	private class ModSource extends ProbEqkSource {
		
		private int sourceID;
		private ProbEqkSource origSource;
		
		public ModSource(int sourceID, ProbEqkSource origSource) {
			this.sourceID = sourceID;
			this.origSource = origSource;
		}

		@Override
		public String getName() {
			return origSource.getName();
		}

		@Override
		public String getInfo() {
			return origSource.getInfo();
		}

		@Override
		public LocationList getAllSourceLocs() {
			return origSource.getAllSourceLocs();
		}

		@Override
		public RuptureSurface getSourceSurface() {
			return origSource.getSourceSurface();
		}

		@Override
		public double getMinDistance(Site site) {
			return origSource.getMinDistance(site);
		}

		@Override
		public int getNumRuptures() {
			return origSource.getNumRuptures();
		}

		@Override
		public ProbEqkRupture getRupture(int nRupture) {
			ProbEqkRupture rup = origSource.getRupture(nRupture);
			double origProb = rup.getProbability();
			double modProb = probMod.getModifiedProb(sourceID, nRupture, origProb);
			if (origProb == modProb)
				return rup;
			rup = (ProbEqkRupture)rup.clone();
			rup.setProbability(modProb);
			return rup;
		}
		
	}

	@Override
	public void updateForecast() {
		erf.updateForecast();
	}

	@Override
	public String getName() {
		return erf.getName();
	}

	@Override
	public Parameter getParameter(String paramName) {
		return erf.getAdjustableParameterList().getParameter(paramName);
	}

	@Override
	public void setTimeSpan(TimeSpan time) {
		erf.setTimeSpan(time);
	}

	@Override
	public TimeSpan getTimeSpan() {
		return erf.getTimeSpan();
	}

	@Override
	public void setParameter(String name, Object value) {
		erf.setParameter(name, value);
	}

	@Override
	public ParameterList getAdjustableParameterList() {
		return erf.getAdjustableParameterList();
	}

}
