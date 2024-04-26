package scratch.kevin.cybershake;

import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.FaultRuptureSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;

import com.google.common.base.Preconditions;

public class UCERF2_A_SegmentedOnlyProbMod implements RuptureProbabilityModifier {
	
	private ERF erf;

	public UCERF2_A_SegmentedOnlyProbMod(ERF erf) {
		this.erf = erf;
		Preconditions.checkState(erf instanceof MeanUCERF2);
	}

	@Override
	public double getModifiedProb(int sourceID, int rupID, double origProb) {
		ProbEqkSource source = erf.getSource(sourceID);
		if (source instanceof FaultRuptureSource && !source.getName().endsWith(" Char"))
			return origProb;
		return 0;
	}

}
