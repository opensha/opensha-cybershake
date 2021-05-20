package org.opensha.sha.cybershake.eew;

import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;

public class ZeroProbMod implements RuptureProbabilityModifier {

	@Override
	public double getModifiedProb(int sourceID, int rupID, double origProb) {
		return 0;
	}

}
