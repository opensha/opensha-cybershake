package scratch.kevin.cybershake;

import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;

public class MinRupProbMod implements RuptureProbabilityModifier {
	
	private RuptureProbabilityModifier[] mods;

	public MinRupProbMod(RuptureProbabilityModifier... mods) {
		this.mods = mods;
		
	}

	@Override
	public double getModifiedProb(int sourceID, int rupID, double origProb) {
		double min = 1d;
		for (RuptureProbabilityModifier mod : mods)
			min = Math.min(min, mod.getModifiedProb(sourceID, rupID, origProb));
		return min;
	}

}
