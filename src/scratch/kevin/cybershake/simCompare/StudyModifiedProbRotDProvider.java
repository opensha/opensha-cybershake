package scratch.kevin.cybershake.simCompare;

import java.util.List;

import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;

public class StudyModifiedProbRotDProvider extends StudyRotDProvider {
	
	private RuptureProbabilityModifier probMod;
	private RuptureVariationProbabilityModifier varProbMod;

	public StudyModifiedProbRotDProvider(StudyRotDProvider prov, RuptureProbabilityModifier probMod, String name) {
		this(prov, probMod, null, name);
	}

	public StudyModifiedProbRotDProvider(StudyRotDProvider prov, RuptureVariationProbabilityModifier varProbMod, String name) {
		this(prov, null, varProbMod, name);
	}

	public StudyModifiedProbRotDProvider(StudyRotDProvider prov, RuptureProbabilityModifier probMod,
			RuptureVariationProbabilityModifier varProbMod, String name) {
		super(prov, name);
		
		this.probMod = probMod;
		this.varProbMod = varProbMod;
	}

	@Override
	public double getAnnualRate(CSRupture rupture) {
		if (probMod == null)
			return super.getAnnualRate(rupture);
		double rupProb = rupture.getRup().getProbability();
		rupProb = probMod.getModifiedProb(rupture.getSourceID(), rupture.getRupID(), rupProb);
		if (rupProb == 0d)
			return 0d;
		return -Math.log(1d - rupProb);
	}

	@Override
	public double getIndividualSimulationRate(CSRupture rupture, double rupAnnualRate, int simulationIndex,
			int numSimulations) {
		if (varProbMod == null)
			return super.getIndividualSimulationRate(rupture, rupAnnualRate, simulationIndex, numSimulations);
		double rupProb = rupture.getRup().getProbability();
		List<Double> varProbs = varProbMod.getVariationProbs(rupture.getSourceID(), rupture.getRupID(), rupProb, null, null);
		return varProbs.get(simulationIndex);
	}

}
