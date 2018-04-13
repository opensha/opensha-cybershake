package scratch.kevin.cybershake.simCompare;

import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;

public class StudyModifiedProbRotDProvider extends StudyRotDProvider {
	
	private RuptureProbabilityModifier probMod;

	public StudyModifiedProbRotDProvider(StudyRotDProvider prov, RuptureProbabilityModifier probMod, String name) {
		super(prov, name);
		
		this.probMod = probMod;
	}

	@Override
	public double getAnnualRate(CSRupture rupture) {
		double origProb = rupture.getRup().getProbability();
		double newProb = probMod.getModifiedProb(rupture.getSourceID(), rupture.getRupID(), origProb);
		if (newProb == 0d)
			return 0d;
		return -Math.log(1d - newProb);
	}

}
