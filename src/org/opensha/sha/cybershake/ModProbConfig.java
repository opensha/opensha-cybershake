package org.opensha.sha.cybershake;

import java.util.Date;

import org.opensha.commons.data.Named;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;

public interface ModProbConfig extends Named {
	
	public int getProbModelID();
	
	public int getTimeSpanID();
	
	public RuptureProbabilityModifier getRupProbModifier();
	
	public RuptureVariationProbabilityModifier getRupVarProbModifier();
	
	public int getHazardDatasetID(int erfID, int rvScenID, int sgtVarID,
			int velModelID, Date timeSpanDate);

}
