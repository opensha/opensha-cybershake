package org.opensha.sha.cybershake.calc;

import java.util.List;
import java.util.Map;

import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;


/**
 * This interface is for calculating curves where rupture variations are not uniformly weighted.
 * 
 * It was originally created for a calculation concerning a temporary increase in hazard
 * for southern-nucleating SAF ruptures during the Bombay Beach swarm in Spring, 2009. 
 * 
 * @author kevin
 *
 */
public interface RuptureVariationProbabilityModifier {
	
	/**
	 * This method is used to weight individual rupture variations non-uniformly. It should return
	 * null if the probabilities are not to be modified, otherwise a list of probabilities for each
	 * rupture variation. The sum of these rupture variation probabilities will often be equal to
	 * originalProb, but doesn't have to. You can also modify the total rupture probability.<br>
	 * <br>
	 * Individual probabilities can be set to zero, and returning a list where each value is 
	 * originalProb/numRVs is equivelant to returning null.
	 * 
	 * @param sourceID
	 * @param rupID
	 * @param originalProb original total rupture probability
	 * @param run
	 * @param im
	 * @return
	 */
	public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb,
			CybershakeRun run, CybershakeIM im);
}
