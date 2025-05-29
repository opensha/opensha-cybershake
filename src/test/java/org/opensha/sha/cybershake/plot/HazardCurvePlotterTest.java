package org.opensha.sha.cybershake.plot;

import static org.junit.Assert.*;

import org.junit.Test;

public class HazardCurvePlotterTest {
	
	// -o /Users/bhatthal/git/opensha-cybershake-fork/src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter  -R 215
	
	// -o /Users/bhatthal/git/opensha-cybershake-fork/src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter  -R 215 -rv /Users/bhatthal/git/opensha-cybershake-fork/src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter/USC_mod_probs_Mojave_Coachella.csv

	// We can compare using the Point2DComparator and set tolerance for how "approximate".

	// Hazard Curve x,y points for the plots are consistent when fetching from DB
	@Test
	public void fetchConsistentCurves() {
		fail("Not yet implemented"); // TODO
	}
	
	// Hazard Curve x,y points are approximately equal when building
	@Test
	public void calculateConsistentCurves() {
		fail("Not yet implemented"); // TODO
	}
	
	// Approximately equal when building with modified rupture variation probabilities
	@Test
	public void calculateConsistentCurvesModifiedRupVarProb() {
		fail("Not yet implemented"); // TODO
	}
	
	// Different when comparing modified rupvar probs to fetched curve.
	@Test
	public void differentCurvesWithModifiedRupVarProb() {
		fail("Not yet implemented"); // TODO
	}
	

}
