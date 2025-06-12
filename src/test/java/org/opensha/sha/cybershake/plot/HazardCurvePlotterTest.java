package org.opensha.sha.cybershake.plot;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;

public class HazardCurvePlotterTest {
	
	// TODO: Fix these tests to use relative paths
	final String RUN_ID = "9306";
	final String RUP_VAR_CSV = "/Users/bhatthal/git/opensha-cybershake-fork/src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter/USC_mod_probs_Mojave_Coachella.csv";
	final String REF_CSV = "/Users/bhatthal/git/opensha-cybershake-fork/src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter/USC_reference_probs.csv";
	final String OUTPUT_DIR = "/Users/bhatthal/git/opensha-cybershake-fork/src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter";
	final String PLOT_CHARS_FILE = "/Users/bhatthal/git/opensha-cybershake-fork/src/main/resources/org/opensha/sha/cybershake/conf/robPlot.xml";
	final String COMPONENT = "RotD50";

	HazardCurvePlotter plotter;
	
//	final String[] FETCH_ARGS = {
//			"-o", OUTPUT_DIR + "/fetchdb/1",
//			"-R", RUN_ID,
//			"-cmp", COMPONENT,
//			"--plot-chars-file", PLOT_CHARS_FILE,
//			"-t", "TXT,PDF"
//	};
	
	@Before
	public void setup() {
		// Create a HazardCurvePlotter
		this.plotter = new HazardCurvePlotter(Cybershake_OpenSHA_DBApplication.getDB());
		// Ensure directories {fetchdb,modprob,refprob}/{1,2} exist and no cached results
	}
	

	// TODO: Read curve data from TXT type output

	// TODO: Create CommandLine option,
	//       mock user input by manually passing String[] args.
	// plotCurvesFromOptions
	
	// TODO: We can compare using the Point2DComparator and set tolerance for how "approximate".

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
