package org.opensha.sha.cybershake.calc;

import static org.junit.Assert.*;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class RuptureVariationProbabilityModifierTest {
	
	// static members
	private static DBAccess db;
	private static List<Double> xVals;
	private static CybershakeRun testRun;
	private static CybershakeIM imType;
	
	private static CachedPeakAmplitudesFromDB cachedAmps2DB;
	
	// test instance members
	private HazardCurveComputation calc;
	
	@BeforeClass
	public static void setUpBeforeClass() {
		db = Cybershake_OpenSHA_DBApplication.getDB();
		
		xVals = Lists.newArrayList();
		for (Point2D pt : new IMT_Info().getDefaultHazardCurve(SA_Param.NAME))
			xVals.add(pt.getX());
		
		testRun = new Runs2DB(db).getRun(2657); // LADT 0.5 hz run
		imType = new HazardCurve2DB(db).getIMFromID(21); // 3sec SA GEOM
		
		cachedAmps2DB = new CachedPeakAmplitudesFromDB(db, null, MeanUCERF2_ToDB.createUCERF2ERF());
	}
	
	@AfterClass
	public static void tearDownAfterClass() {
		db.destroy();
	}
	
	@Before
	public void setUp() {
		calc = new HazardCurveComputation(db);
		calc.setPeakAmpsAccessor(cachedAmps2DB);
	}
	
	@Test
	public void testSourceOrder() {
		System.out.println("Testing source order");
		// simple test to make sure that source order isn't important
		int num = 10;
		double tol = 1e-6; // percent
		SiteInfo2DB siteDB = new SiteInfo2DB(db);
		List<Integer> srcIdList = Lists.newArrayList(siteDB.getSrcIdsForSite(testRun.getSiteID(), testRun.getERFID()));
		DiscretizedFunc origCurve = calc.computeHazardCurve(xVals, testRun, imType, srcIdList);
		for (int i=0; i<num; i++) {
			Collections.shuffle(srcIdList);
			DiscretizedFunc testCurve = calc.computeHazardCurve(xVals, testRun, imType, srcIdList);
			assertWithinTol(origCurve, testCurve, tol);
		}
	}

	@Test
	public void testTinyPerturbations() {
		System.out.println("Testing tiny perturbations");
		final double variationFract = 0.00001;
		double testMaxPDiff = 1e-3;
		int trials = 10;
		
		DiscretizedFunc origCurve = calc.computeHazardCurve(xVals, testRun, imType);
		calc.setRupVarProbModifier(new RuptureVariationProbabilityModifier() {
			
			@Override
			public List<Double> getVariationProbs(int sourceID, int rupID,
					double originalProb, CybershakeRun run, CybershakeIM im) {
				int numRVs;
				try {
					numRVs = cachedAmps2DB.getIM_Values(run.getRunID(), sourceID, rupID, im).size();
				} catch (SQLException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
				double probPer = originalProb/(double)numRVs;
				List<Double> probs = Lists.newArrayList();
				for (int i=0; i<numRVs; i++) {
					// random number that can be +/- half of the above variation percent of the actual prob value
					double r = (Math.random()-0.5) * probPer * variationFract;
					probs.add(probPer + r);
				}
				return probs;
			}
		});
		for (int i=0; i<trials; i++) {
			DiscretizedFunc modCurve = calc.computeHazardCurve(xVals, testRun, imType);
			
			assertWithinTol(origCurve, modCurve, testMaxPDiff);
		}
	}
	
	private static class OEFTestMod implements RuptureVariationProbabilityModifier {
		
		private int sourceToMod;
		private double totTriggerProbPerRup;
		private double fractToTrigger;
		
		private List<List<Double>> imValsToMod = Lists.newArrayList();
		
		private boolean bundledCalc = false;
		
		public OEFTestMod(int sourceToMod, double totTriggerProbPerRup, double fractToTrigger) {
			this.sourceToMod = sourceToMod;
			this.totTriggerProbPerRup = totTriggerProbPerRup;
			this.fractToTrigger = fractToTrigger;
		}
		
		@Override
		public List<Double> getVariationProbs(int sourceID, int rupID,
				double originalProb, CybershakeRun run, CybershakeIM im) {
			if (sourceID != sourceToMod)
				return null;
			List<Double> ims;
			try {
				ims = cachedAmps2DB.getIM_Values(run.getRunID(), sourceID, rupID, im);
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			// now store in new list since we'll be cutting this short
			ims = Lists.newArrayList(ims);
			int numRVs = ims.size();
			double probPer = originalProb/(double)numRVs;
			int numRVsToPromote = (int)(numRVs * fractToTrigger);
			ims = ims.subList(0, numRVsToPromote);
			while (rupID >= imValsToMod.size())
				imValsToMod.add(null);
			imValsToMod.set(rupID, ims);
			if (numRVsToPromote == 0)
				return null;
			double promotionPer = totTriggerProbPerRup/(double)numRVsToPromote;
			
			List<Double> probs = Lists.newArrayList();
			for (int i=0; i<numRVs; i++) {
				if (bundledCalc) {
					if (i < numRVsToPromote)
						probs.add(probPer + promotionPer);
					else
						probs.add(probPer);
				} else {
					if (i < numRVsToPromote)
						probs.add(promotionPer);
					else
						probs.add(0d);
				}
			}
			return probs;
		}
	}
	
	@Test
	public void testOEFScenario() {
		// this will test that it's safe to include really high probabilities for certain RVs instead of adding rupture
		System.out.println("Testing OEF Scenario");
		
		// we will artificially bump up 1/4 of the RVs for this scenario
		final int sourceToMod = 90; // San Andreas S. Mojave characteristic
		final double fractToTrigger = 0.25;
		final double totTriggerProbPerRup = 0.005;
		
		OEFTestMod mod = new OEFTestMod(sourceToMod, totTriggerProbPerRup, fractToTrigger);
		mod.bundledCalc = true;
		calc.setRupVarProbModifier(mod);
		calc.setRupVarAdditionProbModifier(null);
		DiscretizedFunc directModCurve = calc.computeHazardCurve(xVals, testRun, imType);
		directModCurve.setName("Bundled In");
		mod.bundledCalc = false;
		calc.setRupVarProbModifier(null);
		calc.setRupVarAdditionProbModifier(mod);
		DiscretizedFunc addModCurve = calc.computeHazardCurve(xVals, testRun, imType);
		addModCurve.setName("By Addition");
		
		// now do it by treating it as a separate source with high probability
		calc.setRupVarProbModifier(null);
		calc.setRupVarAdditionProbModifier(null);
		DiscretizedFunc separateSourceCurve = calc.computeHazardCurve(xVals, testRun, imType);
		DiscretizedFunc origCurve = separateSourceCurve.deepClone();
		// now we need to add the bumped up RVs
		
		// first undo the one minus that's applied at the end of the curve calc
		for(int j=0; j<separateSourceCurve.size(); ++j) 
			separateSourceCurve.set(separateSourceCurve.getX(j),(1-separateSourceCurve.getY(j)));
		// now add another source
		for (int rupID=0; rupID<mod.imValsToMod.size(); rupID++) {
			List<Double> imVals = mod.imValsToMod.get(rupID);
			int numRVs;
			try {
				numRVs = cachedAmps2DB.getIM_Values(testRun.getRunID(), sourceToMod, rupID, imType).size();
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			int numRVsToPromote = (int)(numRVs * fractToTrigger);
			Preconditions.checkState(numRVsToPromote == imVals.size(),
					numRVsToPromote+" != "+imVals.size()+" (numRVs="+numRVs+")");
			
			HazardCurveComputation.handleRupture(xVals, imVals, separateSourceCurve, totTriggerProbPerRup,
					sourceToMod, rupID, null, null, testRun, imType);
		}
		
		// now go one minus again
		for(int j=0; j<separateSourceCurve.size(); ++j) 
			separateSourceCurve.set(separateSourceCurve.getX(j),(1-separateSourceCurve.getY(j)));
		separateSourceCurve.setName("Separate Source");
		
		origCurve.setName("Unmodified");
		assertWithinTol(separateSourceCurve, addModCurve, 1e-8, origCurve, directModCurve);
	}
	
	private static final boolean plot_curves_on_failure = true;
	
	private static void assertWithinTol(DiscretizedFunc targetCurve, DiscretizedFunc testCurve, double maxPDiff,
			DiscretizedFunc... debugCurves) {
		assertEquals(targetCurve.size(), testCurve.size());
		
		double biggest = 0;
		
		for (int i=0; i<targetCurve.size(); i++) {
			double expected = targetCurve.getY(i);
			double actual = testCurve.getY(i);
			
			if (expected == 0 && actual == 0)
				continue;
			
			double pDiff = DataUtils.getPercentDiff(actual, expected);
			biggest = Math.max(biggest, pDiff);
			
			String message = "Curve mismatch at pt "+i+", x="+targetCurve.getX(i)+".";
			message += "\n\tTarget: Y="+expected;
			message += "\n\tTest: Y="+actual;
			message += "\n\tPDiff: "+pDiff;
			
			if (plot_curves_on_failure && pDiff > maxPDiff) {
				List<DiscretizedFunc> funcs = Lists.newArrayList();
				List<PlotCurveCharacterstics> chars = Lists.newArrayList();
				
				funcs.add(targetCurve);
				if (targetCurve.getName() == null)
					targetCurve.setName("Target");
				chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLACK));
				funcs.add(testCurve);
				if (testCurve.getName() == null)
					testCurve.setName("Test");
				chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLUE));
				
				List<Color> extraColors = Lists.newArrayList(Color.RED, Color.CYAN, Color.GREEN, Color.ORANGE, Color.YELLOW);
				
				if (debugCurves != null) {
					for (DiscretizedFunc curve : debugCurves) {
						funcs.add(curve);
						chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, extraColors.remove(0)));
					}
				}
				
				PlotSpec spec = new PlotSpec(funcs, chars, "Curve Test Failed", "IMT", "POE");
				spec.setLegendVisible(true);
				GraphWindow gw = new GraphWindow(spec);
				gw.setXLog(true);
				gw.setYLog(true);
				System.out.println("About to fail. Message:\n"+message);
				while (gw.isVisible()) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			assertTrue(message, pDiff <= maxPDiff);
		}
		System.out.println("Passed with max PDiff: "+biggest+" %");
	}

}
