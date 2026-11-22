package org.opensha.sha.cybershake.calc.mcer;

import static org.junit.Assert.*;

import org.junit.Test;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;

public class UGMS_HazardCurveCompareTest {

	@Test
	public void sa2in50InterpolatesInLogLog() {
		// log-log linear: P = 1e-2 * (SA/0.1)^-2, so SA = 0.1 * sqrt(1e-2/P)
		ArbitrarilyDiscretizedFunc curve = new ArbitrarilyDiscretizedFunc();
		curve.set(0.1, 1e-2);
		curve.set(1d, 1e-4);
		double expected = 0.1*Math.sqrt(1e-2/UGMS_HazardCurveCompare.PROB_2_IN_50);
		assertEquals(expected, UGMS_HazardCurveCompare.getSA_2in50(curve), expected*1e-6);
	}

	@Test
	public void sa2in50IsNaNWhenCurveDoesNotSpanLevel() {
		ArbitrarilyDiscretizedFunc curve = new ArbitrarilyDiscretizedFunc();
		curve.set(0.1, 1e-1);
		curve.set(1d, 1e-2);
		assertTrue(Double.isNaN(UGMS_HazardCurveCompare.getSA_2in50(curve)));
	}

	@Test
	public void describeRatiosNamesWhereCyberShakeIsAboveAndBelow() {
		double[] periods = {2d, 3d, 7.5};
		assertEquals("CyberShake RotD50 is 0.50 to 1.40 times USGS at the 2% in 50 year level: below USGS at 2.0 s "
				+ "and above it at 3.0, 7.5 s.",
				UGMS_HazardCurveCompare.describeRatios(periods, new double[] {0.5, 1.2, 1.4}));
		assertTrue(UGMS_HazardCurveCompare.describeRatios(periods, new double[] {1.1, Double.NaN, 2d})
				.endsWith("1.10 to 2.00 times USGS at the 2% in 50 year level, above USGS at every period."));
		assertTrue(UGMS_HazardCurveCompare.describeRatios(periods, new double[] {0.5, 0.6, 0.7})
				.endsWith(", below USGS at every period."));
		assertTrue(UGMS_HazardCurveCompare.describeRatios(periods, new double[] {Double.NaN, Double.NaN, Double.NaN})
				.startsWith("No period"));
	}

}
