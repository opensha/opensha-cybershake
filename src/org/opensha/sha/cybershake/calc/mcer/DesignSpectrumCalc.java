package org.opensha.sha.cybershake.calc.mcer;

import java.awt.geom.Point2D;
import java.util.List;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;

import com.google.common.collect.Lists;

public class DesignSpectrumCalc {
	
	private static final double minX = 0.01;
	private static final double maxX = 10d;
	private static final double deltaX = 0.01;
	private static final int numX = (int)((maxX - minX)/deltaX) + 1;
	
	/**
	 * Calculates spectrum from either SMS/SM1 or SDS/SD1 values
	 * @param sxs either SMS or SDS
	 * @param sx1 either SM1 or SD1
	 * @param tl
	 * @return
	 */
	public static DiscretizedFunc calcSpectrum(double sxs, double sx1, double tl) {
		double ts = sx1/sxs;
		double t0 = 0.2*ts;
		
		double yIntercept = 0.4*sxs;
		double initialSlope = (sxs - yIntercept)/(t0);
		
		System.out.println("Calculating spectrum with SxS="+sxs+", Sx1="+sx1+", TL="+tl+", TS="+ts+", T0="+t0+", yInt="+yIntercept);
		
		EvenlyDiscretizedFunc evenXVals = new EvenlyDiscretizedFunc(minX, numX, deltaX);
		List<Double> xVals = Lists.newArrayList();
		for (Point2D pt : evenXVals)
			xVals.add(pt.getX());
		
		xVals.add(tl);
		xVals.add(ts);
		xVals.add(t0);
		xVals.add(1d);
		
		ArbitrarilyDiscretizedFunc spectrum = new ArbitrarilyDiscretizedFunc();
		
		for (double x : xVals) {
			double y;
			if (x <= t0) {
				// linear from yIntercept to SxS
				y = initialSlope*x + yIntercept;
			} else if (x <= ts) {
				y = sxs;
			} else if (x <= tl) {
				y = sx1/x;
			} else {
				y = sx1*tl/(x*x);
			}
			spectrum.set(x, y);
		}
		
		return spectrum;
	}

}
