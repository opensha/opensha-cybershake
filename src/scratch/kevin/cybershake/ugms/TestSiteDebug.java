package scratch.kevin.cybershake.ugms;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.util.Interpolate;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.mcer.ASCEDetLowerLimitCalc;
import org.opensha.sha.calc.mcer.AbstractMCErProbabilisticCalc;
import org.opensha.sha.cybershake.calc.mcer.MCERDataProductsCalc;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.SiteParams.DepthTo1pt0kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.DepthTo2pt5kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;
import org.opensha.sha.imr.param.SiteParams.Vs30_TypeParam;
import org.opensha.sha.util.component.ShahiBaker2014Trans;

import scratch.UCERF3.erf.mean.MeanUCERF3;
import scratch.UCERF3.erf.mean.MeanUCERF3.Presets;

public class TestSiteDebug {

	public static void main(String[] args) {
		MeanUCERF3 erf = new MeanUCERF3();
		erf.setPreset(Presets.COMPLETE_MODEL);
//		erf.setPreset(Presets.BOTH_FM_MAG_VAR);
		erf.getTimeSpan().setDuration(1d);
		erf.updateForecast();
//		ScalarIMR gmpe = AttenRelRef.NGAWest_2014_AVG.instance(null);
		ScalarIMR gmpe = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		gmpe.setParamDefaults();
		
		// Ken & Marty's site
////		Site site = new Site(new Location(34.06, -118.2312)); // orig test site
//		Site site = new Site(new Location(34.06, -118.23)); // snapped test site
//		
//		double targetVs30 = 415;
//		double lowerVs30 = 366;
//		double upperVs30 = 465;
//		Double z10 = 350;
//		Double z25 = 2.2;
		
		// CB & Sanaz's site
		Site site = new Site(new Location(34.17, -118.53)); // snapped test site
		
		double targetVs30 = 260;
		double lowerVs30 = 229;
		double upperVs30 = 274;
		// USGS Z values
		Double z10 = 216d;
		Double z25 = 1.782;
		// UGMS Z values
//		Double z10 = 250d;
//		Double z25 = 1.8;
		// default Z model
//		Double z10 = null;
//		Double z25 = null;
		
		double period = 5d;
		String vsType = Vs30_TypeParam.VS30_TYPE_INFERRED;
		
		double[] vs30s = {targetVs30, lowerVs30, upperVs30};
		
		gmpe.setIntensityMeasure(SA_Param.NAME);
		SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
		
		for (Parameter<?> param : gmpe.getSiteParams()) {
			if (param.getName().equals(Vs30_Param.NAME))
				((Parameter<Double>)param).setValue(new Double(targetVs30));
			if (param.getName().equals(DepthTo1pt0kmPerSecParam.NAME))
				((Parameter<Double>)param).setValue(z10);
			if (param.getName().equals(DepthTo2pt5kmPerSecParam.NAME))
				((Parameter<Double>)param).setValue(z25);
			if (param.getName().equals(Vs30_TypeParam.NAME))
				((Parameter<String>)param).setValue(vsType);
			System.out.println(param.getName()+": "+param.getValue());
			site.addParameter(param);
		}
		
		DiscretizedFunc periodsFunc = new ArbitrarilyDiscretizedFunc();
		for (double p : new double[] {0.01,0.02,0.03,0.05,0.075,0.1,0.15,0.2,0.25,0.3,0.4,
				0.5,0.75,1.0,1.5,2.0,3.0,4.0,5.0,7.5,10.0})
			periodsFunc.set(p, 0d);
		DiscretizedFunc lowerLimit = ASCEDetLowerLimitCalc.calc(periodsFunc,
				site.getParameter(Double.class, Vs30_Param.NAME).getValue(), site.getLocation());
		System.out.println("Lower limit");
		System.out.println(lowerLimit);
		System.exit(0);
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		
		ShahiBaker2014Trans trans = new ShahiBaker2014Trans();
//		DiscretizedFunc myXVals = new ArbitrarilyDiscretizedFunc();
//		double ratio = trans.getScalingFactor(period);
//		for (Point2D pt : xVals)
//			myXVals.set(pt.getX()/ratio, 0d);
//		xVals = myXVals;
		
		DiscretizedFunc logXVals = new ArbitrarilyDiscretizedFunc();
		for (Point2D pt : xVals)
			logXVals.set(Math.log(pt.getX()), 1d);
		
		HazardCurveCalculator calc = new HazardCurveCalculator();
		
		double lowerRTGM = Double.NaN;
		double upperRTGM = Double.NaN;
		
		for (double vs30 : vs30s) {
			site.getParameter(Vs30_Param.NAME).setValue(vs30);
			calc.getHazardCurve(logXVals, site, gmpe, erf);
			for (int i=0; i<xVals.size(); i++)
				xVals.set(i, logXVals.getY(i));
			
			System.out.println("Vs30: "+vs30);
			
//			System.out.println("RotD50 Curve");
//			System.out.println(xVals);
			
			double rtgm = AbstractMCErProbabilisticCalc.calcRTGM(xVals);
			
//			System.out.println("RD50 RTGM: "+rtgm);
			
			DiscretizedFunc rd100Curve = trans.convertCurve(xVals, period);
			
//			System.out.println("RotD100 Curve");
//			System.out.println(rd100Curve);
			
			double rtgm100 = AbstractMCErProbabilisticCalc.calcRTGM(rd100Curve);
			
			System.out.println("RD100 RTGM: "+rtgm100);
			
			if (vs30 == lowerVs30)
				lowerRTGM = rtgm100;
			if (vs30 == upperVs30)
				upperRTGM = rtgm100;
		}
		System.out.println("Interpolation:");
		double[] xs = { lowerVs30, upperVs30 };
		double[] ys = { lowerRTGM, upperRTGM };
		double interpRTGM = Interpolate.findLogLogY(xs, ys, targetVs30);
		System.out.println("RD100 RTGM: "+interpRTGM);
	}

}
