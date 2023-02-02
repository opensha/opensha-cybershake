package org.opensha.sha.cybershake.openshaAPIs;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbDiscrEmpiricalDistFunc;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.exceptions.IMRException;
import org.opensha.commons.exceptions.ParameterException;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.param.constraint.impl.DoubleDiscreteConstraint;
import org.opensha.commons.param.event.ParameterChangeEvent;
import org.opensha.commons.param.event.ParameterChangeListener;
import org.opensha.commons.param.event.ParameterChangeWarningListener;
import org.opensha.commons.param.impl.IntegerParameter;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.DampingParam;
import org.opensha.sha.imr.param.IntensityMeasureParams.PeriodParam;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncLevelParam;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncTypeParam;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Doubles;

public class CyberShakeIMR extends AttenuationRelationship implements ParameterChangeListener {

	public static final String NAME = "CyberShake Fake Attenuation Relationship";
	public static final String SHORT_NAME = "CyberShakeIMR";

	/** ParameterList of all Site parameters */
	protected ParameterList siteParams = new ParameterList();

	/** ParameterList of all eqkRupture parameters */
	protected ParameterList eqkRuptureParams = new ParameterList();

	boolean dbConnInitialized = false;
	private PeakAmplitudesFromDB ampsDB = null;
	private HazardCurve2DB curvesDB;
	
	private IntegerParameter runIDParam;
	private int curRunID;
	
	private IntegerParameter imTypeIDParam;
	private CybershakeIM curIM = null;

	private boolean isInitialized;
	
	private LoadingCache<CacheKey, List<Double>> imsCache;
	
	private class CacheKey {
		private final int runID;
		private final int sourceID;
		private final int rupID;
		private final CybershakeIM im;
		public CacheKey(int runID, int sourceID, int rupID, CybershakeIM im) {
			super();
			this.runID = runID;
			this.sourceID = sourceID;
			this.rupID = rupID;
			this.im = im;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((im == null) ? 0 : im.hashCode());
			result = prime * result + runID;
			result = prime * result + rupID;
			result = prime * result + sourceID;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CacheKey other = (CacheKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (im == null) {
				if (other.im != null)
					return false;
			} else if (!im.equals(other.im))
				return false;
			if (runID != other.runID)
				return false;
			if (rupID != other.rupID)
				return false;
			if (sourceID != other.sourceID)
				return false;
			return true;
		}
		private CyberShakeIMR getOuterType() {
			return CyberShakeIMR.this;
		}
	}

	public CyberShakeIMR(ParameterChangeWarningListener listener) {
		this(listener, null);
	}

	public CyberShakeIMR(ParameterChangeWarningListener listener, DBAccess db) {
		this(listener, db, null);
	}

	public CyberShakeIMR(ParameterChangeWarningListener listener, DBAccess db, PeakAmplitudesFromDB ampsDB) {
		super();
		isInitialized = false;
		//		loading = true;
		initSupportedIntensityMeasureParams();

		initOtherParams();

		if (db != null)
			initDB(db, ampsDB);
	}

	private void checkInit() {
		// we don't want to initilize the DB connection until we know the user actually wants to use this
		if (!isInitialized) {
			System.out.println("Initializing CyberShake IMR!");
			if (!dbConnInitialized)
				this.initDB();

			// Run ID
			runIDParam = new IntegerParameter("Run ID", -1);
			runIDParam.addParameterChangeListener(this);
			this.curRunID = runIDParam.getValue();
			
			// IM Type ID
			imTypeIDParam = new IntegerParameter("IM Type ID", -1);
			imTypeIDParam.addParameterChangeListener(this);
			this.curIM = null;

			ParameterList[] listsToAdd = { otherParams, imlAtExceedProbIndependentParams,
					exceedProbIndependentParams, meanIndependentParams, stdDevIndependentParams };

			for (ParameterList paramList : listsToAdd) {
				paramList.addParameter(runIDParam);
				paramList.addParameter(imTypeIDParam);
			}
			
			CacheLoader<CacheKey, List<Double>> loader = new CacheLoader<CacheKey, List<Double>>() {

				@Override
				public List<Double> load(CacheKey key) throws Exception {
					List<Double> ret = ampsDB.getIM_Values(key.runID, key.sourceID, key.rupID, key.im);
					if (ret == null)
						ret = new ArrayList<>();
					else
						for (int i=0; i<ret.size(); i++)
							ret.set(i, ret.get(i)/HazardCurveComputation.CONVERSION_TO_G);
					return ret;
				}
				
			};
			
			imsCache = CacheBuilder.newBuilder().maximumSize(IM_VALS_BUFF_SIZE).build(loader);

			isInitialized = true;
		}
	}

	private void initDB() {
		initDB(null, null);
	}

	private void initDB(DBAccess db, PeakAmplitudesFromDB ampsDB) {
		if (db == null)
			db = Cybershake_OpenSHA_DBApplication.getDB();
		if (ampsDB == null)
			ampsDB = new PeakAmplitudesFromDB(db);
		this.ampsDB = ampsDB;
		this.curvesDB = new HazardCurve2DB(db);
		dbConnInitialized = true;
	}

	@Override
	public void setSite(Site site) {
		// do nothing
	}

	@Override
	public double getExceedProbability() throws ParameterException,
	IMRException {
		double iml = (Double)this.getIntensityMeasureLevel();
		return getExceedProbability(iml);
	}

	private CyberShakeEqkRupture getRuptureAsCSRup() {
		if (this.eqkRupture instanceof CyberShakeEqkRupture) {
			return (CyberShakeEqkRupture)this.eqkRupture;
		} else
			throw new RuntimeException("The CyberShakeIMR isn't being used with a CyberShake ERF!");
	}

	/**
	 * Returns a normalized cumulative distribution for the CyberShake rupture variation values
	 * @param vals
	 * @return
	 */
	private DiscretizedFunc getCumDistFunction(List<Double> vals) {
		return ArbDiscrEmpiricalDistFunc.calcQuickNormCDF(vals, null);
//		ArbDiscrEmpiricalDistFunc function = new ArbDiscrEmpiricalDistFunc();
//
//		for (double val : vals) {
//			function.set(val,1);
//		}
//
//		DiscretizedFunc normCumDist = function.getNormalizedCumDist();
//
//		return normCumDist;
	}

	/**
	 * Returns a new ArbitrarilyDiscretizedFunc where each x value is the natural log
	 * of the original function
	 * @param func
	 * @return
	 */
	private DiscretizedFunc getLogXFunction(DiscretizedFunc func) {
		double[] xVals = new double[func.size()];
		double[] yVals = new double[xVals.length];
		for (int i=0; i<func.size(); i++) {
			xVals[i] = Math.log(func.getX(i));
			yVals[i] = func.getY(i);
		}

		return new LightFixedXFunc(xVals, yVals);
	}

	private void oneMinusYFunction(DiscretizedFunc func) {
		for (int i=0; i<func.size(); i++) {
			func.set(i, 1 - func.getY(i));
		}
	}

	/**
	 * First gets the norm cum dist using getCumDistFunction(). Then it creates a new function where
	 * x = log(x) and y = 1 - y;
	 * @param vals
	 * @return
	 */

	private DiscretizedFunc getLogX_OneMinusYCumDistFunction(List<Double> vals) {
		DiscretizedFunc normCumDist = getCumDistFunction(vals);

		DiscretizedFunc logFunc = getLogXFunction(normCumDist);
		oneMinusYFunction(logFunc);

		return logFunc;
	}

	private double getProbabilityFromLogCumDistFunc(DiscretizedFunc logFunc, double iml) {
		double prob;
		if(iml < logFunc.getMinX())
			prob = 1;
		else if(iml > logFunc.getMaxX())
			prob = 0;
		else
			prob = logFunc.getInterpolatedY(iml);

		return prob;
	}

	@Override
	public double getExceedProbability(double iml) {
		checkInit();
		CyberShakeEqkRupture rup = getRuptureAsCSRup();

		List<Double> imVals;
		try {
			imVals = imsCache.get(new CacheKey(curRunID, rup.getSrcID(), rup.getRupID(), curIM));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}

		if (imVals == null)
			// no amps for this run/source
			return 0d;
		// remove any zeros
		for (int i=imVals.size(); --i>=0;)
			if (imVals.get(i) == 0d)
				imVals.remove(i);

		if (imVals.size() == 0)
			// all zeros
			return 0d;

		DiscretizedFunc logFunc = getLogX_OneMinusYCumDistFunction(imVals);

		return getProbabilityFromLogCumDistFunc(logFunc, iml);
	}

	/**
	 *  This fills in the exceedance probability for multiple intensityMeasure
	 *  levels (often called a "hazard curve"); the levels are obtained from
	 *  the X values of the input function, and Y values are filled in with the
	 *  asociated exceedance probabilities. NOTE: THE PRESENT IMPLEMENTATION IS
	 *  STRANGE IN THAT WE DON'T NEED TO RETURN ANYTHING SINCE THE FUNCTION PASSED
	 *  IN IS WHAT CHANGES (SHOULD RETURN NULL?).
	 *
	 * @param  intensityMeasureLevels  The function to be filled in
	 * @return                         The function filled in
	 * @exception  ParameterException  Description of the Exception
	 */
	public DiscretizedFunc getExceedProbabilities(
			DiscretizedFunc intensityMeasureLevels
			) throws ParameterException {
		checkInit();

		CyberShakeEqkRupture rup = null;
		if (this.eqkRupture instanceof CyberShakeEqkRupture) {
			rup = (CyberShakeEqkRupture)this.eqkRupture;
		} else throw new RuntimeException("The CyberShakeIMR isn't being used with a CyberShake ERF!");
		
		List<Double> imVals;
		try {
			imVals = imsCache.get(new CacheKey(curRunID, rup.getSrcID(), rup.getRupID(), curIM));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}

		if (imVals == null)
			// no amps for this run/source
			imVals = new ArrayList<>();
		// remove any zeros
		for (int i=imVals.size(); --i>=0;)
			if (imVals.get(i) == 0d)
				imVals.remove(i);

		if (imVals.size() == 0) {
			// all zeros
			for (int i=0; i<intensityMeasureLevels.size(); i++) {
				intensityMeasureLevels.set(i, 0);
			}
			return intensityMeasureLevels;
		}

		DiscretizedFunc logFunc = getLogX_OneMinusYCumDistFunction(imVals);

		for (int i=0; i<intensityMeasureLevels.size(); i++) {
			double iml = intensityMeasureLevels.getX(i);
			double prob = getProbabilityFromLogCumDistFunc(logFunc, iml);
			intensityMeasureLevels.set(i, prob);
		}

		return intensityMeasureLevels;
	}

	@Override
	protected void initOtherParams() {

		// Sigma truncation type parameter:
		sigmaTruncTypeParam = new SigmaTruncTypeParam();

		// Sigma truncation level parameter:
		sigmaTruncLevelParam = new SigmaTruncLevelParam();

		// Put parameters in the otherParams list:
		otherParams.clear();
		otherParams.addParameter(sigmaTruncTypeParam);
		otherParams.addParameter(sigmaTruncLevelParam);

	}

	@Override
	protected void initSupportedIntensityMeasureParams() {

		// hard coded so that we don't have to retrieve from the DB whenever this IMR is included in an application

		// Create SA Parameter:
		DoubleDiscreteConstraint periodConstraint = new DoubleDiscreteConstraint();

		periodConstraint.addDouble(0.01);
		periodConstraint.addDouble(0.1);
		periodConstraint.addDouble(0.1111111);
		periodConstraint.addDouble(0.125);
		periodConstraint.addDouble(0.1428571);
		periodConstraint.addDouble(0.1666667);
		periodConstraint.addDouble(0.2);
		periodConstraint.addDouble(0.2222222);
		periodConstraint.addDouble(0.25);
		periodConstraint.addDouble(0.2857143);
		periodConstraint.addDouble(0.3333333);
		periodConstraint.addDouble(0.4);
		periodConstraint.addDouble(0.5);
		periodConstraint.addDouble(0.6666667);
		periodConstraint.addDouble(1);
		periodConstraint.addDouble(1.111111);
		periodConstraint.addDouble(1.25);
		periodConstraint.addDouble(1.428571);
		periodConstraint.addDouble(1.666667);
		periodConstraint.addDouble(2);
		periodConstraint.addDouble(2.2);
		periodConstraint.addDouble(2.4);
		periodConstraint.addDouble(2.6);
		periodConstraint.addDouble(2.8);
		periodConstraint.addDouble(3);
		periodConstraint.addDouble(3.2);
		periodConstraint.addDouble(3.4);
		periodConstraint.addDouble(3.6);
		periodConstraint.addDouble(3.8);
		periodConstraint.addDouble(4);
		periodConstraint.addDouble(4.2);
		periodConstraint.addDouble(4.4);
		periodConstraint.addDouble(4.6);
		periodConstraint.addDouble(4.8);
		periodConstraint.addDouble(5);
		periodConstraint.addDouble(5.5);
		periodConstraint.addDouble(6);
		periodConstraint.addDouble(6.5);
		periodConstraint.addDouble(7);
		periodConstraint.addDouble(7.5);
		periodConstraint.addDouble(8);
		periodConstraint.addDouble(8.5);
		periodConstraint.addDouble(9);
		periodConstraint.addDouble(9.5);
		periodConstraint.addDouble(10);

		periodConstraint.setNonEditable();
		saPeriodParam = new PeriodParam(periodConstraint, 3.0, false);
		saDampingParam = new DampingParam();
		saParam = new SA_Param(saPeriodParam, saDampingParam);
		saParam.setNonEditable();

		supportedIMParams.addParameter(saParam);
	}

	public String getShortName() {
		return SHORT_NAME;
	}

	public String getName() {
		return NAME;
	}

	public void setParamDefaults() {}

	public void parameterChange(ParameterChangeEvent event) {
		checkInit();
		if (event.getParameter() == runIDParam) {
			curRunID = runIDParam.getValue();
		} else if (event.getParameter() == imTypeIDParam) {
			int imTypeID = imTypeIDParam.getValue();
			if (imTypeID < 0)
				curIM = null;
			else
				curIM = curvesDB.getIMFromID(imTypeID);
		}

	}

	@Override
	protected void initPropagationEffectParams() {}

	@Override
	protected void setPropagationEffectParams() {}

	private static final int IM_VALS_BUFF_SIZE = 100;

	private double calcMean(List<Double> vals) {
		double tot = 0;
		for (double val : vals) {
			tot += Math.log(val);
		}
		double mean = tot / (double)vals.size();
//		System.out.println("getMean(): "+mean);
		return mean;
	}

	private double calcStdDev(List<Double> vals) {
		double[] array = Doubles.toArray(vals);
		for (int i=0; i<array.length; i++)
			array[i] = Math.log(array[i]);
		double std = Math.sqrt(StatUtils.variance(array));
//		double mean = calcMean(vals);
//
//		// subtract the mean from each one, square them, and sum them
//		double sum = 0;
//		for (double val : vals) {
//			val = Math.log(val);
//			val = val - mean;
//			val = Math.pow(val, 2);
//			sum += val;
//		}
//		//		System.out.println("Sum: " + sum);
//		// std deviation is the sqrt(sum / (numVals - 1))
//		double std = Math.sqrt(sum / (vals.size() - 1));
//		//		if (std != 0)
//		//			System.out.println("********************************** STD DEV: " + std);
//		System.out.println("getStdDev(): "+std);
		return std;
	}

	public double getMean() {
//		System.out.println("getMean()");
		if (curRunID < 0)
			return 0d;
		CyberShakeEqkRupture rup = null;
		if (this.eqkRupture instanceof CyberShakeEqkRupture) {
			rup = (CyberShakeEqkRupture)this.eqkRupture;
		} else throw new RuntimeException("The CyberShakeIMR isn't being used with a CyberShake ERF!");
		
		List<Double> imVals;
		try {
			imVals = imsCache.get(new CacheKey(curRunID, rup.getSrcID(), rup.getRupID(), curIM));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		if (imVals == null)
			return 0d;
		return calcMean(imVals);
	}

	public double getStdDev() {
//		System.out.println("getStdDev()");
		if (curRunID < 0)
			return 0d;
		CyberShakeEqkRupture rup = null;
		if (this.eqkRupture instanceof CyberShakeEqkRupture) {
			rup = (CyberShakeEqkRupture)this.eqkRupture;
		} else throw new RuntimeException("The CyberShakeIMR isn't being used with a CyberShake ERF!");
		
		List<Double> imVals;
		try {
			imVals = imsCache.get(new CacheKey(curRunID, rup.getSrcID(), rup.getRupID(), curIM));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		if (imVals == null)
			return 0d;
		return calcStdDev(imVals);
	}

//	@Override
//	public double getEpsilon() {
//		double iml = ((Double) im.getValue()).doubleValue();
//		double mean = getMean();
//		double sd = getStdDev();
//		double epsilon = (iml - getMean())/getStdDev();
//		System.out.println("epsilon = ["+(float)iml+" - "+(float)mean+"]/"+(float)sd+" = "+(float)+epsilon);
//		return epsilon;
//	}

	@Override
	public ListIterator getOtherParamsIterator() {
		// this is called when the IMR gets activated in the GUI bean
		checkInit();
		return super.getOtherParamsIterator();
	}

	@Override
	public ParameterList getOtherParams() {
		// this is called when the IMR gets activated in the GUI bean
		checkInit();
		return super.getOtherParams();
	}

	public void setRunID(int runID) {
		checkInit();
		runIDParam.setValue(runID);
	}
	
	public void setIM(CybershakeIM im) {
		this.curIM = im;
	}

	public static void main(String args[]) {
//		CyberShakeIMR imr = new CyberShakeIMR(null);
//		imr.checkInit();
//		try {
//			imr.getIMVals(28, 34, 5, 3, 1, 1, 0, new CybershakeIM(21, null, 3, "", null));
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.exit(1);
	}

	// Methods required by abstract parent, but not needed here
	protected void initEqkRuptureParams() {}
	protected void initSiteParams() {}
}
