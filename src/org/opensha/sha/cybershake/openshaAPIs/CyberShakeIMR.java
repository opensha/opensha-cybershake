/*******************************************************************************
 * Copyright 2009 OpenSHA.org in partnership with
 * the Southern California Earthquake Center (SCEC, http://www.scec.org)
 * at the University of Southern California and the UnitedStates Geological
 * Survey (USGS; http://www.usgs.gov)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.opensha.sha.cybershake.openshaAPIs;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbDiscrEmpiricalDistFunc;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.exceptions.IMRException;
import org.opensha.commons.exceptions.ParameterException;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.param.constraint.impl.DoubleDiscreteConstraint;
import org.opensha.commons.param.constraint.impl.IntegerConstraint;
import org.opensha.commons.param.constraint.impl.IntegerDiscreteConstraint;
import org.opensha.commons.param.constraint.impl.StringConstraint;
import org.opensha.commons.param.event.ParameterChangeEvent;
import org.opensha.commons.param.event.ParameterChangeListener;
import org.opensha.commons.param.event.ParameterChangeWarningListener;
import org.opensha.commons.param.impl.IntegerParameter;
import org.opensha.commons.param.impl.StringParameter;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.DampingParam;
import org.opensha.sha.imr.param.IntensityMeasureParams.PeriodParam;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncLevelParam;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncTypeParam;

import com.google.common.base.Preconditions;

public class CyberShakeIMR extends AttenuationRelationship implements ParameterChangeListener {

	public static final String NAME = "CyberShake Fake Attenuation Relationship";
	public static final String SHORT_NAME = "CyberShakeIMR";
	
	/**
	 * distance tolerance in KM for site selection
	 */
	private static final double distanceTolerance = 5d;
	
	EqkRupture curRupture = null;

	/** ParameterList of all Site parameters */
	protected ParameterList siteParams = new ParameterList();

	/** ParameterList of all eqkRupture parameters */
	protected ParameterList eqkRuptureParams = new ParameterList();

	boolean dbConnInitialized = false;
	DBAccess db = null;
	SiteInfo2DB site2db = null;
	PeakAmplitudesFromDB ampsDB = null;
	Runs2DB runs2db = null;
	
	int forcedRunID = -1;

	List<CybershakeSite> sites = null;
	CybershakeSite csSite = null;

	public static final String SGT_VAR_PARAM = "SGT Variation ID";
	public static final String RUP_VAR_SCENARIO_PARAM = "Rupture Variation Scenario ID";
	public static final String VEL_MODEL_PARAM = "Velocity Model";

	//source index parameter
	private IntegerParameter sgtVarParam;

	//rupture index parameter
	private IntegerParameter rupVarScenarioParam;
	
	//vel model index parameter
	private StringParameter velModelParam;

	private ArrayList<CybershakeVelocityModel> velModels;
	
	int selectedSGTVariation = 5;
	int selectedRupVarScenario = 3;
	int selectedVelModel = 1;
	
	double curPeriod = 0;
	CybershakeIM curIM = null;

	ArrayList<CybershakeIM> csIMs = null;

	private boolean isInitialized;

	public CyberShakeIMR(ParameterChangeWarningListener listener) {
		super();
		isInitialized = false;
//		loading = true;
		initSupportedIntensityMeasureParams();

		sgtVarParam = new IntegerParameter(SGT_VAR_PARAM, -1);
		rupVarScenarioParam = new IntegerParameter(RUP_VAR_SCENARIO_PARAM, -1);
		velModelParam = new StringParameter(VEL_MODEL_PARAM, "");
		//		saPeriod = saPeriods.get(0);
		//		saPeriodParam = new StringParameter(SA_PERIOD_SELECTOR_PARAM_NAME,
		//		saPeriods,saPeriod);
		//		saPeriodParam.addParameterChangeListener(this);

		//		this.supportedIMParams.addParameter(saPeriodParam);

		initOtherParams();
	}

	private void checkInit() {
		// we don't want to initilize the DB connection until we know the user actually wants to use this
		if (!isInitialized) {
			System.out.println("Initializing CyberShake IMR!");
			this.initDB();

			csIMs = ampsDB.getSupportedIMs();
			curIM = getIMForPeriod(3);

			// SGT Variation ID
			ArrayList<Integer> ids = ampsDB.getSGTVarIDs();
			selectedSGTVariation = ids.get(0);
			sgtVarParam.setValue(ids.get(0));
			sgtVarParam.setConstraint(new IntegerDiscreteConstraint(ids));
			sgtVarParam.addParameterChangeListener(this);

			// Rupture Variation IDs
			ids = ampsDB.getRupVarScenarioIDs();
			selectedRupVarScenario = ids.get(0);
			rupVarScenarioParam.setValue(ids.get(0));
			rupVarScenarioParam.setConstraint(new IntegerDiscreteConstraint(ids));
			rupVarScenarioParam.addParameterChangeListener(this);
			
			velModels = runs2db.getVelocityModels();
			selectedVelModel = velModels.get(0).getID();
			ArrayList<String> vals = new ArrayList<String>();
			for (CybershakeVelocityModel velModel : velModels) {
				vals.add(velModel.toString());
			}
			velModelParam.setValue(vals.get(0));
			velModelParam.setConstraint(new StringConstraint(vals));
			velModelParam.addParameterChangeListener(this);
			
			ParameterList[] listsToAdd = { otherParams, imlAtExceedProbIndependentParams,
					exceedProbIndependentParams, meanIndependentParams, stdDevIndependentParams };
			
			for (ParameterList paramList : listsToAdd) {
				paramList.addParameter(rupVarScenarioParam);
				paramList.addParameter(sgtVarParam);
				paramList.addParameter(velModelParam);
			}

			saPeriodParam.addParameterChangeListener(this);

			isInitialized = true;
		}
	}

	private void initDB() {
		db = Cybershake_OpenSHA_DBApplication.getDB();
		site2db = new SiteInfo2DB(db);
		ampsDB = new PeakAmplitudesFromDB(db);
		runs2db = new Runs2DB(db);
		sites = site2db.getAllSitesFromDB();
		dbConnInitialized = true;
	}

	@Override
	public void setSite(Site site) {
		checkInit();
		System.out.println("Setting the site!");
		if (!dbConnInitialized)
			initDB();
		
		CybershakeSite minSite = null;
		double minDist = Double.POSITIVE_INFINITY;
		
		for (CybershakeSite csSite : sites) {
			double dist = LocationUtils.horzDistanceFast(csSite.createLocation(), site.getLocation());
			if (dist < distanceTolerance && dist < minDist) {
				// it's a match!
				minSite = csSite;
				minDist = dist;
				System.out.println("Idedntified possible CyberShake site (dist=" + dist + " KM): " + csSite);
			}
		}
		this.csSite = minSite;
		if (this.csSite == null)
			System.out.println("No match for site: " + site);
		else
			System.out.println("Using site: " + this.csSite.name);
		this.site = site;
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
	
	private ArbitrarilyDiscretizedFunc getCumDistFunction(List<Double> vals) {
		ArbDiscrEmpiricalDistFunc function = new ArbDiscrEmpiricalDistFunc();
		
		for (double val : vals) {
			function.set(val,1);
		}
		
		ArbitrarilyDiscretizedFunc normCumDist = function.getNormalizedCumDist();
		
		return normCumDist;
	}
	
	/**
	 * Returns a new ArbitrarilyDiscretizedFunc where each x value is the natural log
	 * of the original function
	 * @param func
	 * @return
	 */
	private ArbitrarilyDiscretizedFunc getLogXFunction(ArbitrarilyDiscretizedFunc func) {
		ArbitrarilyDiscretizedFunc logFunc = new ArbitrarilyDiscretizedFunc();
		
		for (int i=0; i<func.size(); i++) {
			logFunc.set(Math.log(func.getX(i)), func.getY(i));
		}
		
		return logFunc;
	}
	
	private void oneMinusYFunction(ArbitrarilyDiscretizedFunc func) {
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
	
	private ArbitrarilyDiscretizedFunc getLogX_OneMinusYCumDistFunction(List<Double> vals) {
		ArbitrarilyDiscretizedFunc normCumDist = getCumDistFunction(vals);
		
		ArbitrarilyDiscretizedFunc logFunc = getLogXFunction(normCumDist);
		oneMinusYFunction(logFunc);
		
		return logFunc;
	}
	
	private double getProbabilityFromLogCumDistFunc(ArbitrarilyDiscretizedFunc logFunc, double iml) {
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

		int srcID = rup.getSrcID();
		int rupID = rup.getRupID();
		int erfID = rup.getErfID();
		int sgtVarID = this.selectedSGTVariation;
		int rupVarID = this.selectedRupVarScenario;
		int velModelID = this.selectedVelModel;

		List<Double> imVals = null; 

		try {
			imVals = getIMVals(this.csSite.id, erfID, sgtVarID, rupVarID, velModelID, srcID, rupID, curIM);
			// remove any zeros
			for (int i=imVals.size(); --i>=0;)
				if (imVals.get(i) == 0d)
					imVals.remove(i);
		} catch (SQLException e) {
			//			e.printStackTrace();
			return 0;
		}
		
		if (imVals.size() == 0)
			// all zeros
			return 0d;
		
		ArbitrarilyDiscretizedFunc logFunc = getLogX_OneMinusYCumDistFunction(imVals);
		
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

		int srcID = rup.getSrcID();
		int rupID = rup.getRupID();
		int erfID = rup.getErfID();
		int sgtVarID = this.selectedSGTVariation;
		int rupVarID = this.selectedRupVarScenario;
		int velModelID = this.selectedVelModel;

		List<Double> imVals = null; 

		try {
			imVals = getIMVals(this.csSite.id, erfID, sgtVarID, rupVarID, velModelID, srcID, rupID, curIM);
		} catch (SQLException e) {
			//			e.printStackTrace();
			for (int i=0; i<intensityMeasureLevels.size(); i++) {
				intensityMeasureLevels.set(i, 0);
			}
			return intensityMeasureLevels;
		}
		
		ArbitrarilyDiscretizedFunc logFunc = getLogX_OneMinusYCumDistFunction(imVals);
		
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

	public void setParamDefaults() {
		// TODO Auto-generated method stub

	}

	private CybershakeIM getIMForPeriod(double period) {
		for (CybershakeIM im : csIMs) {
			if (Math.abs(im.getVal() - period) < 0.01) {
				curPeriod = im.getVal();
				System.out.println("Matched period of " + period +  " with: " + im);
				return im;
			}
		}
		return null;
	}

	public void parameterChange(ParameterChangeEvent event) {
		checkInit();
		String paramName = event.getParameterName();

		if (paramName.equals(PeriodParam.NAME)) {
			this.curIM = getIMForPeriod((Double)event.getParameter().getValue());
			System.out.println("We got a period of " + (Double)event.getParameter().getValue() + "! " + curIM);
		} else if (paramName.equals(SGT_VAR_PARAM)) {
			selectedSGTVariation = sgtVarParam.getValue();
			//			this.reloadParams();
		} else if (paramName.equals(RUP_VAR_SCENARIO_PARAM)) {
			selectedRupVarScenario = rupVarScenarioParam.getValue();
			//			this.reloadParams();
		} else if (paramName.equals(VEL_MODEL_PARAM)) {
			String velModelStr = velModelParam.getValue();
			selectedVelModel = -1;
			for (CybershakeVelocityModel velModel : velModels) {
				if (velModelStr.equals(velModel.toString()))
					selectedVelModel = velModel.getID();
			}
			Preconditions.checkState(selectedVelModel >= 0, "Vel model not found: "+velModelStr);
		}

	}

	@Override
	protected void initPropagationEffectParams() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void setPropagationEffectParams() {
		// TODO Auto-generated method stub

	}

	private String getBuffKey(int siteID, int erfID, int sgtVarID, int rupVarID, int velModelID, int srcID, int rupID, CybershakeIM im) {
		return siteID + "_" + erfID + "_" + sgtVarID + "_" + rupVarID + "_" + velModelID + "_" + srcID + "_" + rupID + "_" + im.getID();
	}

	private List<Double> getIMVals(int siteID, int erfID, int sgtVarID, int rupVarID, int velModelID, int srcID, int rupID, CybershakeIM im) throws SQLException {
		if (imValsBuff == null) {
			imValsBuff = new LinkedList<List<Double>>();
			imValsBuffKeys = new LinkedList<String>();
			//			for (int i=0; i<IM_VALS_BUFF_SIZE; i++) {
			//				imValsBuff.add(null);
			//				imValsBuffKeys.add(null);
			//			}
		}

		String key = getBuffKey(siteID, erfID, sgtVarID, rupVarID, velModelID, srcID, rupID, im);
		for (int i=0; i<imValsBuffKeys.size(); i++) {
			String bufKey = imValsBuffKeys.get(i);

			if (bufKey.equals(key)) {
				return imValsBuff.get(i);
			}
		}

		// if we made it this far, then it's not in the buffer...we'll need to get it manually
//		System.out.println("Loading amps for " + erfID + " " + srcID + " " + rupID);
		int runID = forcedRunID;
		if (runID <= 0)
			runID = runs2db.getLatestRunID(siteID, erfID, sgtVarID, rupVarID, velModelID, null, null, null, null);
		Preconditions.checkState(runID > 0, "Couldn't get runID for: siteID="+siteID+", erfID="+erfID
				+", sgtVarID="+sgtVarID+", rupVarID="+rupVarID+", velModelID="+velModelID);
		List<Double> imVals = ampsDB.getIM_Values(runID, srcID, rupID, im);

//		String valStr = "";
		for (int i=0; i<imVals.size(); i++) {
			double val = imVals.get(i);
//			valStr += val + " ";
			imVals.set(i, imVals.get(i)/HazardCurveComputation.CONVERSION_TO_G);
		}
//		System.out.println("VALS: " + valStr);

		// first if the buffer is full, make room for it
		if (imValsBuff.size() >= IM_VALS_BUFF_SIZE) {
			imValsBuff.removeFirst();
			imValsBuffKeys.removeFirst();
		}

		// now add it to the buffer
		imValsBuff.add(imVals);
		imValsBuffKeys.add(key);

		return imVals;
	}

	private static final int IM_VALS_BUFF_SIZE = 5;
	private LinkedList<String> imValsBuffKeys = null;
	private LinkedList<List<Double>> imValsBuff = null;

	private double calcMean(List<Double> vals) {
		double tot = 0;
		for (double val : vals) {
			tot += Math.log(val);
		}
		double mean = tot / (double)vals.size();
		return mean;
	}

	private double calcStdDev(List<Double> vals) {
		double mean = calcMean(vals);

		// subtract the mean from each one, square them, and sum them
		double sum = 0;
		for (double val : vals) {
			val = Math.log(val);
			val = val - mean;
			val = Math.pow(val, 2);
			sum += val;
		}
		//		System.out.println("Sum: " + sum);
		// std deviation is the sqrt(sum / (numVals - 1))
		double std = Math.sqrt(sum / (vals.size() - 1));
		//		if (std != 0)
		//			System.out.println("********************************** STD DEV: " + std);
		return std;
	}

	public double getMean() {
		if (csSite == null)
			return Double.NEGATIVE_INFINITY;
		CyberShakeEqkRupture rup = null;
		if (this.eqkRupture instanceof CyberShakeEqkRupture) {
			rup = (CyberShakeEqkRupture)this.eqkRupture;
		} else throw new RuntimeException("The CyberShakeIMR isn't being used with a CyberShake ERF!");

		int srcID = rup.getSrcID();
		int rupID = rup.getRupID();
		int erfID = rup.getErfID();
		int sgtVarID = this.selectedSGTVariation;
		int rupVarID = this.selectedRupVarScenario;
		int velModelID = this.selectedVelModel;

		try {
			List<Double> imVals = getIMVals(this.csSite.id, erfID, sgtVarID, rupVarID, velModelID, srcID, rupID, curIM);

			return calcMean(imVals);
		} catch (SQLException e) {
			//			e.printStackTrace();
			//			System.out.println("Skipping rupture: " + srcID + " " + rupID);
			return Double.NEGATIVE_INFINITY;
		}
	}

	public double getStdDev() {
		if (csSite == null)
			return 0;
		CyberShakeEqkRupture rup = null;
		if (this.eqkRupture instanceof CyberShakeEqkRupture) {
			rup = (CyberShakeEqkRupture)this.eqkRupture;
		} else throw new RuntimeException("The CyberShakeIMR isn't being used with a CyberShake ERF!");

		int srcID = rup.getSrcID();
		int rupID = rup.getRupID();
		int erfID = rup.getErfID();
		int sgtVarID = this.selectedSGTVariation;
		int rupVarID = this.selectedRupVarScenario;
		int velModelID = this.selectedVelModel;

		try {
			List<Double> vals = getIMVals(this.csSite.id, erfID, sgtVarID, rupVarID, velModelID, srcID, rupID, curIM);

			return calcStdDev(vals);
		} catch (SQLException e) {
			//			e.printStackTrace();
//			System.out.println("Skipping rupture: " + srcID + " " + rupID);
			return 0;
		}
	}

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
	
	public void setForcedRunID(int forcedRunID) {
		this.forcedRunID = forcedRunID;
	}

	public static void main(String args[]) {
		CyberShakeIMR imr = new CyberShakeIMR(null);
		imr.checkInit();
		try {
			imr.getIMVals(28, 34, 5, 3, 1, 1, 0, new CybershakeIM(21, null, 3, "", null));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(1);
	}
	
	// Methods required by abstract parent, but not needed here
	protected void initEqkRuptureParams() {}
	protected void initSiteParams() {}
}
