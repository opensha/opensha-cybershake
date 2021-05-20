package org.opensha.sha.cybershake.calc;

import java.awt.geom.Point2D;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.AttenRels2DB;
import org.opensha.sha.cybershake.db.CybershakeHazardDataset;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.HazardDataset2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.OtherParams.ComponentParam;

import com.google.common.base.Preconditions;

public class BackgroundSeismicityCalculator {

	public static void main(String[] args) {
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;

		ScalarIMR gmpe = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		gmpe.setParamDefaults();
		HardCodedInterpDiffMapCreator.setTruncation(gmpe, 3d);
		Component gmpeComp = ((ComponentParam)gmpe.getParameter(ComponentParam.NAME)).getValue();
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		
		AbstractERF erf = study.getERF();
		System.out.println("Orig ERF has "+erf.getNumSources()+" sources");
		erf.getParameter(UCERF2.BACK_SEIS_NAME).setValue(UCERF2.BACK_SEIS_ONLY);
		erf.getParameter(UCERF2.BACK_SEIS_RUP_NAME).setValue(UCERF2.BACK_SEIS_RUP_CROSSHAIR);
		erf.updateForecast();
		System.out.println("Back-seis ERF has "+erf.getNumSources()+" sources");
		
		DBAccess db = study.getWriteDB(true, true);
		try {
			AttenRels2DB gmpes2db = new AttenRels2DB(db);
			int backSeisAttenRelID = gmpes2db.getAttenRelID(gmpe);
			System.out.println("AttenRel ID: "+backSeisAttenRelID);
			Preconditions.checkState(backSeisAttenRelID > 0);
			
			HazardDataset2DB datasets2db = new HazardDataset2DB(db);
			HazardCurve2DB curves2db = new HazardCurve2DB(db);
			CybershakeSiteInfo2DB sites2db = new CybershakeSiteInfo2DB(db);
			for (int datasetID : study.getDatasetIDs()) {
				System.out.println("Original dataset ID: "+datasetID);
				
				CybershakeHazardDataset ds = datasets2db.getDataset(datasetID);
				
				// see if we already have a bg dataset ID
				int bgDSID = datasets2db.getDatasetID(ds.erfID, ds.rvScenID, ds.sgtVarID, ds.velModelID,
						ds.probModelID, ds.timeSpanID, ds.timeSpanStart, ds.maxFreq, ds.lowFreqCutoff,
						backSeisAttenRelID);
				if (bgDSID > 0) {
					System.out.println("Reusing BG datasetID: "+bgDSID);
				} else {
					System.out.println("Need to insert a new dataset ID");
					bgDSID = datasets2db.addNewDataset(ds.erfID, ds.rvScenID, ds.sgtVarID, ds.velModelID,
							ds.probModelID, ds.timeSpanID, ds.timeSpanStart, ds.maxFreq, ds.lowFreqCutoff,
							backSeisAttenRelID);
					System.out.println("\tInserted dataset ID: "+bgDSID);
				}
				Preconditions.checkState(bgDSID > datasetID);
				
				List<CybershakeRun> runs = study.runFetcher().hasHazardCurves(datasetID).fetch();
				System.out.println("Fetched "+runs.size()+" runs for "+datasetID);
				
				// make sure simulation vs30 fields populated
				for (int r=runs.size(); --r>=0 && vs30Source == Vs30_Source.Simulation;) {
					CybershakeRun run = runs.get(r);
					if (!(run.getModelVs30() != null || run.getMeshVsitop() != null)) {
						System.out.println("Will skip run "+run.getRunID()+" as simulation Vs is not definied");
						runs.remove(r);
					}
				}
				List<Site> sites = CyberShakeSiteBuilder.buildSites(study, vs30Source, runs);
				
				for (int i=0; i<runs.size(); i++) {
					CybershakeRun run = runs.get(i);
					List<Integer> origCurveIDs = curves2db.getAllHazardCurveIDsForRun(
							run.getRunID(), datasetID, -1);
					CybershakeSite site = sites2db.getSiteFromDB(run.getSiteID());
					System.out.println(site.short_name+", run "+run.getRunID());
					for (int origCurveID : origCurveIDs) {
						CybershakeIM im = curves2db.getIMForCurve(origCurveID);
						System.out.println("\toriginal curve with ID="+origCurveID+", IM="+im);
						if (im.getComponent() == null || !im.getComponent().isComponentSupported(gmpeComp)) {
							System.out.println("\t\tskipping as component ("+im.getComponent().getShortName()
									+") not supported by GMPE");
							continue;
						}
						
						// see if we already have a BG curve
						int bgCurveID = curves2db.getHazardCurveID(run.getRunID(), bgDSID, im.getID());
						if (bgCurveID > 0) {
							System.out.println("\t\talready have a back seis curve, curveID="+bgCurveID);
						} else {
							System.out.println("\t\tneed to calculate back seis curve");
							DiscretizedFunc csCurve = curves2db.getHazardCurve(origCurveID);
							DiscretizedFunc bgCurve = calcBackSeisCurve(csCurve, erf, gmpe, sites.get(i), im);
							curves2db.insertHazardCurve(run.getRunID(), im.getID(), bgCurve, bgDSID);
							System.out.println("\t\tDONE");
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			db.destroy();
			System.exit(1);
		}
		
		db.destroy();
		System.exit(0);
	}
	
	private static DiscretizedFunc calcBackSeisCurve(DiscretizedFunc csCurve, AbstractERF erf,
			ScalarIMR gmpe, Site site, CybershakeIM im) {
		DiscretizedFunc logCurve = new ArbitrarilyDiscretizedFunc();
		for (Point2D pt : csCurve)
			logCurve.set(Math.log(pt.getX()), 0d);
		
		HazardCurveCalculator calc = new HazardCurveCalculator();
		
		gmpe.setIntensityMeasure(SA_Param.NAME);
		SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), im.getVal());
		
		calc.getHazardCurve(logCurve, site, gmpe, erf);
		
		DiscretizedFunc combCurve = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<csCurve.size(); i++)
			combCurve.set(csCurve.getX(i), 1d - (1d - csCurve.getY(i))*(1d - logCurve.getY(i)));
		return combCurve;
	}

}
