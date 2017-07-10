package org.opensha.sha.cybershake.calc;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.jfree.data.Range;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.collect.Lists;

public class ModifiedMapCalc {
	
	private HazardCurveFetcher fetch;
	private HazardCurveComputation calc;
	private Runs2DB runs2db;
	private SiteInfo2DB sites2db;
	
	private GeoDataSet origData;
	private GeoDataSet modData;
	
	// will be fetched during calculation
	private int velModelID = -1;
	private int imTypeID = -1;
	
	public ModifiedMapCalc(DBAccess db, HazardCurveFetcher fetch, RuptureVariationProbabilityModifier rupVarMod) {
		this(db, fetch, getCalc(db, rupVarMod));
	}
	
	private static HazardCurveComputation getCalc(DBAccess db, RuptureVariationProbabilityModifier rupVarMod) {
		HazardCurveComputation calc = new HazardCurveComputation(db);
		calc.setRupVarProbModifier(rupVarMod);
		return calc;
	}
	
	public ModifiedMapCalc(DBAccess db, HazardCurveFetcher fetch, HazardCurveComputation calc) {
		this.fetch = fetch;
		this.calc = calc;
		
		runs2db = new Runs2DB(db);
		sites2db = new SiteInfo2DB(db);
		
		imTypeID = fetch.getIM().getID();
	}
	
	public void generateMap(File outputDir, String prefix, String title, boolean logPlot,
			Double customMin, Double customMax, boolean isProbAt_IML, double val) throws Exception {
		calc(isProbAt_IML, val);
		plot(outputDir, prefix, title, logPlot, customMin, customMax, isProbAt_IML, val);
	}
	
	private void calc(boolean isProbAt_IML, double val) {
		CybershakeIM im = fetch.getIM();
		List<Integer> runIDs = fetch.getRunIDs();
		
		origData = new ArbDiscrGeoDataSet(true);
		modData = new ArbDiscrGeoDataSet(true);
		for (int i=0; i<runIDs.size(); i++) {
			int runID = runIDs.get(i);
			CybershakeRun run = runs2db.getRun(runID);
			CybershakeSite site = sites2db.getSiteFromDB(run.getSiteID());
			if (site.type_id == CybershakeSite.TYPE_TEST_SITE)
				continue;
			if (velModelID < 0)
				velModelID = run.getVelModelID();
			DiscretizedFunc origCurve = fetch.getFuncs().get(i);
			List<Double> xVals = Lists.newArrayList();
			for (Point2D pt : origCurve)
				xVals.add(pt.getX());
			DiscretizedFunc curve = calc.computeHazardCurve(xVals, run, im);
			Location loc = fetch.getCurveSites().get(i).createLocation();
			
			double newVal = HazardDataSetLoader.getCurveVal(curve, isProbAt_IML, val);
			double origVal = HazardDataSetLoader.getCurveVal(origCurve, isProbAt_IML, val);
			
			modData.set(loc, newVal);
			origData.set(loc, origVal);
		}
	}
	
	private void plot(File outputDir, String prefix, String title, boolean logPlot,
			Double customMin, Double customMax, boolean isProbAt_IML, double val)
					throws IOException, ClassNotFoundException, GMT_MapException, SQLException {
		// now deal with infinities
		if (logPlot) {
			for (int i=0; i<modData.size(); i++)
				if (modData.get(i) == 0d)
					modData.set(i, 1e-16);
			for (int i=0; i<origData.size(); i++)
				if (origData.get(i) == 0d)
					origData.set(i, 1e-16);
		}

		ScalarIMR baseMapIMR = AttenRelRef.NGA_2008_4AVG.instance(null);
		HardCodedInterpDiffMapCreator.setTruncation(baseMapIMR, 3.0);
		
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();

		System.out.println("Modified:");
		String addr = HardCodedInterpDiffMapCreator.getMap(region, modData, logPlot, velModelID, imTypeID,
				customMin, customMax, isProbAt_IML, val, baseMapIMR, false, title+", Modified");
		FileUtils.downloadURL(addr+"/interpolated.150.png", new File(outputDir, prefix+"_mod.png"));
		System.out.println("Orig:");
		addr = HardCodedInterpDiffMapCreator.getMap(region, origData, logPlot, velModelID, imTypeID,
				customMin, customMax, isProbAt_IML, val, baseMapIMR, false, title+", Original");
		FileUtils.downloadURL(addr+"/interpolated.150.png", new File(outputDir, prefix+"_orig.png"));

		// now ratio
		String[] addrs = HardCodedInterpDiffMapCreator.getCompareMap(
				false, modData, origData, title, true, region);
		FileUtils.downloadURL(addrs[0]+"/interpolated.150.png", new File(outputDir, prefix+"_diff.png"));
		FileUtils.downloadURL(addrs[1]+"/interpolated.150.png", new File(outputDir, prefix+"_ratio.png"));
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
