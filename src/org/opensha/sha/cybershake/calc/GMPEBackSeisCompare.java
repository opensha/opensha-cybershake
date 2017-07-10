package org.opensha.sha.cybershake.calc;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.hazardMap.HazardCurveSetCalculator;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.attenRelImpl.NGAWest_2014_Averaged_AttenRel;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.util.SiteTranslator;

import com.google.common.collect.Lists;

public class GMPEBackSeisCompare {

	public static void main(String[] args) throws IOException {
		List<Integer> runIDs = Lists.newArrayList(2657, 3037, 2722, 3022, 3030, 3027, 2636, 2638,
				2660, 2703, 3504, 2988, 2965, 3007);
		File baseDir = new File("/home/kevin/CyberShake/MCER");
		File outputDir = new File(baseDir, "back_seis_plots");
		
		ScalarIMR gmpe = new NGAWest_2014_Averaged_AttenRel(null, false); // NGA2 without Idriss
		gmpe.setParamDefaults();
		gmpe.setIntensityMeasure(SA_Param.NAME);
		String periodFileStr = "3sec";
		SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), 3d);
		String xAxisLabel = "3sec SA";
		
		AbstractERF totalERF = MeanUCERF2_ToDB.createUCERF2ERF();
		AbstractERF faultERF = MeanUCERF2_ToDB.createUCERF2ERF();
		AbstractERF backERF = MeanUCERF2_ToDB.createUCERF2ERF();
		
		totalERF.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_INCLUDE);
		totalERF.updateForecast();
		
		faultERF.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_EXCLUDE);
		faultERF.updateForecast();
		
		backERF.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_ONLY);
		backERF.updateForecast();
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		Runs2DB run2db = new Runs2DB(db); 
		CybershakeSiteInfo2DB site2db = new CybershakeSiteInfo2DB(db);
		
		HazardCurveCalculator calc = new HazardCurveCalculator();
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(gmpe.getIntensityMeasure());
		DiscretizedFunc logXVals = HazardCurveSetCalculator.getLogFunction(xVals.deepClone());
		
		SiteTranslator trans = new SiteTranslator();
		
		for (int runID : runIDs) {
			CybershakeRun run = run2db.getRun(runID);
			int siteID = run.getSiteID();
			CybershakeSite csSite = site2db.getSiteFromDB(siteID);
			Location loc = csSite.createLocation();
			
			System.out.println("Doing "+csSite.short_name);
			
			Site site = new Site(loc);
			
			OrderedSiteDataProviderList providers =
					HazardCurvePlotter.createProviders(run.getVelModelID());
			
			trans.setAllSiteParams(gmpe, providers.getBestAvailableData(loc));
			site.addParameterList(gmpe.getSiteParams());
			
			DiscretizedFunc totalHazard = calc.getAnnualizedRates(
					HazardCurveSetCalculator.unLogFunction(xVals,
							calc.getHazardCurve(logXVals.deepClone(), site, gmpe, totalERF)), 1d);
			totalHazard.setName("Total");
			
			DiscretizedFunc faultHazard = calc.getAnnualizedRates(
					HazardCurveSetCalculator.unLogFunction(xVals,
							calc.getHazardCurve(logXVals.deepClone(), site, gmpe, faultERF)), 1d);
			faultHazard.setName("Fault");
			
			DiscretizedFunc backHazard = calc.getAnnualizedRates(
					HazardCurveSetCalculator.unLogFunction(xVals,
							calc.getHazardCurve(logXVals.deepClone(), site, gmpe, backERF )), 1d);
			backHazard.setName("Background");
			
			List<DiscretizedFunc> funcs = Lists.newArrayList();
			List<PlotCurveCharacterstics> chars = Lists.newArrayList();
			
			funcs.add(totalHazard);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLACK));
			
			funcs.add(faultHazard);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.RED));
			
			funcs.add(backHazard);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLUE));
			
			PlotSpec spec = new PlotSpec(funcs, chars, csSite.short_name, xAxisLabel,
					"Annual Frequency Of Exceedence");
			spec.setLegendVisible(true);
			
			HeadlessGraphPanel gp = new HeadlessGraphPanel();
			gp.setBackgroundColor(Color.WHITE);
//			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
			gp.setTickLabelFontSize(18);
			gp.setAxisLabelFontSize(20);
			gp.setPlotLabelFontSize(21);
			
			gp.drawGraphPanel(spec, true, true, new Range(3e-3, 3e0), new Range(5e-7, 5e-1));
			gp.getChartPanel().setSize(1000, 800);
			gp.setVisible(true);
			
			gp.validate();
			gp.repaint();
			
			File file = new File(outputDir, csSite.short_name+"_"+periodFileStr+"_gmpe_back_seis_compare");
			gp.saveAsPDF(file.getAbsolutePath()+".pdf");
			gp.saveAsPNG(file.getAbsolutePath()+".png");
		}
		db.destroy();
		System.exit(0);
	}

}
