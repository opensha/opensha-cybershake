package scratch.kevin.cybershake.ucerf3;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.disaggregation.DisaggregationCalculator;
import org.opensha.sha.calc.disaggregation.DisaggregationCalculatorAPI;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.param.IncludeBackgroundOption;
import org.opensha.sha.earthquake.param.IncludeBackgroundParam;
import org.opensha.sha.earthquake.param.ProbabilityModelOptions;
import org.opensha.sha.earthquake.param.ProbabilityModelParam;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.gui.infoTools.DisaggregationPlotViewerWindow;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.util.SiteTranslator;

import com.google.common.base.Preconditions;

import scratch.UCERF3.erf.mean.MeanUCERF3;
import scratch.UCERF3.erf.mean.MeanUCERF3.Presets;

public class U3_U2_CurveCompare {

	public static void main(String[] args) throws IOException {
		ScalarIMR[] gmpes = { AttenRelRef.ASK_2014.instance(null), AttenRelRef.BSSA_2014.instance(null), 
				AttenRelRef.CB_2014.instance(null), AttenRelRef.CY_2014.instance(null),
				AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null)};
		
		double[] periods = {3, 5, 7.5, 10};
		
		File outputDir = new File("/tmp/ugms_u3_u2_gmpe_compare");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		
		CybershakeSite[] csSites = { sites2db.getSiteFromDB("COO"), sites2db.getSiteFromDB("PAS"), sites2db.getSiteFromDB("SBSM") };
		int velModelID = 5;
		OrderedSiteDataProviderList provs = CyberShakeSiteBuilder.getMapBasinProviders(velModelID);
		SiteTranslator trans = new SiteTranslator();
		
		Site[] sites = new Site[csSites.length];
		for (int i=0; i<csSites.length; i++) {
			sites[i] = new Site(csSites[i].createLocation());
			sites[i].setName(csSites[i].short_name);
			ArrayList<SiteDataValue<?>> datas = provs.getAllAvailableData(sites[i].getLocation());
			System.out.println("Site: "+csSites[i].short_name);
			for (Parameter<?> param : gmpes[0].getSiteParams()) {
				param = (Parameter<?>) param.clone();
				trans.setParameterValue(param, datas);
				System.out.println("\t"+param.getName()+": "+param.getValue());
				sites[i].addParameter(param);
			}
			
		}
		
		db.destroy();
		
		double disaggProb = 4e-4;
		
		int[] hazard_curve_rps = { 1000, 2500, 10000 };
		
		MeanUCERF2 u2 = new MeanUCERF2();
		u2.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_EXCLUDE);
		u2.setParameter(UCERF2.PROB_MODEL_PARAM_NAME, UCERF2.PROB_MODEL_POISSON);
		u2.getTimeSpan().setDuration(1);
		u2.updateForecast();
		
		MeanUCERF3 u3 = new MeanUCERF3();
		u3.setPreset(Presets.BOTH_FM_BRANCH_AVG);
		u3.setParameter(IncludeBackgroundParam.NAME, IncludeBackgroundOption.EXCLUDE);
		u3.setParameter(ProbabilityModelParam.NAME, ProbabilityModelOptions.POISSON);
		u3.getTimeSpan().setDuration(1);
		u3.updateForecast();
		
		HazardCurveCalculator calc = new HazardCurveCalculator();
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		DiscretizedFunc logXVals = new ArbitrarilyDiscretizedFunc();
		for (Point2D pt : xVals)
			logXVals.set(Math.log(pt.getX()), 1);
		
		DisaggregationCalculatorAPI disaggCalc = new DisaggregationCalculator();

		// disagg plot settings
		double minMag = 5;
		int numMags = 10;
		double deltaMag = 0.5;

		int numSourcesForDisag = 100;

		boolean showSourceDistances = true;

		double maxZAxis = Double.NaN;
		
		for (Site site : sites) {
			System.out.println("Doing "+site.getName());
			for (ScalarIMR gmpe : gmpes) {
				gmpe.setParamDefaults();
				gmpe.setIntensityMeasure(SA_Param.NAME);
				for (double period : periods) {
					System.out.println(gmpe.getShortName()+", "+(float)period+"s");
					SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
					
					calc.getHazardCurve(logXVals, site, gmpe, u2);
					DiscretizedFunc u2Curve = getCurve(logXVals, xVals);
					
					calc.getHazardCurve(logXVals, site, gmpe, u3);
					DiscretizedFunc u3Curve = getCurve(logXVals, xVals);
					
					List<DiscretizedFunc> funcs = new ArrayList<>();
					List<PlotCurveCharacterstics> chars = new ArrayList<>();
					
					Range xRange = new Range(1e-3, 1e1);
					Range yRange = new Range(1e-8, 1e0);
					
					u2Curve.setName("UCERF2");
					funcs.add(u2Curve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE));
					
					u3Curve.setName("UCERF3");
					funcs.add(u3Curve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED));
					
					if (hazard_curve_rps != null && hazard_curve_rps.length > 0) {
						CPT rpCPT = getRPlogCPT(hazard_curve_rps);
						for (int rp : hazard_curve_rps) {
							Color color = rpCPT.getColor((float)Math.log10(rp));
							double probLevel = 1d/(double)rp;
							DiscretizedFunc probLine = new ArbitrarilyDiscretizedFunc();
							probLine.set(xRange.getLowerBound(), probLevel);
							probLine.set(xRange.getUpperBound(), probLevel);
							probLine.setName(rp+"yr");
							funcs.add(probLine);
							chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 1f, color));
						}
					}
					
					String siteName = site.getName();

					PlotSpec spec = new PlotSpec(funcs, chars, siteName+" Hazard Curves, "+gmpe.getShortName(),
							(float)period+"s SA", "Annual Probability");
					spec.setLegendVisible(true);
					
					PlotPreferences plotPrefs = PlotPreferences.getDefault();
					plotPrefs.setTickLabelFontSize(18);
					plotPrefs.setAxisLabelFontSize(20);
					plotPrefs.setPlotLabelFontSize(21);
					plotPrefs.setBackgroundColor(Color.WHITE);
					
					HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
					
					String prefix = siteName.replaceAll(" ", "_")+"_curves_"+(float)period+"s_"+gmpe.getShortName();
					gp.drawGraphPanel(spec, true, true, xRange, yRange);
					gp.getChartPanel().setSize(800, 600);
					File pngFile = new File(outputDir, prefix+".png");
					gp.saveAsPNG(pngFile.getAbsolutePath());
					
					double iml = HazardDataSetLoader.getCurveVal(u2Curve, false, disaggProb); // iml at prob
					
					System.out.println("Disaggregating U2 for prob="+disaggProb+", iml="+iml);
					disaggCalc.setMagRange(minMag, numMags, deltaMag);
					disaggCalc.setNumSourcesToShow(numSourcesForDisag);
					disaggCalc.setShowDistances(showSourceDistances);
					boolean success = disaggCalc.disaggregate(Math.log(iml), site, gmpe, u2, calc.getSourceFilters(), calc.getAdjustableParams());
					if (!success)
						throw new RuntimeException("Disagg calc failed (see errors above, if any).");
					disaggCalc.setMaxZAxisForPlot(maxZAxis);
					System.out.println("Done Disaggregating");
					String metadata = "temp metadata";

					System.out.println("Fetching plot...");
					String address = disaggCalc.getDisaggregationPlotUsingServlet(metadata);

					String meanModeText = disaggCalc.getMeanAndModeInfo();
					String binDataText = disaggCalc.getBinData();
					String sourceDataText = disaggCalc.getDisaggregationSourceInfo();
					
					prefix = siteName.replaceAll(" ", "_")+"_disagg_"+(float)iml+"_"+(float)period+"s_"+gmpe.getShortName();

					File outputFile = new File(outputDir, prefix+"_u2");

					String metadataText = "Custom disagg";

					DisaggregationPlotViewerWindow.saveAsPDF(
							address+DisaggregationCalculator.DISAGGREGATION_PLOT_PDF_NAME,
							outputFile.getAbsolutePath()+".pdf", meanModeText, metadataText, binDataText, sourceDataText, null);
					FileUtils.downloadURL(address+DisaggregationCalculator.DISAGGREGATION_PLOT_PNG_NAME,
							new File(outputFile.getAbsolutePath()+".png"));
					DisaggregationPlotViewerWindow.saveAsTXT(outputFile.getAbsolutePath()+".txt", meanModeText, metadataText,
							binDataText, sourceDataText, null);
					
					iml = HazardDataSetLoader.getCurveVal(u3Curve, false, disaggProb); // iml at prob
					
					System.out.println("Disaggregating U3 for prob="+disaggProb+", iml="+iml);
					
					success = disaggCalc.disaggregate(Math.log(iml), site, gmpe, u3, calc.getSourceFilters(), calc.getAdjustableParams());
					if (!success)
						throw new RuntimeException("Disagg calc failed (see errors above, if any).");
					disaggCalc.setMaxZAxisForPlot(maxZAxis);
					System.out.println("Done Disaggregating");

					System.out.println("Fetching plot...");
					address = disaggCalc.getDisaggregationPlotUsingServlet(metadata);

					meanModeText = disaggCalc.getMeanAndModeInfo();
					binDataText = disaggCalc.getBinData();
					sourceDataText = disaggCalc.getDisaggregationSourceInfo();

					outputFile = new File(outputDir, prefix+"_u3");

					DisaggregationPlotViewerWindow.saveAsPDF(
							address+DisaggregationCalculator.DISAGGREGATION_PLOT_PDF_NAME,
							outputFile.getAbsolutePath()+".pdf", meanModeText, metadataText, binDataText, sourceDataText, null);
					FileUtils.downloadURL(address+DisaggregationCalculator.DISAGGREGATION_PLOT_PNG_NAME,
							new File(outputFile.getAbsolutePath()+".png"));
					DisaggregationPlotViewerWindow.saveAsTXT(outputFile.getAbsolutePath()+".txt", meanModeText, metadataText,
							binDataText, sourceDataText, null);
				}
			}
		}
	}
	
	private static DiscretizedFunc getCurve(DiscretizedFunc logCurve, DiscretizedFunc xVals) {
		Preconditions.checkState(logCurve.size() == xVals.size());
		ArbitrarilyDiscretizedFunc curve = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<xVals.size(); i++)
			curve.set(xVals.getX(i), logCurve.getY(i));
		return curve;
	}
	
	public static CPT getRPlogCPT(int[] rps) {
		int minRP = Integer.MAX_VALUE;
		int maxRP = 0;
		
		for (int rp : rps) {
			if (rp < minRP)
				minRP = rp;
			if (rp > maxRP)
				maxRP = rp;
		}
		return new CPT(Math.log10(minRP), Math.log10(maxRP), Color.LIGHT_GRAY, Color.BLACK);
	}

}
