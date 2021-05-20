package scratch.kevin.cybershake.ugms;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.calc.HazardCurveCalculator;
import org.opensha.sha.calc.disaggregation.DisaggregationCalculator;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.MCERDataProductsCalc;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.plot.DisaggregationPlotter;
import org.opensha.sha.cybershake.plot.PlotType;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.param.ProbabilityModelOptions;
import org.opensha.sha.earthquake.param.ProbabilityModelParam;
import org.opensha.sha.gui.infoTools.DisaggregationPlotViewerWindow;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;

import scratch.UCERF3.erf.mean.MeanUCERF3;
import scratch.UCERF3.erf.mean.MeanUCERF3.Presets;

public class DisaggCompare {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		
		boolean doCS = false;
		boolean doGMPE = true;
		
		File outputDir = new File("/home/kevin/CyberShake/MCER/disagg_comparisons");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		double[] periods = { 5d };
		String[] siteNames = { "SBSM", "PAS", "COO", "USC", "STNI" };
//		String[] siteNames = { "STNI" };
		double[] imLevels = {  };
		double[] probs = { 4e-4 };
		
		AbstractERF csERF = study.getERF();
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), MCERDataProductsCalc.cacheDir, csERF);
		List<CybershakeIM> csIMs = new ArrayList<>();
		for (double period : periods)
			csIMs.add(CybershakeIM.getSA(CyberShakeComponent.RotD100, period));
		
		AttenuationRelationship gmpe = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		gmpe.setParamDefaults();
		gmpe.setIntensityMeasure(SA_Param.NAME);
		HazardCurveCalculator curveCalc = new HazardCurveCalculator();
		MeanUCERF3 gmpeERF = null;
		if (doGMPE) {
			gmpeERF = new MeanUCERF3();
			gmpeERF.setPreset(Presets.COMPLETE_MODEL);
//			if (gmpeExcludeBackground)
//				erf.setParameter(IncludeBackgroundParam.NAME, IncludeBackgroundOption.EXCLUDE);
			gmpeERF.setParameter(ProbabilityModelParam.NAME, ProbabilityModelOptions.POISSON);
			gmpeERF.getTimeSpan().setDuration(1d);
			gmpeERF.updateForecast();
		}
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		DiscretizedFunc logXVals = new ArbitrarilyDiscretizedFunc();
		for (Point2D pt : xVals)
			logXVals.set(Math.log(pt.getX()), pt.getY());
		
		double minMag = 6d;
		double deltaMag = 0.2;
		int numMags = (int)((8.6d - minMag)/deltaMag + 0.5);
		int numSourcesForDisag = 100;
		boolean showSourceDistances = true;
		double maxZAxis = Double.NaN;
		DisaggregationCalculator gmpeDisagg = new DisaggregationCalculator();
		gmpeDisagg.setMagRange(minMag, numMags, deltaMag);
		gmpeDisagg.setNumSourcestoShow(numSourcesForDisag);
		gmpeDisagg.setShowDistances(showSourceDistances);
		
		CyberShakeSiteBuilder siteBuilder = new CyberShakeSiteBuilder(Vs30_Source.Wills2015, study.getVelocityModelID());
		
		SiteInfo2DB sites2db = new SiteInfo2DB(study.getDB());
		
		for (String siteName : siteNames) {
			CybershakeRun run = study.runFetcher().forSiteNames(siteName).fetch().get(0);
			
			if (doCS) {
				System.out.println("Doing CyberShake, "+siteName);
				System.out.println("Run: "+run);
				
				DisaggregationPlotter csDisagg = new DisaggregationPlotter(study.getDB(), amps2db, run.getRunID(), csERF,
						csIMs, null, Doubles.asList(probs), Doubles.asList(imLevels), outputDir,
						Lists.newArrayList(PlotType.PDF, PlotType.PNG, PlotType.TXT));
				csDisagg.setMagRange(minMag, numMags, deltaMag);
				csDisagg.disaggregate();
			}
			
			if (doGMPE) {
				System.out.println("Doing GMPE, "+siteName);
				
				Site site = siteBuilder.buildSite(run, sites2db.getSiteFromDB(run.getSiteID()));
				for (Parameter<?> param : site)
					System.out.println("\t"+param.getName()+": "+param.getValue());
				
				for (double period : periods) {
					SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
					List<Double> imls = new ArrayList<>();
					List<String> outNames = new ArrayList<>();
					for (double iml : imLevels) {
						imls.add(Math.log(iml));
						outNames.add(siteName+"_GMPE_DisaggIML_"+(float)iml+"_SA_"+optionalDigitDF.format(period)+"sec");
					}
					for (double prob : probs) {
						System.out.println("Calculating hazard curve to get IML...");
						curveCalc.getHazardCurve(logXVals, site, gmpe, gmpeERF);
						System.out.println("Curve: "+logXVals);
						for (int i=0; i<logXVals.size(); i++)
							xVals.set(i, logXVals.getY(i));
						double iml = xVals.getFirstInterpolatedX_inLogXLogYDomain(prob);
						System.out.println("IML: "+iml);
						imls.add(Math.log(iml));
						outNames.add(siteName+"_GMPE_DisaggPOE_"+(float)prob+"_SA_"+optionalDigitDF.format(period)+"sec");
					}
					for (int i=0; i<imls.size(); i++) {
						double iml = imls.get(i);
						String outputFileName = new File(outputDir, outNames.get(i)).getAbsolutePath();
						Preconditions.checkState(gmpeDisagg.disaggregate(iml, site, gmpe, gmpeERF, curveCalc.getAdjustableParams()));
						gmpeDisagg.setMaxZAxisForPlot(maxZAxis);
						String address = gmpeDisagg.getDisaggregationPlotUsingServlet("asfd");
						DisaggregationPlotViewerWindow.saveAsPDF(
								address+DisaggregationCalculator.DISAGGREGATION_PLOT_PDF_NAME,
								outputFileName+".pdf", "safda", "asdf", "asfda", "asdf");
						FileUtils.downloadURL(address+ DisaggregationCalculator.DISAGGREGATION_PLOT_PNG_NAME, new File(outputFileName+".png"));
					}
				}
			}
		}
		
		study.getDB().destroy();
	}

	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.#");

}
