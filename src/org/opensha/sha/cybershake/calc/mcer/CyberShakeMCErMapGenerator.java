package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.XMLUtils;
import org.opensha.sha.calc.mcer.AbstractMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.AbstractMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedCurveBasedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.CachedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CombinedMultiMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.CombinedMultiMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CurveBasedMCErProbabilisitCalc;
import org.opensha.sha.calc.mcer.GMPE_MCErDeterministicCalc;
import org.opensha.sha.calc.mcer.GMPE_MCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.calc.mcer.MCErMapGenerator;
import org.opensha.sha.calc.mcer.WeightProvider;
import org.opensha.sha.calc.mcer.WeightedAverageMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.WeightedAverageMCErProbabilisticCalc;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.calc.RupProbModERF;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;
import org.opensha.sha.util.SiteTranslator;

import scratch.UCERF3.erf.FaultSystemSolutionERF;
import scratch.UCERF3.erf.mean.MeanUCERF3;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CyberShakeMCErMapGenerator {
	
	private static final boolean cache_cybershake = true;
	private static final boolean cache_gmpe = true;
	
	public static void calculateMaps(int datasetID, CyberShakeComponent component, double period,
			ERF erf, List<AttenuationRelationship> gmpes, File outputDir) throws IOException, GMT_MapException {
		calculateMaps(datasetID, component, period, erf, erf, gmpes, outputDir, false, null);
	}
	
	public static void calculateMaps(int datasetID, CyberShakeComponent component, double period,
			ERF erf, ERF gmpeERF, List<AttenuationRelationship> gmpes, File outputDir, boolean weightAverage, File gmpeCacheDir)
					throws IOException, GMT_MapException {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		
		CybershakeIM im = CyberShakeMCErProbabilisticCalc.getIMsForPeriods(db, component, Lists.newArrayList(period)).get(0);
		
		HazardCurveFetcher fetcher = new HazardCurveFetcher(db, datasetID, im.getID());
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, MCERDataProductsCalc.cacheDir, erf);
		
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		
		/*
		 * Create site list
		 */
		List<Site> sites = getSitesList(fetcher, gmpes);
		writeSitesFile(new File(outputDir, "sites.xml"), sites);
		
		/*
		 * Create calculators
		 */
		
		AbstractMCErDeterministicCalc csDetCalc = new CyberShakeMCErDeterministicCalc(amps2db, erf, component);
		CurveBasedMCErProbabilisitCalc csProbCalc = new CyberShakeMCErProbabilisticCalc(db, component);
		
		RuptureProbabilityModifier detProbMod = ((CyberShakeMCErDeterministicCalc)csDetCalc).getRupProbMod();
		
		if (cache_cybershake) {
			// now cache
			File cacheDir = new File(outputDir, ".cs_cache");
			Preconditions.checkState(cacheDir.exists() || cacheDir.mkdir());
			
			String cachePrefix = "cs_dataset"+datasetID+"_"+component.name();
			File detCacheFile = new File(cacheDir, cachePrefix+"_deterministic.xml");
			File probCacheFile = new File(cacheDir, cachePrefix+"_probabilistic_curve.xml");
			
			csDetCalc = new CachedMCErDeterministicCalc(csDetCalc, detCacheFile);
//			csProbCalc = new CachedMCErProbabilisticCalc(csProbCalc, probCacheFile);
			csProbCalc = new CachedCurveBasedMCErProbabilisticCalc(csProbCalc, probCacheFile);
		}
		
		AbstractMCErDeterministicCalc gmpeDetCalc = null;
		AbstractMCErProbabilisticCalc gmpeProbCalc = null;
		
		List<CurveBasedMCErProbabilisitCalc> gmpeProbCalcs = null;
		if (gmpes != null && !gmpes.isEmpty()) {
			Component gmpeComponent = MCErCalcUtils.getSupportedTranslationComponent(
					gmpes.get(0), component.getGMPESupportedComponents());
			Preconditions.checkNotNull(gmpeComponent);
			
			gmpes.get(0).setIntensityMeasure(SA_Param.NAME);
			SA_Param.setPeriodInSA_Param(gmpes.get(0).getIntensityMeasure(), period);
			DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(gmpes.get(0).getIntensityMeasure());
			
			List<AbstractMCErDeterministicCalc> detCalcs = Lists.newArrayList();
			gmpeProbCalcs = Lists.newArrayList();
			
			if (gmpeERF == null)
				gmpeERF = erf;
			
			ERF gmpeDetERF = MCERDataProductsCalc.getGMPEDetERF(gmpeERF, detProbMod);
			
			for (AttenuationRelationship gmpe : gmpes) {
				detCalcs.add(new GMPE_MCErDeterministicCalc(gmpeDetERF, gmpe, gmpeComponent));
				gmpeProbCalcs.add(new GMPE_MCErProbabilisticCalc(gmpeERF, gmpe, gmpeComponent, xVals));
			}
			
			if (gmpeCacheDir != null) {
				for (int i=0; i<gmpes.size(); i++) {
					AttenuationRelationship gmpe = gmpes.get(i);
					String cachePrefix = CyberShakeMCErMapGenerator.getCachePrefix(
							-1, gmpeERF, gmpeComponent, Lists.newArrayList(gmpe));
					
					detCalcs.set(i, new CachedMCErDeterministicCalc(detCalcs.get(i),
							new File(gmpeCacheDir, cachePrefix+"_deterministic.xml")));
					gmpeProbCalcs.set(i, new CachedCurveBasedMCErProbabilisticCalc(gmpeProbCalcs.get(i),
							new File(gmpeCacheDir, cachePrefix+"_probabilistic_curve.xml")));
				}
			}
			
			if (gmpes.size() == 1){
				gmpeDetCalc = detCalcs.get(0);
				gmpeProbCalc = gmpeProbCalcs.get(0);
			} else {
				// this will take the max determ val from each GMPE
				gmpeDetCalc = new CombinedMultiMCErDeterministicCalc(detCalcs);
				// this will average the prob values from each GMPE
				gmpeProbCalc = new CombinedMultiMCErProbabilisticCalc(gmpeProbCalcs);
			}
			
			if (cache_gmpe) {
				// now cache
				File cacheDir = new File(outputDir, ".gmpe_cache");
				Preconditions.checkState(cacheDir.exists() || cacheDir.mkdir());
				
				String cachePrefix = getCachePrefix(datasetID, erf,
						gmpeComponent, gmpes);
				File detCacheFile = new File(cacheDir, cachePrefix+"_deterministic.xml");
				File probCacheFile = new File(cacheDir, cachePrefix+"_probabilistic.xml");
				
				gmpeDetCalc = new CachedMCErDeterministicCalc(gmpeDetCalc, detCacheFile);
				gmpeProbCalc = new CachedMCErProbabilisticCalc(gmpeProbCalc, probCacheFile);
			}
		}
		
		outputDir = new File(outputDir, getPeriodDirName(period));
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		MCErMapGenerator.calculateMaps("CyberShake", null, csProbCalc, csDetCalc, "GMPE", gmpeProbCalc, gmpeDetCalc,
				region, sites, period, outputDir);
		
		if (weightAverage) {
			System.out.println("Generating Averaged Maps");
			WeightProvider weightProv = new CyberShakeWeightProvider(csProbCalc, gmpeProbCalcs, csDetCalc, gmpeDetCalc);
			
			WeightedAverageMCErDeterministicCalc avgDetCalc =
					new WeightedAverageMCErDeterministicCalc(weightProv, csDetCalc, gmpeDetCalc);
			List<CurveBasedMCErProbabilisitCalc> allProbCalcs = Lists.newArrayList();
			allProbCalcs.add(csProbCalc);
			allProbCalcs.addAll(gmpeProbCalcs);
			WeightedAverageMCErProbabilisticCalc avgProbCalc =
					new WeightedAverageMCErProbabilisticCalc(weightProv, allProbCalcs);
			
			MCErMapGenerator.calculateMaps("Weight Avg", "weight_avg", avgProbCalc, avgDetCalc, null, null, null,
					region, sites, period, outputDir);
		}
	}
	
	static String getPeriodDirName(double period) {
		if (period == (double)(int)period)
			return (int)period+"s";
		else
			return (float)period+"s";
	}
	
	private static String getERFName(ERF erf) {
		if (erf instanceof MeanUCERF2) {
			return "UCERF2";
		} else if (erf instanceof MeanUCERF3) {
			if (((MeanUCERF3)erf).isTrueMean())
				return "MeanUCERF3_full";
			else
				return "MeanUCERF3_downsampled";
		} else if (erf instanceof FaultSystemSolutionERF) {
			return "UCERF3";
		} else {
			return ClassUtils.getClassNameWithoutPackage(erf.getClass());
		}
	}

	static String getCachePrefix(int datasetID, ERF erf,
			Component gmpeComponent, List<? extends ScalarIMR> gmpes) {
		String erfName = getERFName(erf);
		
		List<String> gmpeNames = Lists.newArrayList();
		for (ScalarIMR gmpe : gmpes)
			gmpeNames.add(gmpe.getShortName());
		
		String cachePrefix = Joiner.on("_").join(gmpeNames)+"_"+erfName;
		if (datasetID >= 0)
			cachePrefix += "_dataset"+datasetID;
		cachePrefix += "_"+gmpeComponent.name();
		return cachePrefix;
	}

	private static List<Site> getSitesList(HazardCurveFetcher fetcher, List<AttenuationRelationship> gmpes) {
		List<CybershakeSite> csSites = fetcher.getCurveSites();
		List<Integer> runIDs = fetcher.getRunIDs();
		
		List<Site> sites = Lists.newArrayList();
		
		// to filter out duplicates
		HashSet<Location> locs = new HashSet<Location>();
		
		OrderedSiteDataProviderList provs = null; // will be created when we have a velocity model ID from a CybershakeRun
		SiteTranslator siteTrans = new SiteTranslator();
		ParameterList siteParams = MCERDataProductsCalc.getSiteParams(gmpes);
		
		Runs2DB runs2db = new Runs2DB(fetcher.getDBAccess());
		
		for (int i=0; i<csSites.size(); i++) {
			CybershakeSite csSite = csSites.get(i);
			if (csSite.type_id == CybershakeSite.TYPE_TEST_SITE)
				continue;
			Location loc = csSite.createLocation();
			if (locs.contains(loc))
				continue;

			int runID = runIDs.get(i);
			CybershakeRun run = runs2db.getRun(runID);
			
			if (provs == null)
				provs = HazardCurvePlotter.createProviders(run.getVelModelID());
			
			CyberShakeSiteRun site = new CyberShakeSiteRun(csSite, run);
			ArrayList<SiteDataValue<?>> datas = provs.getBestAvailableData(site.getLocation());
			for (Parameter<?> param : siteParams) {
				param = (Parameter<?>)param.clone();
				siteTrans.setParameterValue(param, datas);
				site.addParameter(param);
			}
			sites.add(site);
			
			locs.add(loc);
		}
		return sites;
	}
	
	static void writeSitesFile(File file, List<Site> sites) throws IOException {
		Document doc = XMLUtils.createDocumentWithRoot();
		Element root = doc.getRootElement();
		
		for (Site site : sites)
			site.toXMLMetadata(root);
		
		XMLUtils.writeDocumentToFile(file, doc);
	}
	
	public static void main(String[] args) throws IOException, GMT_MapException {
		int datasetID = 57;
		String studyName = "study_15_4";
		MCErMapGenerator.doPSV = false;
		MCErMapGenerator.plot_log = false;
		MCErMapGenerator.interpSettings.setInterpSpacing(0.01);
		
		CyberShakeComponent component = CyberShakeComponent.RotD100;
		double[] periods = { 2,3,4,5,7.5,10 };
//		double period = 10d;
		
		boolean weightAverage = false;

		ERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		List<AttenuationRelationship> gmpes = null;
//		List<AttenuationRelationship> gmpes = Lists.newArrayList();
//		gmpes.add(AttenRelRef.ASK_2014.instance(null));
//		gmpes.add(AttenRelRef.BSSA_2014.instance(null));
//		gmpes.add(AttenRelRef.CB_2014.instance(null));
//		gmpes.add(AttenRelRef.CY_2014.instance(null));
//		for (AttenuationRelationship gmpe : gmpes)
//			gmpe.setParamDefaults();
		
		ERF gmpeERF = null;
//		MeanUCERF3 gmpeERF = new MeanUCERF3();
//		gmpeERF.setMeanParams(0d, true, 0d, MeanUCERF3.RAKE_BASIS_NONE);
//		gmpeERF.updateForecast();
//		
//		File gmpeCacheDir = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen/2015_09_29-ucerf3_full_ngaw2");
		File gmpeCacheDir = null;
		
		String outputName = studyName+"_"+component.name().toLowerCase();
		
		if (gmpeERF != null)
			outputName += "_gmpe"+getERFName(gmpeERF);

		File outputDir = new File("/home/kevin/CyberShake/MCER/maps/"+outputName);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdirs());

		for (double period : periods) {
			System.out.println("Period: "+(float)period+"s");
			calculateMaps(datasetID, component, period, erf, gmpeERF, gmpes, outputDir, weightAverage, gmpeCacheDir);
		}

		//// UCERF3/UCERF2 comparisons
		//twoPercentIn50 = true;
		//
		//String twoP_add = "";
		//if (twoPercentIn50 == true)
		//	twoP_add = "_2pin50";
		//File outputDir = new File("/home/kevin/CyberShake/MCER/maps/ucerf3_ucerf2_gmpe_rotd100/"+(int)period+"s"+twoP_add);
		//Preconditions.checkState(outputDir.exists() || outputDir.mkdirs());
		//
		//ERF ucerf2 = MeanUCERF2_ToDB.createUCERF2ERF();
		//ERF ucerf3 = new FaultSystemSolutionERF(FaultSystemIO.loadSol(new File(
		//		"/home/kevin/workspace/OpenSHA/dev/scratch/UCERF3/data/scratch/InversionSolutions/"
		//		+ "2013_05_10-ucerf3p3-production-10runs_COMPOUND_SOL_FM3_1_MEAN_BRANCH_AVG_SOL.zip")));
		//ucerf3.setParameter(ProbabilityModelParam.NAME, ProbabilityModelOptions.POISSON);
		//ucerf3.getTimeSpan().setDuration(1d);
		//ucerf3.setParameter(IncludeBackgroundParam.NAME, IncludeBackgroundOption.EXCLUDE);
		//ucerf3.updateForecast();
		//
		//List<ERF> erfs = Lists.newArrayList(ucerf3, ucerf2);
		//List<String> names = Lists.newArrayList("UCERF3", "UCERF2");
		//
		//List<AttenuationRelationship> gmpes = Lists.newArrayList();
		//gmpes.add(AttenRelRef.ASK_2014.instance(null));
		//gmpes.add(AttenRelRef.BSSA_2014.instance(null));
		//gmpes.add(AttenRelRef.CB_2014.instance(null));
		//gmpes.add(AttenRelRef.CY_2014.instance(null));
		//for (AttenuationRelationship gmpe : gmpes)
		//	gmpe.setParamDefaults();
		//
		//generateGMPEOnlyMaps(erfs, names, gmpes, datasetID, imTypeID, period, new WillsMap2006(), outputDir);

		System.exit(0);
	}

}
