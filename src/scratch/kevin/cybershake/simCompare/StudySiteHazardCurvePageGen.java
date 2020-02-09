package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.ZipFile;

import org.opensha.commons.data.Site;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.refFaultParamDb.vo.FaultSectionPrefData;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.UCERF2_AleatoryMagVarRemovalMod;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;

import scratch.kevin.bbp.BBP_Site;
import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.CSRuptureComparison;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simCompare.SiteHazardCurveComarePageGen;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;
import scratch.kevin.simulators.ruptures.LightweightBBP_CatalogSimZipLoader;

public class StudySiteHazardCurvePageGen extends SiteHazardCurveComarePageGen<CSRupture> {

	public StudySiteHazardCurvePageGen(SimulationRotDProvider<CSRupture> simProv, String simName,
			List<SimulationRotDProvider<?>> compSimProvs) {
		super(simProv, simName, compSimProvs);
	}
	
	public static List<CSRuptureComparison> calcComps(StudyRotDProvider prov, Site site,
			AttenRelRef gmpeRef, double[] periods) {
		List<CSRuptureComparison> compsList = new ArrayList<>();
		
		List<Future<?>> futures = new ArrayList<>();
		
		ExecutorService exec = getExec();
		
		for (CSRupture siteRup : prov.getRupturesForSite(site)) {
			CSRuptureComparison comp = new CSRuptureComparison(siteRup);
			comp.addApplicableSite(site);
			compsList.add(comp);
			futures.add(exec.submit(new GMPECalcRunnable(gmpeRef, site, comp, periods)));
		}
		
		System.out.println("Waiting on "+futures.size()+" GMPE futures");
		for (Future<?> future : futures) {
			try {
				future.get();
			} catch (Exception e) {
				ExceptionUtils.throwAsRuntimeException(e);
			}
		}
		System.out.println("DONE with GMPE calc");
		
		return compsList;
	}
	
	private static class GMPECalcRunnable implements Runnable {
		
		private AttenRelRef gmpeRef;
		private Site site;
		private CSRuptureComparison comp;
		private double[] periods;

		public GMPECalcRunnable(AttenRelRef gmpeRef, Site site, CSRuptureComparison comp, double[] periods) {
			this.gmpeRef = gmpeRef;
			this.site = site;
			this.comp = comp;
			this.periods = periods;
		}

		@Override
		public void run() {
			ScalarIMR gmpe = checkOutGMPE(gmpeRef);
			
			gmpe.setAll(comp.getGMPERupture(), site, gmpe.getIntensityMeasure());
			
			RuptureSurface surf = comp.getGMPERupture().getRuptureSurface();
			comp.setDistances(site, surf.getDistanceRup(site.getLocation()), surf.getDistanceJB(site.getLocation()));
			
			for (double period : periods) {
				SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
				comp.addResult(site, period, gmpe.getMean(), gmpe.getStdDev());
			}
			
			checkInGMPE(gmpeRef, gmpe);
		}
		
	}
	
	private static LightweightBBP_CatalogSimZipLoader loadBBP(File bbpDir, Site site, double durationYears) throws IOException {
		File bbpZip = new File(bbpDir, "results_rotD.zip");
		Preconditions.checkState(bbpZip.exists(), "BBP zip file not found: "+bbpZip.getAbsolutePath());
		File sitesFile = new File(bbpDir, "sites.stl");
		Preconditions.checkState(sitesFile.exists(), "BBP sites file not found: "+sitesFile.getAbsolutePath());
		
		double minDist = Double.POSITIVE_INFINITY;
		BBP_Site bbpSite = null;
		for (BBP_Site s : BBP_Site.readFile(sitesFile)) {
			double dist = LocationUtils.horzDistanceFast(site.getLocation(), s.getLoc());
			if (dist < minDist) {
				minDist = dist;
				bbpSite = s;
			}
		}
		Preconditions.checkState(minDist < 1d, "No BBP site within 1km of input site!");
		
		List<BBP_Site> bbpSites = new ArrayList<>();
		bbpSites.add(bbpSite);
		BiMap<BBP_Site, Site> gmpeSites = HashBiMap.create();
		gmpeSites.put(bbpSite, site);
		
		return new LightweightBBP_CatalogSimZipLoader(new ZipFile(bbpZip), bbpSites, gmpeSites, durationYears);
	}
	
	static Table<String, CSRupture, Double> getSourceContribFracts(
			AbstractERF erf, Collection<CSRupture> ruptures, RSQSimCatalog catalog, boolean fractional) {
		if (erf instanceof MeanUCERF2) {
			Map<String, List<Integer>> sourceNameToIDs = MeanUCERF2_ToDB.getFaultsToSourcesMap(erf);
			Map<Integer, String> sourceIDsToNames = new HashMap<>();
			for (String name : sourceNameToIDs.keySet())
				for (Integer sourceID : sourceNameToIDs.get(name))
					sourceIDsToNames.put(sourceID, name);
			Table<String, CSRupture, Double> table = HashBasedTable.create();
			for (CSRupture rup : ruptures) {
				int sourceID = rup.getSourceID();
				String name = sourceIDsToNames.get(sourceID);
				table.put(name, rup, 1d);
			}
			return table;
		} else if (erf instanceof RSQSimSectBundledERF) {
			List<Map<String, Double>> sourceFractsList = new ArrayList<>();
			Map<String, List<Integer>> faultNamesToIDsMap = catalog.getFaultModel().getNamedFaultsMapAlt();
			Map<Integer, String> idsToFaultNamesMap = new HashMap<>();
			for (String faultName : faultNamesToIDsMap.keySet()) {
				String name = faultName;
				if (name.startsWith("San Andreas"))
					name = "San Andreas";
				else if (name.startsWith("San Jacinto"))
					name = "San Jacinto";
				for (Integer id : faultNamesToIDsMap.get(faultName))
					idsToFaultNamesMap.put(id, name);
			}
			
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
				RSQSimSectBundledSource source = ((RSQSimSectBundledERF)erf).getSource(sourceID);
				List<FaultSectionPrefData> sects = source.getSortedSourceSects();
				double totArea = 0d;
				List<Double> areas = new ArrayList<>();
				for (FaultSectionPrefData sect : sects) {
					double area = sect.getOrigDownDipWidth()*sect.getTraceLength();
					totArea += area;
					areas.add(area);
				}
				Map<String, Double> sourceFracts = new HashMap<>();
				for (int i=0; i<sects.size(); i++) {
					FaultSectionPrefData sect = sects.get(i);
					int id = sect.getParentSectionId();
					String name = idsToFaultNamesMap.get(id);
					if (name == null) {
						// not a named fault
						name = sect.getParentSectionName();
						if (name.startsWith("San Andreas"))
							name = "San Andreas";
						else if (name.startsWith("San Jacinto"))
							name = "San Jacinto";
					}
					if (fractional) {
						double fract = areas.get(i)/totArea;
						if (sourceFracts.containsKey(name))
							fract += sourceFracts.get(name);
						sourceFracts.put(name, fract);
					} else {
						sourceFracts.put(name, 1d);
					}
				}
				sourceFractsList.add(sourceFracts);
			}
			
			Table<String, CSRupture, Double> table = HashBasedTable.create();
			for (CSRupture rup : ruptures) {
				Map<String, Double> fracts = sourceFractsList.get(rup.getSourceID());
				for (String name : fracts.keySet())
					table.put(name, rup, fracts.get(name));
			}
			
			return table;
		}
		return null;
	}

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		/*
		 * For RSQSim studies
		 */
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_PROTOTYPE_2457;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2018_04_12-rundir2457-all-m6.5-skipYears5000-noHF-standardSites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_2457.instance(new File("/data/kevin/simulators/catalogs"));
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
////		File bbpDir = new File("/data/kevin/bbp/parallel/2018_04_13-rundir2585_1myrs-all-m6.5-skipYears5000-noHF-csLASites");
//		File bbpDir = new File("/data/kevin/bbp/parallel/2019_11_11-rundir2585_1myrs-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_2585_1MYR.instance(new File("/data/kevin/simulators/catalogs"));
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_9_RSQSIM_2740;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2018_09_10-rundir2740-all-m6.5-skipYears5000-noHF-csLASites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_2740.instance(new File("/data/kevin/simulators/catalogs"));

//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_2_RSQSIM_4841;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2020_02_03-rundir4841-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_4841.instance();
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_20_2_RSQSIM_4860;
		File bbpDir = new File("/data/kevin/bbp/parallel/2020_02_07-rundir4860-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
		RSQSimCatalog catalog = Catalogs.BRUCE_4841.instance();
		
		Vs30_Source vs30Source = Vs30_Source.Simulation;
//		CyberShakeStudy[] compStudies = { CyberShakeStudy.STUDY_15_4 };
		CyberShakeStudy[] compStudies = {  };
		double catDurationYears = catalog.getDurationYears() - 5000d;
		System.out.println("Catalog duration: "+(int)Math.round(catDurationYears)+" years");
		
		/*
		 * For regular studies
		 */
//		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
//		Vs30_Source vs30Source = Vs30_Source.Simulation;
//		CyberShakeStudy[] compStudies = { };
//		
//		RSQSimCatalog catalog = null;
//		File bbpDir = null;
//		double catDurationYears = -1;
		
		boolean includeAleatoryStrip = true;
		
		String[] siteNames = { "USC" };
//		String[] siteNames = { "OSI", "PDE", "s022" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC", "s119", "s279", "s480" };
//		String[] siteNames = { "LAPD", "SBSM", "PAS", "WNGC" };
//		String[] siteNames = { "s119", "s279", "s480" };
//		String[] siteNames = { "LAPD" };
//		String[] siteNames = { "SMCA" };
		
		boolean replotCurves = true;
		boolean replotDisaggs = false;
		
//		AttenRelRef[] gmpeRefs = { AttenRelRef.NGAWest_2014_AVG_NOIDRISS, AttenRelRef.ASK_2014 };
//		AttenRelRef[] gmpeRefs = { AttenRelRef.NGAWest_2014_AVG_NOIDRISS };
		AttenRelRef[] gmpeRefs = { AttenRelRef.ASK_2014 };
		double[] periods = { 3, 5, 7.5, 10 };
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		try {
			CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, study.getERF());
			StudyRotDProvider mainProv = new StudyRotDProvider(study, amps2db, periods, study.getName());
			
			for (String siteName : siteNames) {
				List<CybershakeRun> matchingRuns = study.runFetcher().forSiteNames(siteName).fetch();
				Preconditions.checkState(!matchingRuns.isEmpty(), "Must have at least 1 run for the given site/study");
				Site site = CyberShakeSiteBuilder.buildSites(study, vs30Source, matchingRuns).get(0);
				
				List<SimulationRotDProvider<?>> compSimProvs = new ArrayList<>();
				
				if (includeAleatoryStrip && study.getERF() instanceof MeanUCERF2)
					compSimProvs.add(new StudyModifiedProbRotDProvider(
							mainProv, new UCERF2_AleatoryMagVarRemovalMod(study.getERF()), study.getName()+" w/o Aleatory Mag"));
				
				Table<String, CSRupture, Double> sourceContribFracts =
						getSourceContribFracts(study.getERF(), mainProv.getRupturesForSite(site), catalog, false);
				
				for (CyberShakeStudy compStudy : compStudies) {
					// strip the run out of the site so we don't get bad associations
					if (site instanceof CyberShakeSiteRun) {
						Site oSite = new Site(site.getLocation(), site.getName());
						oSite.addParameterList(oSite);
						site = oSite;
					}
					CachedPeakAmplitudesFromDB compAmps2db = new CachedPeakAmplitudesFromDB(compStudy.getDB(), ampsCacheDir, compStudy.getERF());
					StudyRotDProvider compProv = new StudyRotDProvider(compStudy, compAmps2db, periods, compStudy.getName());
					compSimProvs.add(compProv);
					if (includeAleatoryStrip && compStudy.getERF() instanceof MeanUCERF2)
						compSimProvs.add(new StudyModifiedProbRotDProvider(
								compProv, new UCERF2_AleatoryMagVarRemovalMod(compStudy.getERF()), compStudy.getName()+" w/o Aleatory Mag"));
				}
				
				LightweightBBP_CatalogSimZipLoader bbpCompProv = null;
				if (bbpDir != null) {
					try {
						bbpCompProv = loadBBP(bbpDir, site, catDurationYears);
						compSimProvs.add(bbpCompProv);
					} catch (IllegalStateException e) {
						System.err.println("BBP load error for site "+siteName+": "+e.getMessage());
					}
				}
				
				StudySiteHazardCurvePageGen pageGen = new StudySiteHazardCurvePageGen(mainProv, study.getName(), compSimProvs);
				pageGen.setReplotCurves(replotCurves);
				pageGen.setReplotDisaggs(replotDisaggs);
//				pageGen.setSourceRupContributionFractions(sourceContribFracts, 4e-4, 10);
				pageGen.setSourceRupContributionFractions(sourceContribFracts, 0d, 10); // 0 = RTGM
				if (bbpCompProv != null)
					pageGen.setCustomPlotColors(bbpCompProv, new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.ORANGE));
				
				for (AttenRelRef gmpeRef : gmpeRefs) {
					File outputDir = new File(studyDir, "site_hazard_"+siteName+"_"+gmpeRef.getShortName()+"_Vs30"+vs30Source.name());
					Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
					
					List<CSRuptureComparison> comps = calcComps(mainProv, site, gmpeRef, periods);
					
					List<String> headerLines = new ArrayList<>();
					headerLines.add("# "+study.getName()+" "+siteName+" Hazard Curves");
					headerLines.add("");
					headerLines.add("**GMPE: "+gmpeRef.getName()+", Vs30 Source: "+vs30Source+"**");
					headerLines.add("");
					headerLines.add("**Study Details**");
					headerLines.add("");
					headerLines.addAll(study.getMarkdownMetadataTable());
					headerLines.add("");
					
					pageGen.generateSitePage(site, comps, outputDir, headerLines, periods, gmpeRef);
				}
			}
			
			System.out.println("Done, writing summary");
			study.writeMarkdownSummary(studyDir);
			System.out.println("Writing studies index");
			CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Shutting down DB connections");
		
		study.getDB().destroy();
		for (CyberShakeStudy compStudy : compStudies)
			compStudy.getDB().destroy();
		
		System.out.println("Shutting down executor");
		
		getExec().shutdown();
		
		System.out.println("Exiting");
		System.exit(0);
	}

}
