package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
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
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.refFaultParamDb.vo.FaultSectionPrefData;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.UCERF2_AleatoryMagVarRemovalMod;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
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

import scratch.UCERF3.enumTreeBranches.FaultModels;
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
	
	static StudyRotDProvider getSimProv(CyberShakeStudy study, String siteName, File ampsCacheDir,
			double[] periods, CybershakeIM[] rd50_ims, Vs30_Source vs30Source) throws SQLException, IOException {
		return getSimProv(study, siteName, ampsCacheDir, periods, rd50_ims, null, vs30Source);
	}
	
	static StudyRotDProvider getSimProv(CyberShakeStudy study, String siteName, File ampsCacheDir,
			double[] periods, CybershakeIM[] rd50_ims, Site site) throws SQLException, IOException {
		return getSimProv(study, siteName, ampsCacheDir, periods, rd50_ims, site, null);
	}
	
	static StudyRotDProvider getSimProv(CyberShakeStudy study, String siteName, File ampsCacheDir, double[] periods,
			CybershakeIM[] rd50_ims, Site site, Vs30_Source vs30Source) throws SQLException, IOException {
		DBAccess db = study.getDB();
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		CybershakeSite csSite = sites2db.getSiteFromDB(siteName);
		
		System.out.println("Finding Run_ID for study "+study);
		String sql = "SELECT C.Run_ID FROM Hazard_Curves C JOIN CyberShake_Runs R ON R.Run_ID=C.Run_ID\n" + 
				"WHERE R.Site_ID="+csSite.id+" AND C.Hazard_Dataset_ID="+study.getDatasetID()+" ORDER BY C.Curve_Date DESC LIMIT 1";
		System.out.println(sql);
		ResultSet rs = db.selectData(sql);
		Preconditions.checkState(rs.first());
		int runID = rs.getInt(1);
		
		System.out.println("Detected Run_ID="+runID);
		
		CybershakeRun run = new Runs2DB(db).getRun(runID);
		
		if (site == null) {
			System.out.println("Building site");
			site = StudyGMPE_Compare.buildSite(csSite, runID, study, vs30Source, db);
		}
		
		Map<Site, Integer> runIDsMap = new HashMap<>();
		runIDsMap.put(site, runID);
		
		AbstractERF erf = study.getERF();
		CSRupture[][] csRups = new CSRupture[erf.getNumSources()][];
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, ampsCacheDir, erf);
		
		Map<Site, List<CSRupture>> siteRupsMap = new HashMap<>();
		
		List<CSRupture> siteRups = StudyGMPE_Compare.getSiteRuptures(site, amps2db, run.getERFID(), run.getRupVarScenID(),
				csRups, erf, runID, rd50_ims[0]);
		siteRupsMap.put(site, siteRups);
		
		return new StudyRotDProvider(amps2db, runIDsMap, siteRupsMap, periods, rd50_ims, null, study.getName());
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
			AbstractERF erf, Collection<CSRupture> ruptures, RSQSimCatalog catalog) {
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
			for (String faultName : faultNamesToIDsMap.keySet())
				for (Integer id : faultNamesToIDsMap.get(faultName))
					idsToFaultNamesMap.put(id, faultName);
			
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
					if (name == null)
						// not a named fault
						name = sect.getParentSectionName();
//					double fract = areas.get(i)/totArea;
					double fract = 1d;
					sourceFracts.put(name, fract);
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
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
		File bbpDir = new File("/data/kevin/bbp/parallel/2018_04_13-rundir2585_1myrs-all-m6.5-skipYears5000-noHF-csLASites");
		RSQSimCatalog catalog = Catalogs.BRUCE_2585_1MYR.instance(new File("/data/kevin/simulators/catalogs"));
		
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
		
//		String[] siteNames = { "USC" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC" };
		String[] siteNames = { "WNGC" };
		
		boolean replotCurves = false;
		boolean replotDisaggs = false;
		
//		AttenRelRef[] gmpeRefs = { AttenRelRef.NGAWest_2014_AVG_NOIDRISS, AttenRelRef.ASK_2014 };
//		AttenRelRef[] gmpeRefs = { AttenRelRef.NGAWest_2014_AVG_NOIDRISS };
		AttenRelRef[] gmpeRefs = { AttenRelRef.ASK_2014 };
		double[] periods = { 3, 5, 7.5, 10 };
		CybershakeIM[] rd50_ims = new PeakAmplitudesFromDB(study.getDB()).getIMs(Doubles.asList(periods),
				IMType.SA, CyberShakeComponent.RotD50).toArray(new CybershakeIM[0]);
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		for (String siteName : siteNames) {
			StudyRotDProvider mainProv = getSimProv(study, siteName, ampsCacheDir, periods, rd50_ims, vs30Source);
			
			Site site = mainProv.getAvailableSites().iterator().next();
			
			List<SimulationRotDProvider<?>> compSimProvs = new ArrayList<>();
			
			if (includeAleatoryStrip && study.getERF() instanceof MeanUCERF2)
				compSimProvs.add(new StudyModifiedProbRotDProvider(
						mainProv, new UCERF2_AleatoryMagVarRemovalMod(study.getERF()), study.getName()+" w/o Aleatory Mag"));
			
			Table<String, CSRupture, Double> sourceContribFracts =
					getSourceContribFracts(study.getERF(), mainProv.getRupturesForSite(site), catalog);
			
			for (CyberShakeStudy compStudy : compStudies) {
				StudyRotDProvider compProv = getSimProv(compStudy, siteName, ampsCacheDir, periods, rd50_ims, site);
				compSimProvs.add(compProv);
				if (includeAleatoryStrip && compStudy.getERF() instanceof MeanUCERF2)
					compSimProvs.add(new StudyModifiedProbRotDProvider(
							compProv, new UCERF2_AleatoryMagVarRemovalMod(compStudy.getERF()), compStudy.getName()+" w/o Aleatory Mag"));
			}
			
			LightweightBBP_CatalogSimZipLoader bbpCompProv = null;
			if (bbpDir != null) {
				bbpCompProv = loadBBP(bbpDir, site, catDurationYears);
				compSimProvs.add(bbpCompProv);
			}
			
			StudySiteHazardCurvePageGen pageGen = new StudySiteHazardCurvePageGen(mainProv, study.getName(), compSimProvs);
			pageGen.setReplotCurves(replotCurves);
			pageGen.setReplotDisaggs(replotDisaggs);
			pageGen.setSourceRupContributionFractions(sourceContribFracts, 4e-4, 10);
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
		
		study.writeMarkdownSummary(studyDir);
		CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		
		study.getDB().destroy();
		for (CyberShakeStudy compStudy : compStudies)
			compStudy.getDB().destroy();
		
		getExec().shutdown();
		
		System.exit(0);
	}

}
