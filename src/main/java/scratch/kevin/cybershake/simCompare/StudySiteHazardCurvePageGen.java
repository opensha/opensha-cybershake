package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipFile;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.UCERF2_AleatoryMagVarRemovalMod;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.FaultSection;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Table;

import scratch.kevin.bbp.BBP_Site;
import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.CSRuptureComparison;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simCompare.SiteHazardCurveComarePageGen;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;
import scratch.kevin.simulators.ruptures.LightweightBBP_CatalogSimZipLoader;

public class StudySiteHazardCurvePageGen extends SiteHazardCurveComarePageGen<CSRupture> {

	private String simName;
	private SimulationRotDProvider<CSRupture> studyProv;
	private List<SimulationRotDProvider<?>> compSimProvs;

	public StudySiteHazardCurvePageGen(SimulationRotDProvider<CSRupture> simProv, String simName,
			List<SimulationRotDProvider<?>> compSimProvs) {
		super(simProv, simName, compSimProvs);
		this.studyProv = simProv;
		this.simName = simName;
		this.compSimProvs = compSimProvs;
	}
	
	public static List<CSRuptureComparison> calcComps(SimulationRotDProvider<CSRupture> prov, Site site,
			AttenRelRef gmpeRef, IMT[] imts) {
		List<CSRuptureComparison> compsList = new ArrayList<>();
		
		List<Future<?>> futures = new ArrayList<>();
		
		ExecutorService exec = getExec();
		
		for (CSRupture siteRup : prov.getRupturesForSite(site)) {
			CSRuptureComparison comp = new CSRuptureComparison(siteRup);
			comp.addApplicableSite(site);
			compsList.add(comp);
			futures.add(exec.submit(new GMPECalcRunnable(gmpeRef, site, comp, imts)));
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
		private IMT[] imts;

		public GMPECalcRunnable(AttenRelRef gmpeRef, Site site, CSRuptureComparison comp, IMT[] imts) {
			this.gmpeRef = gmpeRef;
			this.site = site;
			this.comp = comp;
			this.imts = imts;
		}

		@Override
		public void run() {
			ScalarIMR gmpe = checkOutGMPE(gmpeRef);
			
			gmpe.setAll(comp.getGMPERupture(), site, gmpe.getIntensityMeasure());
			
			RuptureSurface surf = comp.getGMPERupture().getRuptureSurface();
			comp.setDistances(site, surf.getDistanceRup(site.getLocation()), surf.getDistanceJB(site.getLocation()));
			
			for (IMT imt : imts) {
				imt.setIMT(gmpe);
				comp.addResult(site, imt, gmpe.getMean(), gmpe.getStdDev());
			}
			
			checkInGMPE(gmpeRef, gmpe);
		}
		
	}
	
	public static LightweightBBP_CatalogSimZipLoader loadBBP(File bbpDir, Site site, RSQSimCatalog catalog) throws IOException {
		double catDurationYears = catalog.getDurationYears();
		String bbpDirName = bbpDir.getName();
		if (bbpDirName.contains("-skipYears")) {
			String yearStr = bbpDirName.substring(bbpDirName.indexOf("-skipYears")+"-skipYears".length());
			if (yearStr.contains("-"))
				yearStr = yearStr.substring(0, yearStr.indexOf("-"));
			int skipYears = Integer.parseInt(yearStr);
			System.out.println("Detected BBP skipYears="+skipYears);
			catDurationYears -= skipYears;
		}
		System.out.println("Catalog duration: "+(int)Math.round(catDurationYears)+" years");
		
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
		
		return new LightweightBBP_CatalogSimZipLoader(new ZipFile(bbpZip), bbpSites, gmpeSites, catDurationYears);
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
				List<FaultSection> sects = source.getSortedSourceSects();
				double totArea = 0d;
				List<Double> areas = new ArrayList<>();
				for (FaultSection sect : sects) {
					double area = sect.getOrigDownDipWidth()*sect.getTraceLength();
					totArea += area;
					areas.add(area);
				}
				Map<String, Double> sourceFracts = new HashMap<>();
				for (int i=0; i<sects.size(); i++) {
					FaultSection sect = sects.get(i);
					int id = sect.getParentSectionId();
					String name = idsToFaultNamesMap.get(id);
					if (name == null) {
						// not a named fault
						name = sect.getParentSectionName();
						if (name.startsWith("San Andreas"))
							name = "San Andreas";
						else if (name.startsWith("San Jacinto"))
							name = "San Jacinto";
						if (name.contains(" alt"))
							name = name.substring(0, name.indexOf("alt")).trim();
						if (name.contains("("))
							name = name.substring(0, name.indexOf("(")).trim();
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
	
	private void writeGroundMotionCSV(File csvFile, Site site, AttenRelRef gmpeRef,
			List<CSRuptureComparison> comps, IMT[] imts) throws IOException {
		CSVFile<String> csv = new CSVFile<>(true);
		
		List<String> header = new ArrayList<>();
		
		AbstractERF studyERF = null;
		if (studyProv instanceof StudyRotDProvider)
			studyERF = ((StudyRotDProvider)studyProv).getERF();
		else if (studyProv instanceof RSQSimSubsetStudyRotDProvider)
			studyERF = ((RSQSimSubsetStudyRotDProvider)studyProv).getERF();
		
		final RSQSimSectBundledERF rsERF = studyERF instanceof RSQSimSectBundledERF
				? (RSQSimSectBundledERF)studyERF : null;
		
		if (rsERF != null) {
			header.add("Event ID");
		} else {
			header.add("Source ID");
			header.add("Rupture ID");
			header.add("Rupture Variation ID");
		}
		header.add("Magnitude");
		header.add("DistanceRup (km)");
		header.add("DistanceJB (km)");
		header.add("Annual Rate");
		
		for (IMT imt : imts) {
			header.add(simName+" ln("+imt.getShortName()+" RotD50)");
			header.add(gmpeRef.getShortName()+" ln("+imt.getShortName()+" Median RotD50)");
			header.add(gmpeRef.getShortName()+" "+imt.getShortName()+" Standard Deviation");
		}
		csv.addLine(header);
		
		if (rsERF != null) {
			List<CSRuptureComparison> sortedComps = new ArrayList<>(comps);
			sortedComps.sort(new Comparator<CSRuptureComparison>() {

				@Override
				public int compare(CSRuptureComparison o1, CSRuptureComparison o2) {
					CSRupture r1 = o1.getRupture();
					int event1 = rsERF.getRupture(r1.getSourceID(), r1.getRupID()).getEventID();
					CSRupture r2 = o2.getRupture();
					int event2 = rsERF.getRupture(r2.getSourceID(), r2.getRupID()).getEventID();
					return Integer.compare(event1, event2);
				}
			});
			comps = sortedComps;
		}
		
		for (CSRuptureComparison comp : comps) {
			if (!comp.isSiteApplicable(site))
				continue;
			CSRupture rup = comp.getRupture();
			double rate = comp.getAnnualRate();
			double mag = comp.getMagnitude();
			double rRup = comp.getDistanceRup(site);
			double rJB = comp.getDistanceJB(site);
			
			int numRVs = studyProv.getNumSimulations(site, rup);
			double rateEach = rate/(double)numRVs;
			for (int i=0; i<numRVs; i++) {
				List<String> line = new ArrayList<>(header.size());
				if (rsERF == null) {
					line.add(rup.getSourceID()+"");
					line.add(rup.getRupID()+"");
					line.add(i+"");
				} else {
					int eventID = rsERF.getRupture(rup.getSourceID(), rup.getRupID()).getEventID();
					line.add(eventID+"");
				}
				line.add((float)mag+"");
				line.add((float)rRup+"");
				line.add((float)rJB+"");
				line.add((float)rateEach+"");
				for (IMT imt : imts) {
					line.add((float)Math.log(studyProv.getValue(site, rup, imt, i))+"");
					line.add((float)comp.getLogMean(site, imt)+"");
					line.add((float)comp.getStdDev(site, imt)+"");
				}
				csv.addLine(line);
			}
		}
		if (csvFile.getName().toLowerCase().endsWith(".gz")) {
			GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(csvFile));
			csv.writeToStream(gz);
			gz.close();
		} else {
			csv.writeToFile(csvFile);
		}
	}

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");
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
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_2_RSQSIM_4860;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2020_02_07-rundir4860-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_4860.instance();
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_2_RSQSIM_4860_10X;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2020_02_12-rundir4860_multi_combine-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_4860_10X.instance();
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_5_RSQSIM_4983;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2020_05_05-rundir4983_stitched-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_4983_STITCHED.instance();
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_5_RSQSIM_4983_SKIP65k;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2020_09_03-rundir4983_stitched-all-m6.5-skipYears65000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_4983_STITCHED.instance();
		
////		CyberShakeStudy study = CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_1_RSQSIM_4983_SKIP65k_1Hz_CVMH;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2020_09_03-rundir4983_stitched-all-m6.5-skipYears65000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_4983_STITCHED.instance();
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
//		File bbpDir = new File("/data/kevin/bbp/parallel/2022_03_15-rundir5413-all-m6.5-skipYears5000-noHF-vmLA_BASIN_500-cs500Sites");
//		RSQSimCatalog catalog = Catalogs.BRUCE_5413.instance();
//		
//		Vs30_Source vs30Source = Vs30_Source.Simulation;
////		CyberShakeStudy[] compStudies = { CyberShakeStudy.STUDY_15_4 };
//		CyberShakeStudy[] compStudies = {  };
		
		/*
		 * For regular studies
		 */
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		CyberShakeStudy[] compStudies = { };
		
		RSQSimCatalog catalog = null;
		File bbpDir = null;
		double catDurationYears = -1;
		
		boolean includeAleatoryStrip = true;
		
//		String[] siteNames = { "USC" };
//		String[] siteNames = { "SBSM" };
//		String[] siteNames = { "SBSM", "PAS" };
//		String[] siteNames = { "SBSM", "LAF", "s022", "STNI", "WNGC", "PDE" };
//		String[] siteNames = { "USC", "SMCA", "OSI", "WSS", "SBSM",
//				"LAF", "s022", "STNI", "WNGC", "PDE" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC", "s119", "s279", "s480" };
//		String[] siteNames = { "LAPD", "SBSM", "PAS", "WNGC" };
//		String[] siteNames = { "s119", "s279", "s480" };
//		String[] siteNames = { "LAPD" };
//		String[] siteNames = { "SMCA" };
//		String[] siteNames = { "PAS" };
//		String[] siteNames = { "GAVI", "LBP", "PAS" };
		String[] siteNames = { "LBP" };
		
		if (args.length > 0) {
			System.out.println("assuming command line arguments are site names");
			siteNames = args;
		}
		
		boolean sourceFractional = true;
		
		boolean replotCurves = true;
		boolean replotDisaggs = true;
		
//		AttenRelRef[] gmpeRefs = { AttenRelRef.NGAWest_2014_AVG_NOIDRISS, AttenRelRef.ASK_2014 };
//		AttenRelRef[] gmpeRefs = { AttenRelRef.NGAWest_2014_AVG_NOIDRISS };
		AttenRelRef[] gmpeRefs = { AttenRelRef.ASK_2014 };
//		IMT[] imts = IMT.forPeriods(new double[] { 3, 5, 7.5, 10 });
//		IMT[] imts = { IMT.PGV, IMT.SA2P0, IMT.SA3P0, IMT.SA5P0, IMT.SA10P0 };
//		IMT[] csvIMTs = { IMT.SA3P0 };
		IMT[] imts = { IMT.SA0P1, IMT.SA0P2, IMT.SA0P5, IMT.SA1P0, IMT.SA2P0, IMT.SA3P0, IMT.SA5P0, IMT.SA10P0 };
		IMT[] csvIMTs = null;
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		try {
			CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, study.getERF());
			String studyName = "CyberShake";
			if (catalog == null)
				studyName += study.getName();
			SimulationRotDProvider<CSRupture> mainProv = new StudyRotDProvider(study, amps2db, imts, studyName);
			if (study == CyberShakeStudy.STUDY_20_5_RSQSIM_4983_SKIP65k) {
				RSQSimSectBundledERF erf = (RSQSimSectBundledERF)study.getERF();
				mainProv = new RSQSimSubsetStudyRotDProvider(mainProv, erf, catalog, 65000d);
			}
			
			for (String siteName : siteNames) {
				List<CybershakeRun> matchingRuns = study.runFetcher().forSiteNames(siteName).fetch();
				Preconditions.checkState(!matchingRuns.isEmpty(), "Must have at least 1 run for the given site/study");
				Site site = CyberShakeSiteBuilder.buildSites(study, vs30Source, matchingRuns).get(0);
				
				List<SimulationRotDProvider<?>> compSimProvs = new ArrayList<>();
				
				if (includeAleatoryStrip && study.getERF() instanceof MeanUCERF2)
					compSimProvs.add(new StudyModifiedProbRotDProvider(
							(StudyRotDProvider)mainProv, new UCERF2_AleatoryMagVarRemovalMod(study.getERF()), study.getName()+" w/o Aleatory Mag"));
				
				Table<String, CSRupture, Double> sourceContribFracts =
						getSourceContribFracts(study.getERF(), mainProv.getRupturesForSite(site), catalog, sourceFractional);
				
				for (CyberShakeStudy compStudy : compStudies) {
					// strip the run out of the site so we don't get bad associations
					if (site instanceof CyberShakeSiteRun) {
						Site oSite = new Site(site.getLocation(), site.getName());
						oSite.addParameterList(oSite);
						site = oSite;
					}
					CachedPeakAmplitudesFromDB compAmps2db = new CachedPeakAmplitudesFromDB(compStudy.getDB(), ampsCacheDir, compStudy.getERF());
					StudyRotDProvider compProv = new StudyRotDProvider(compStudy, compAmps2db, imts, compStudy.getName());
					compSimProvs.add(compProv);
					if (includeAleatoryStrip && compStudy.getERF() instanceof MeanUCERF2)
						compSimProvs.add(new StudyModifiedProbRotDProvider(
								compProv, new UCERF2_AleatoryMagVarRemovalMod(compStudy.getERF()), compStudy.getName()+" w/o Aleatory Mag"));
				}
				
				LightweightBBP_CatalogSimZipLoader bbpCompProv = null;
				if (bbpDir != null) {
					try {
						bbpCompProv = loadBBP(bbpDir, site, catalog);
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
					
					List<CSRuptureComparison> comps = calcComps(mainProv, site, gmpeRef, imts);
					
					List<String> headerLines = new ArrayList<>();
					headerLines.add("# "+study.getName()+" "+siteName+" Hazard Curves");
					headerLines.add("");
					headerLines.add("**GMPE: "+gmpeRef.getName()+", Vs30 Source: "+vs30Source+"**");
					headerLines.add("");
					headerLines.add("**Study Details**");
					headerLines.add("");
					headerLines.addAll(study.getMarkdownMetadataTable());
					headerLines.add("");
					
					pageGen.generateSitePage(site, comps, outputDir, headerLines, imts, gmpeRef);
					
					File resourcesDir = new File(outputDir, "resources");
					Preconditions.checkState(resourcesDir.exists());
					File csvFile = new File(resourcesDir, site.getName()+"_rd50s.csv.gz");
					if (!csvFile.exists() && csvIMTs != null && csvIMTs.length > 0) {
						System.out.println("Writing CSV: "+csvFile.getAbsolutePath());
						pageGen.writeGroundMotionCSV(csvFile, site, gmpeRef, comps, csvIMTs);
					}
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
