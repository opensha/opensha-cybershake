package scratch.kevin.cybershake.simCompare;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.NamedComparator;
import org.opensha.commons.data.Site;
import org.opensha.refFaultParamDb.vo.FaultSectionPrefData;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.imr.AttenRelRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.CSRuptureComparison;
import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.Vs30_Source;
import scratch.kevin.simCompare.RuptureComparison;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simCompare.SourceSiteDistPageGen;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;

public class StudySourceSiteDistPageGen extends SourceSiteDistPageGen<CSRupture> {

	public StudySourceSiteDistPageGen(SimulationRotDProvider<CSRupture> simProv, List<Site> sites) {
		super(simProv, sites);
	}
	
	static StudyRotDProvider getSimProv(CyberShakeStudy study, String[] siteNames, File ampsCacheDir, double[] periods,
			CybershakeIM[] rd50_ims, Vs30_Source vs30Source, CSRupture[][] csRups) throws SQLException, IOException {
		DBAccess db = study.getDB();
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		Runs2DB runs2db = new Runs2DB(db);
		
		Map<Site, Integer> runIDsMap = new HashMap<>();
		
		AbstractERF erf = study.getERF();
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, ampsCacheDir, erf);
		
		Map<Site, List<CSRupture>> siteRupsMap = new HashMap<>();
		
		for (String siteName : siteNames) {
			CybershakeSite csSite = sites2db.getSiteFromDB(siteName);
			
			System.out.println("Finding Run_ID for study "+study);
			String sql = "SELECT C.Run_ID FROM Hazard_Curves C JOIN CyberShake_Runs R ON R.Run_ID=C.Run_ID\n" + 
					"WHERE R.Site_ID="+csSite.id+" AND C.Hazard_Dataset_ID="+study.getDatasetID()+" ORDER BY C.Curve_Date DESC LIMIT 1";
			System.out.println(sql);
			ResultSet rs = db.selectData(sql);
			Preconditions.checkState(rs.first());
			int runID = rs.getInt(1);
			
			System.out.println("Detected Run_ID="+runID);
			
			CybershakeRun run = runs2db.getRun(runID);
			
			System.out.println("Building site");
			Site site = StudyGMPE_Compare.buildSite(csSite, runID, study, vs30Source, db);
			
			runIDsMap.put(site, runID);
			List<CSRupture> siteRups = StudyGMPE_Compare.getSiteRuptures(site, amps2db, run.getERFID(), run.getRupVarScenID(),
					csRups, erf, runID, rd50_ims[0]);
			siteRupsMap.put(site, siteRups);
		}
		
		return new StudyRotDProvider(amps2db, runIDsMap, siteRupsMap, periods, rd50_ims, null, study.getName());
	}
	
	private static List<List<CSRupture>> getRupturesForSoruces(List<String> sourceNames, List<int[]> parentIDs, AbstractERF erf, CSRupture[][] csRups) {
		List<List<CSRupture>> ret = new ArrayList<>();
		if (erf instanceof RSQSimSectBundledERF) {
			RSQSimSectBundledERF rsERF = (RSQSimSectBundledERF)erf;
			Preconditions.checkState(parentIDs != null && parentIDs.size() == sourceNames.size());
			for (int i=0; i<sourceNames.size(); i++) {
				List<CSRupture> rupsForSource = new ArrayList<>();
				ret.add(rupsForSource);
				int[] sourceParents = parentIDs.get(i);
				for (CSRupture[] sourceRups : csRups) {
					if (sourceRups == null)
						continue;
					for (CSRupture rup : sourceRups) {
						if (rup == null)
							continue;
						for (FaultSectionPrefData sect : rsERF.getRupture(rup.getSourceID(), rup.getRupID()).getSortedSubSects()) {
							if (Ints.contains(sourceParents, sect.getParentSectionId())) {
								rupsForSource.add(rup);
								break;
							}
						}
					}
				}
				System.out.println("Found "+rupsForSource.size()+" ruptures for "+sourceNames.get(i));
				Preconditions.checkState(!rupsForSource.isEmpty(), "None found!");
			}
		} else {
//			Preconditions.checkst
			throw new IllegalStateException("not yet implemented for non RSQSim");
		}
		return ret;
	}

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		// RSQSim
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_PROTOTYPE_2457;
		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
		
		List<String> sourceNames = new ArrayList<>();
		List<int[]> parentIDs = new ArrayList<>();
		
		sourceNames.add("San Andreas (Mojave)");
		parentIDs.add(new int[] { 286, 301});
		
		sourceNames.add("Puente Hills");
		parentIDs.add(new int[] { 240});
		
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		
//		String[] siteNames = { "USC" };
		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC" };
		
		AttenRelRef[] gmpeRefs = { AttenRelRef.ASK_2014, AttenRelRef.BSSA_2014, AttenRelRef.CB_2014, AttenRelRef.CY_2014 };
		double[] periods = { 3, 5, 10 };
		CybershakeIM[] rd50_ims = new PeakAmplitudesFromDB(study.getDB()).getIMs(Doubles.asList(periods),
				IMType.SA, CyberShakeComponent.RotD50).toArray(new CybershakeIM[0]);
		
		CSRupture[][] csRups = new CSRupture[study.getERF().getNumSources()][];
		
		StudyRotDProvider simProv = getSimProv(study, siteNames, ampsCacheDir, periods, rd50_ims, vs30Source, csRups);
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		List<Site> sites = new ArrayList<>(simProv.getAvailableSites());
		sites.sort(new NamedComparator());
		
		Map<AttenRelRef, List<CSRuptureComparison>> gmpeComps = new HashMap<>();
		
		for (AttenRelRef gmpeRef : gmpeRefs) {
			System.out.println("Calculating for "+gmpeRef.getName());
			Map<CSRupture, CSRuptureComparison> compsMap = new HashMap<>();
			for (Site site : simProv.getAvailableSites()) {
				List<CSRuptureComparison> siteComps = StudySiteHazardCurvePageGen.calcComps(simProv, site, gmpeRef, periods);
				for (CSRuptureComparison comp : siteComps) {
					if (compsMap.containsKey(comp.getRupture())) {
						// combine
						CSRuptureComparison prevComp = compsMap.get(comp.getRupture());
						for (double period : comp.getPeriods(site))
							prevComp.addResult(site, period, comp.getLogMean(site, period), comp.getStdDev(site, period));
					} else {
						// new
						compsMap.put(comp.getRupture(), comp);
					}
				}
			}
			gmpeComps.put(gmpeRef, new ArrayList<>(compsMap.values()));
		}
		
		List<List<CSRupture>> rupsForSources = getRupturesForSoruces(sourceNames, parentIDs, study.getERF(), csRups);
		Table<AttenRelRef, String, List<RuptureComparison<CSRupture>>> sourceCompsTable = HashBasedTable.create();
		for (int i=0; i<sourceNames.size(); i++) {
			HashSet<CSRupture> rups = new HashSet<>(rupsForSources.get(i));
			for (AttenRelRef gmpeRef : gmpeComps.keySet()) {
				List<RuptureComparison<CSRupture>> sourceComps = new ArrayList<>();
				for (RuptureComparison<CSRupture> comp : gmpeComps.get(gmpeRef))
					if (rups.contains(comp.getRupture()))
						sourceComps.add(comp);
				sourceCompsTable.put(gmpeRef, sourceNames.get(i), sourceComps);
			}
		}
		
		File outputDir = new File(studyDir, "source_site_comparisons_Vs30"+vs30Source.name());
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		StudySourceSiteDistPageGen pageGen = new StudySourceSiteDistPageGen(simProv, sites);
		
		List<String> headerLines = new ArrayList<>();
		headerLines.add("# "+study.getName()+" Source/Site GMPE Comparisons");
		headerLines.add("");
		headerLines.add("**Vs30 Source: "+vs30Source+"**");
		headerLines.add("");
		headerLines.add("**GMPEs:**");
		for (AttenRelRef gmpe : gmpeRefs)
			headerLines.add("* "+gmpe.getName());
		
		pageGen.generatePage(sourceCompsTable, outputDir, headerLines, periods);
		
		study.writeMarkdownSummary(studyDir);
		CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		
		study.getDB().destroy();
		
		StudySiteHazardCurvePageGen.getExec().shutdown();
		
		System.exit(0);
	}

}
