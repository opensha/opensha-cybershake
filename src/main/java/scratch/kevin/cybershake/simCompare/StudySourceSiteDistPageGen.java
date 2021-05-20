package scratch.kevin.cybershake.simCompare;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.NamedComparator;
import org.opensha.commons.data.Site;
import org.opensha.refFaultParamDb.vo.FaultSectionPrefData;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
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
import org.opensha.sha.faultSurface.FaultSection;
import org.opensha.sha.imr.AttenRelRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.CSRuptureComparison;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.RuptureComparison;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simCompare.SourceSiteDistPageGen;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;

public class StudySourceSiteDistPageGen extends SourceSiteDistPageGen<CSRupture> {

	public StudySourceSiteDistPageGen(SimulationRotDProvider<CSRupture> simProv, List<Site> sites) {
		super(simProv, sites);
	}
	
	private static List<List<CSRupture>> getRupturesForSoruces(List<String> sourceNames, List<int[]> parentIDs, AbstractERF erf,
			Collection<CSRupture> csRuptures) {
		List<List<CSRupture>> ret = new ArrayList<>();
		if (erf instanceof RSQSimSectBundledERF) {
			RSQSimSectBundledERF rsERF = (RSQSimSectBundledERF)erf;
			Preconditions.checkState(parentIDs != null && parentIDs.size() == sourceNames.size());
			for (int i=0; i<sourceNames.size(); i++) {
				List<CSRupture> rupsForSource = new ArrayList<>();
				ret.add(rupsForSource);
				int[] sourceParents = parentIDs.get(i);
				for (CSRupture rup : csRuptures) {
					for (FaultSection sect : rsERF.getRupture(rup.getSourceID(), rup.getRupID()).getSortedSubSects()) {
						if (Ints.contains(sourceParents, sect.getParentSectionId())) {
							rupsForSource.add(rup);
							break;
						}
					}
				}
				System.out.println("Found "+rupsForSource.size()+" ruptures for "+sourceNames.get(i));
				Preconditions.checkState(!rupsForSource.isEmpty(), "None found!");
			}
		} else {
			throw new IllegalStateException("currently only implemented for RSQSim");
		}
		return ret;
	}

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		
		// RSQSim
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_PROTOTYPE_2457;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_9_RSQSIM_2740;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_5_RSQSIM_4983;
		
		List<String> sourceNames = new ArrayList<>();
		List<int[]> parentIDs = new ArrayList<>();
		
		sourceNames.add("San Andreas (Mojave)");
		parentIDs.add(new int[] { 286, 301});
		
		sourceNames.add("Puente Hills");
		parentIDs.add(new int[] { 240});
		
		sourceNames.add("San Jacinto (San Beranardino)");
		parentIDs.add(new int[] { 119});
		
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		
		String[] siteNames = { "USC", "SBSM" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC" };
//		String[] siteNames = { "USC", "OSI", "PDE", "s022", "WNGC" };
		
		boolean hypoSort = true;
		
		AttenRelRef[] gmpeRefs = { AttenRelRef.ASK_2014, AttenRelRef.BSSA_2014, AttenRelRef.CB_2014, AttenRelRef.CY_2014 };
		double[] periods = { 3, 5, 10 };
		IMT[] imts = IMT.forPeriods(periods);
		
//		StudyRotDProvider simProv = getSimProv(study, siteNames, ampsCacheDir, periods, rd50_ims, vs30Source, csRups);
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, study.getERF());
		StudyRotDProvider simProv = new StudyRotDProvider(study, amps2db, imts, study.getName());
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		List<CybershakeRun> runs = study.runFetcher().forSiteNames(siteNames).fetch();
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study, vs30Source, runs);
		sites.sort(new NamedComparator());
		
		Map<AttenRelRef, List<CSRuptureComparison>> gmpeComps = new HashMap<>();
		
		for (AttenRelRef gmpeRef : gmpeRefs) {
			System.out.println("Calculating for "+gmpeRef.getName());
			Map<CSRupture, CSRuptureComparison> compsMap = new HashMap<>();
			for (Site site : sites) {
				List<CSRuptureComparison> siteComps = StudySiteHazardCurvePageGen.calcComps(simProv, site, gmpeRef, imts);
				for (CSRuptureComparison comp : siteComps) {
					if (compsMap.containsKey(comp.getRupture())) {
						// combine
						CSRuptureComparison prevComp = compsMap.get(comp.getRupture());
						for (IMT imt : comp.getIMTs(site))
							prevComp.addResult(site, imt, comp.getLogMean(site, imt), comp.getStdDev(site, imt));
					} else {
						// new
						compsMap.put(comp.getRupture(), comp);
					}
				}
			}
			gmpeComps.put(gmpeRef, new ArrayList<>(compsMap.values()));
		}
		
		HashSet<CSRupture> uniqueRups = new HashSet<>();
		for (Site site : sites)
			uniqueRups.addAll(simProv.getRupturesForSite(site));
		System.out.println("Found "+uniqueRups+" ruptures for all sites");
		List<List<CSRupture>> rupsForSources = getRupturesForSoruces(sourceNames, parentIDs, study.getERF(), uniqueRups);
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
		
		pageGen.generatePage(sourceCompsTable, outputDir, headerLines, imts, hypoSort);
		
		study.writeMarkdownSummary(studyDir);
		CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		
		study.getDB().destroy();
		
		StudySiteHazardCurvePageGen.getExec().shutdown();
		
		System.exit(0);
	}

}
