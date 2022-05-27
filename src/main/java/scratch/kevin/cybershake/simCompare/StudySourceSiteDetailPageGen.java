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
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
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
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.simulators.RSQSimEvent;
import org.opensha.sha.simulators.utils.RSQSimUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.CSRuptureComparison;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.RuptureComparison;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simCompare.SourceSiteDetailPageGen;
import scratch.kevin.simCompare.SourceSiteDistPageGen;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Loader;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;

public class StudySourceSiteDetailPageGen extends SourceSiteDetailPageGen {
	
	public StudySourceSiteDetailPageGen(SimulationRotDProvider<RSQSimEvent> simProv, String sourceName,
			int[] parentSectIDs, RSQSimCatalog catalog, List<RSQSimEvent> events, List<Site> sites) throws IOException {
		super(simProv, sourceName, parentSectIDs, catalog, events, sites);
	}
	
	private static class RSQSimSimProvAdapter implements SimulationRotDProvider<RSQSimEvent> {
		
		private StudyRotDProvider prov;
		private Map<RSQSimEvent, CSRupture> csRupMap;
		private RSQSimSectBundledERF rsERF;
		private Map<Integer, RSQSimEvent> rsEventMap;

		public RSQSimSimProvAdapter(StudyRotDProvider prov, RSQSimSectBundledERF rsERF,
				Map<Integer, RSQSimEvent> rsEventMap) {
			this.prov = prov;
			this.rsERF = rsERF;
			this.rsEventMap = rsEventMap;
			csRupMap = new HashMap<>();
		}
		
		private synchronized CSRupture getCSRup(RSQSimEvent event, Site site) {
			CSRupture ret = csRupMap.get(event);
			if (ret == null && site != null) {
				for (CSRupture rup : prov.getRupturesForSite(site)) {
					int eventID = rsERF.getRupture(rup.getSourceID(), rup.getRupID()).getEventID();
					RSQSimEvent event2 = rsEventMap.get(eventID);
					csRupMap.put(event2, rup);
				}
			}
			return csRupMap.get(event);
		}
		
		private RSQSimEvent getRSEvent(CSRupture csRup) {
			return rsEventMap.get(rsERF.getRupture(csRup.getSourceID(), csRup.getRupID()).getEventID());
		}

		@Override
		public String getName() {
			return prov.getName();
		}

		@Override
		public DiscretizedFunc getRotD50(Site site, RSQSimEvent rupture, int index) throws IOException {
			return prov.getRotD50(site, getCSRup(rupture, site), index);
		}

		@Override
		public DiscretizedFunc getRotD100(Site site, RSQSimEvent rupture, int index) throws IOException {
			return prov.getRotD100(site, getCSRup(rupture, site), index);
		}

		@Override
		public DiscretizedFunc[] getRotD(Site site, RSQSimEvent rupture, int index) throws IOException {
			return prov.getRotD(site, getCSRup(rupture, site), index);
		}

		@Override
		public DiscretizedFunc getRotDRatio(Site site, RSQSimEvent rupture, int index) throws IOException {
			return prov.getRotDRatio(site, getCSRup(rupture, site), index);
		}

		@Override
		public double getPGV(Site site, RSQSimEvent rupture, int index) throws IOException {
			return prov.getPGV(site, getCSRup(rupture, site), index);
		}

		@Override
		public double getDuration(Site site, RSQSimEvent rupture, DurationTimeInterval interval, int index)
				throws IOException {
			return prov.getDuration(site, getCSRup(rupture, site), interval, index);
		}

		@Override
		public int getNumSimulations(Site site, RSQSimEvent rupture) {
			return prov.getNumSimulations(site, getCSRup(rupture, site));
		}

		@Override
		public Location getHypocenter(RSQSimEvent rupture, int index) {
			return RSQSimUtils.getHypocenter(rupture);
		}

		@Override
		public Collection<RSQSimEvent> getRupturesForSite(Site site) {
			List<RSQSimEvent> rups = new ArrayList<>();
			for (CSRupture rup : prov.getRupturesForSite(site))
				rups.add(getRSEvent(rup));
			return rups;
		}

		@Override
		public boolean hasRotD50() {
			return prov.hasRotD50();
		}

		@Override
		public boolean hasRotD100() {
			return prov.hasRotD100();
		}

		@Override
		public boolean hasPGV() {
			return prov.hasPGV();
		}

		@Override
		public boolean hasDurations() {
			return prov.hasDurations();
		}

		@Override
		public double getAnnualRate(RSQSimEvent rupture) {
			return prov.getAnnualRate(getCSRup(rupture, null));
		}

		@Override
		public double getMinimumCurvePlotRate(Site site) {
			return prov.getMinimumCurvePlotRate(site);
		}

		@Override
		public double getMagnitude(RSQSimEvent rupture) {
			return rupture.getMagnitude();
		}
		
	}

	public static void main(String[] args) throws SQLException, IOException {
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_PROTOTYPE_2457;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_2585;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_9_RSQSIM_2740;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_20_5_RSQSIM_4983;
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
		RSQSimCatalog catalog = study.getRSQSimCatalog();
		
//		String sourceName = "San Jacinto (Northern)";
//		String sourcePrefix = "nsjc";
//		int[] parentIDs = { 119, 289, 401, 293, 292 };
		
//		String sourceName = "San Jacinto";
//		String sourcePrefix = "sjc";
//		int[] parentIDs = { 119, 289, 401, 293, 292, 101, 99, 28 };
		
		String sourceName = "San Andreas (Mojave)";
		String sourcePrefix = "saf_mojave";
		int[] parentIDs = { 286, 301 };
		
//		String[] siteNames = { "SBSM" };
		String[] siteNames = { "SBSM", "USC" };
//		String[] siteNames = { "USC", "STNI", "LAPD", "SBSM", "PAS", "WNGC" };
//		String[] siteNames = { "USC", "OSI", "PDE", "s022", "WNGC" };
		
		double minMag = 6d;
		int skipYears = 5000;
		
		IMT[] imts = { IMT.SA3P0, IMT.SA5P0, IMT.SA10P0 };
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, study.getERF());
		StudyRotDProvider simProv = new StudyRotDProvider(study, amps2db, imts, study.getName());
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		List<CybershakeRun> runs = study.runFetcher().forSiteNames(siteNames).fetch();
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		sites.sort(new NamedComparator());
		
		File outputDir = new File(studyDir, "source_site_details_"+sourcePrefix);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		Loader loader = catalog.loader();
		if (minMag > 0d)
			loader.minMag(minMag);
		if (skipYears > 0)
			loader.skipYears(skipYears);
		HashSet<Integer> allParents = new HashSet<>();
		for (int parent : parentIDs)
			allParents.add(parent);
		loader.forParentSections(true, Ints.toArray(allParents));
		System.out.println("Loading events...");
		List<RSQSimEvent> events = loader.minMag(minMag).load();
		System.out.println("Loaded "+events.size()+" events");
		Map<Integer, RSQSimEvent> eventsMap = new HashMap<>();
		for (RSQSimEvent event : events)
			eventsMap.put(event.getID(), event);
		
		RSQSimSectBundledERF rsERF = (RSQSimSectBundledERF)study.getERF();
		RSQSimSimProvAdapter adapter = new RSQSimSimProvAdapter(simProv, rsERF, eventsMap);
		
		StudySourceSiteDetailPageGen pageGen = new StudySourceSiteDetailPageGen(
				adapter, sourceName, parentIDs, catalog, events, sites);
		
		pageGen.generatePage(outputDir, null, imts);
		
		study.writeMarkdownSummary(studyDir);
		CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		
		study.getDB().destroy();
		
		StudySiteHazardCurvePageGen.getExec().shutdown();
		
		System.exit(0);
	}

}
