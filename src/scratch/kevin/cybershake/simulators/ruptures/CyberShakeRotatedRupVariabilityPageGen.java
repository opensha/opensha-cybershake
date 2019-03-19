package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.Site;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.imr.attenRelImpl.ngaw2.NGAW2_WrapperFullParam;
import org.opensha.sha.imr.attenRelImpl.ngaw2.NGAW2_Wrappers;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;

import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.ruptures.RotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.RotatedRupVariabilityConfig.RotationSpec;
import scratch.kevin.simulators.ruptures.RotatedRupVariabilityPageGen;
import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;

public class CyberShakeRotatedRupVariabilityPageGen extends RotatedRupVariabilityPageGen {

	private Scenario scenario;

	public CyberShakeRotatedRupVariabilityPageGen(RSQSimCatalog catalog, Scenario scenario,
			RotatedRupVariabilityConfig config, SimulationRotDProvider<RotationSpec> prov, double[] calcPeriods) {
		super(catalog, config, scenario.getMagnitude(), prov, calcPeriods);
		this.scenario = scenario;
	}

	@Override
	protected String getScenarioName() {
		return scenario.getName();
	}

	@Override
	protected String getScenarioShortName() {
		return scenario.getShortName();
	}

	@Override
	protected String[] getScenarioMatchCriteria() {
		return scenario.getMatchCriteria();
	}

	@Override
	protected Scenario getBBP_PartB_Scenario(RotatedRupVariabilityConfig config) {
		return scenario;
	}
	
	public static void main(String[] args) {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_19_2_RSQSIM_ROT_2740;
		
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		
		String[] siteNames = { "USC" };
		
		double[] calcPeriods = { 3, 4, 5, 7.5, 10 };
		double[] periods = { 3, 5, 10 };
		
		try {
			List<CybershakeRun> matchingRuns = study.runFetcher().forSiteNames(siteNames).fetch();
			Preconditions.checkState(matchingRuns.size() == siteNames.length, "Expected %s runs for %s sites",
					matchingRuns.size(), siteNames.length);
			List<Site> sites = CyberShakeSiteBuilder.buildSites(study, vs30Source, matchingRuns);
			
			Preconditions.checkState(study.getERF() instanceof RSQSimRotatedRuptureFakeERF);
			RSQSimRotatedRuptureFakeERF erf = (RSQSimRotatedRuptureFakeERF)study.getERF();
			erf.setLoadRuptures(false);
			
			CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, erf);
			CSRotatedRupSimProv simProv = new CSRotatedRupSimProv(study, amps2db, calcPeriods);
			RSQSimCatalog catalog = erf.getCatalog();
			
			Map<Scenario, RotatedRupVariabilityConfig> configMap = erf.getConfigMap();
			
			NGAW2_WrapperFullParam[] refGMPEs = { new NGAW2_Wrappers.ASK_2014_Wrapper(), new NGAW2_Wrappers.BSSA_2014_Wrapper(),
					new NGAW2_Wrappers.CB_2014_Wrapper(), new NGAW2_Wrappers.CY_2014_Wrapper()};

			Map<Scenario, CyberShakeRotatedRupVariabilityPageGen> pageGensMap = new HashMap<>();
			HashSet<Integer> eventIDsSet = new HashSet<>();
			for (Scenario scenario : configMap.keySet()) {
				RotatedRupVariabilityConfig config = configMap.get(scenario);
				System.out.println("Config has "+config.getRotations().size());
				config = config.forSites(sites);
				System.out.println("Trimmed down to "+config.getRotations().size()+" rotations for "+sites.size()+" sites");
				
				CyberShakeRotatedRupVariabilityPageGen pageGen = new CyberShakeRotatedRupVariabilityPageGen(
						catalog, scenario, config, simProv, calcPeriods);
				
				pageGen.setGMPEs(refGMPEs);
				
				eventIDsSet.addAll(pageGen.getAllEventIDs());
				
				pageGensMap.put(scenario, pageGen);
			}
			
			Map<Integer, RSQSimEvent> eventsMap = loadEvents(catalog, eventIDsSet);
			
			File studyDir = new File(mainOutputDir, study.getDirName());
			Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
			
			for (Scenario scenario : pageGensMap.keySet()) {
				System.out.println("Doing scenario: "+scenario);
				
				CyberShakeRotatedRupVariabilityPageGen pageGen = pageGensMap.get(scenario);
				
				pageGen.setEventsMap(eventsMap);
				
				File rotDir = new File(studyDir, "rotated_ruptures_"+scenario.getPrefix());
				Preconditions.checkState(rotDir.exists() || rotDir.mkdir());
				
				List<String> methodSpecificLines = new ArrayList<>();
				
				methodSpecificLines.add("**Study Details**");
				methodSpecificLines.add("");
				methodSpecificLines.addAll(study.getMarkdownMetadataTable());
				
				pageGen.generatePage(rotDir, periods, methodSpecificLines);
			}
			
			System.out.println("Done, writing summary");
			study.writeMarkdownSummary(studyDir);
			System.out.println("Writing studies index");
			CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		study.getDB().destroy();
		System.exit(0);
	}

}
