package scratch.kevin.cybershake.simulators.ruptures;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;

import com.google.common.base.Preconditions;

import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityPageGen.VariabilityType;

public class CSRotRupPhiTauCompPageGen {

	public static void main(String[] args) throws IOException {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		CyberShakeStudy study = CyberShakeStudy.STUDY_19_3_RSQSIM_ROT_2585;
		String[] siteNames = { "USC", "PAS", "SBSM", "WNGC", "STNI", "SMCA" };
		Float[] dists = { 20f, 50f, 100f };
		Scenario[] scenarios = { Scenario.M6p6_VERT_SS_SURFACE, Scenario.M6p6_REVERSE,
				Scenario.M7p2_VERT_SS_SURFACE };
		
		File regressDir = new File("/home/kevin/CyberShake/cs_rot_rup_dists");
		Map<Scenario, File> regressCSVs = new HashMap<>();
		regressCSVs.put(Scenario.M6p6_VERT_SS_SURFACE, new File(regressDir, "m6p6_vert_ss_surface_phi_tau.csv"));
		regressCSVs.put(Scenario.M6p6_REVERSE, new File(regressDir, "m6p6_reverse_phi_tau.csv"));
		regressCSVs.put(Scenario.M7p2_VERT_SS_SURFACE, new File(regressDir, "m7p2_vert_ss_surface_phi_tau.csv"));
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		
		File outputDir = new File(studyDir, "rotation_phi_tau_comparison");
		Preconditions.checkArgument(outputDir.exists() || outputDir.mkdir());
		
		File resourcesDir = new File(outputDir, "resources");
		Preconditions.checkArgument(resourcesDir.exists() || resourcesDir.mkdir());
		
		List<String> lines = new ArrayList<>();
		
		lines.add("# Rotation Phi and Tau comparisons");
		lines.add("");
		
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		for (Scenario scenario : scenarios) {
			lines.add("## "+scenario.getName());
			lines.add(topLink); lines.add("");
			
			File csvFile = regressCSVs.get(scenario);
//			CSVFile<String> csv = CSVFile.readFile(csvFile, true);
			
			File scenDir = new File(studyDir, "rotated_ruptures_"+scenario.getPrefix());
			File scenResources = new File(scenDir, "resources");
			
			for (boolean phi : new boolean[] { true, false }) {
				TableBuilder table = MarkdownUtils.tableBuilder();
				
				if (phi) {
					lines.add("### Within-Event");
					lines.add(topLink); lines.add("");
				} else {
					lines.add("### Between-Event");
					lines.add(topLink); lines.add("");
				}
				
				table.initNewLine();
				table.addColumn("Site");
				for (Float dist : dists)
					table.addColumn(dist+" km");
				table.finalizeLine();
				
				for (String site : siteNames) {
					table.initNewLine();
					table.addColumn("**"+site+"**");
					for (Float dist : dists) {
						String prefix = phi ? "within_event_ss_" : "between_events_";
						prefix += "m"+optionalDigitDF.format(scenario.getMagnitude())
							+"_"+optionalDigitDF.format(dist)+"km_"+site+"_downsampled_hist.png";
						table.addColumn("![plot](../"+scenDir.getName()+"/resources/"+prefix+")");
					}
					table.finalizeLine();
					
//					table.addLine(vals)
//					String regressPrefix = scenario.getPrefix()+"_"+site+"_mixed_effects"
				}
				lines.addAll(table.build());
				lines.add("");
			}
		}
		
		// add TOC
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
		lines.add(tocIndex, "## Table Of Contents");

		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
	}
	
	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.##");
	
	private static void writeHist(File resourcesDir, String prefix, String siteName, CSVFile<String> csv, boolean phi) {
		
	}

}
