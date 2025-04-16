package org.opensha.sha.cybershake.calc;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.opensha.commons.util.ClassUtils;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;

import com.google.common.base.Joiner;

/**
 * This project will add support to multiply rupture direction probability by a
 * specified bias. We will modify Cybershake OpenSHA to support unequal
 * probabilities of specified rupture directivity. Users will provide a CSV file
 * with the probabilities to this newly created CLT.
 */
public class HazardCurvePrefRupCalculator implements RuptureVariationProbabilityModifier {

	/**
	 * Constructor
	 */
	public HazardCurvePrefRupCalculator() {
		// TODO: Set parameters to include CSV file
		// TODO: Parse CSV and set local vars	
	}

	@Override
	public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run,
			CybershakeIM im) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private static String getStudyList() {
		List<String> names = new ArrayList<>();
		for (CyberShakeStudy study : CyberShakeStudy.values())
			if (!study.name().toLowerCase().contains("rsqsim"))
				names.add(study.name());
		return Joiner.on(", ").join(names);
	}

	/**
	 * Usage:
	 *   --study <study enum name> --period <periods, comma separated>
	 *   [--run-id <run id> OR --site-name <site short name> OR --site-id <site id>]
	 *   --rv-probs-csv <path to CSV input file> --output-dir <output directory>
	 */
	private static Options createOptions() {
		Options ops = new Options();

		Option studyOp = new Option("st", "study", true, "CyberShake study. One of: "+getStudyList());
		studyOp.setRequired(true);
		ops.addOption(studyOp);
		
		Option periodsOp = new Option("p", "period", true, "Period(s) to plot, multiple can be comma separated. O for PGA, -1 for PGV");
		periodsOp.setRequired(true);
		ops.addOption(periodsOp);

		// One of run-id, site-name, or site-id is required.
		// This is only enforced after after CommandLine is parsed in main.
		Option runOp = new Option("r", "run-id", true, "Run ID");
		runOp.setRequired(false);
		ops.addOption(runOp);

		Option siteNameOp = new Option("sinm", "site-name", true, "Site Name");
		siteNameOp.setRequired(false);
		ops.addOption(siteNameOp);
		
		Option siteOp = new Option("si", "site-id", true, "Site ID");
		siteOp.setRequired(false);
		ops.addOption(siteOp);

		Option rvOp = new Option("rv", "rv-probs-csv", true, "Rupture Variation input CSV");
		rvOp.setRequired(true);
		ops.addOption(rvOp);

		Option outputDirOp = new Option("o", "output-dir", true, "Output directory");
		outputDirOp.setRequired(true);
		ops.addOption(outputDirOp);

		Option helpOp = new Option("?", "help", false, "Show this message");
		helpOp.setRequired(false);
		ops.addOption(helpOp);

		return ops;
	}

	/**
	 * Run the CLT for HazardCurvePrefRupCalculator.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Options options = createOptions();
			
			String appName = ClassUtils.getClassNameWithoutPackage(
					HazardCurvePrefRupCalculator.class);
			
			CommandLineParser parser = new DefaultParser();
			
			if (args.length == 0) {
				HazardCurvePlotter.printUsage(options, appName);
			}
			
			if (args.length == 1 && (args[0].endsWith("-help") || args[0].endsWith("-?")))
				HazardCurvePlotter.printHelp(options, appName);
			
			try {
				CommandLine cmd = parser.parse(options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					HazardCurvePlotter.printHelp(options, appName);
				}
				
				if (!(cmd.hasOption("run-id")
						|| cmd.hasOption("site-name")
						|| cmd.hasOption("site-id"))) {
					throw new MissingOptionException(
							"One of run-id, site-name, or site-id is required.");
				}
				
				// TODO: Load CSV input file
				// TODO: Create instance of rupture calc with input file
				// TODO: Create HazardCurveComputation instance to use rupture calc.

			} catch (MissingOptionException e) {
				System.err.println(e.getMessage());
				HazardCurvePlotter.printUsage(options, appName);
			}
			
			System.out.println("Done!");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
