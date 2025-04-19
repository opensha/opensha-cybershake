package org.opensha.sha.cybershake.calc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.data.CSVReader;
import org.opensha.commons.data.CSVReader.Row;

import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Command Line Tool handles user-provided CSV of rupture variations for
 * HazardCurve calculations with preferred rupture directivity.
 */
public class HazardCurvePrefRupCalculator implements RuptureVariationProbabilityModifier {

	File rvProbsCSV;
	Map<Integer, Double> ruptureVariationBiases;
	
	/**
	 * Constructor parses command line parameters from main method and initializes
	 * local parameters for use in HazardCurve computation.
	 * @throws MissingOptionException 
	 */
	public HazardCurvePrefRupCalculator(CommandLine cmd) throws MissingOptionException {
		
		// One of run-id, site-name, or site-id is required.
		if (!(cmd.hasOption("run-id")
				|| cmd.hasOption("site-name")
				|| cmd.hasOption("site-id"))) {
			throw new MissingOptionException(
					"One of run-id, site-name, or site-id is required.");
		}
		// TODO: Finish parsing the rest of the parameters and then set local vars.
	}

	@Override
	public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run,
			CybershakeIM im) {
		// TODO Copy over from RupVarProbModifierTest but override rupVar probs
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
	 * Reads rupture variation biases from a CSV file.
	 * This static method can be used independently of the CLT (See main).
	 * @param csvFile
	 * @return
	 */
	private static Map<Integer, Double> getRupVarBiases(File csvFile) {
		Map<Integer, Double> biasMap = new HashMap<>();
		try (CSVReader reader = new CSVReader(new FileInputStream(csvFile))) {
			// Validate headers
			Row headers = reader.read();
			Preconditions.checkArgument(headers.columns()==2, "Must have two columns in CSV map");
			Preconditions.checkArgument(headers.get(0).equals("Rup_Var_ID"),
					"First column header must be named `Rup_Var_ID`");
			Preconditions.checkArgument(headers.get(1).equals("Prob"),
					"Second column header must be named `Prob`");
			// Read data line by line into map
			for (Row row : reader) {
				Preconditions.checkArgument(row.columns()==2, "Must have two columns in CSV map");
				biasMap.put(row.getInt(0), row.getDouble(1));
			}
		} catch (FileNotFoundException e) {
			System.err.println("CSV File not found.");
			e.printStackTrace();
		} catch (NumberFormatException e) {
			System.err.println("First column must be of type `Integer` and second column must be of type `Double`");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Rupture variation biases CSV File encountered an IO exception.");
			e.printStackTrace();
		}
		return biasMap;
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
				HazardCurvePrefRupCalculator calc = new HazardCurvePrefRupCalculator(cmd);
				
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
		
		// Test `getRupVarBiases` works
//		System.out.println(
//				HazardCurvePrefRupCalculator.getRupVarBiases(
//						new File("/Users/bhatthal/git/opensha-cybershake-fork/rupVarBiases.csv")));
		
	}

}
