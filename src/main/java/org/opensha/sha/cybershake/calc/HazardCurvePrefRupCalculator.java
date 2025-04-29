package org.opensha.sha.cybershake.calc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.data.CSVReader;
import org.opensha.commons.data.CSVReader.Row;

import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDBAPI;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Command Line Tool handles user-provided CSV of rupture variations for
 * HazardCurve calculations with preferred rupture directivity.
 */
public class HazardCurvePrefRupCalculator implements RuptureVariationProbabilityModifier {


	// Maps a <Source_ID + Rupture_ID> composite key to a Set of <Rup_Var_ID + Probability>
	// Rupture variations are not globally unique, but are unique to a given source and rupture.
	// Each rupture variation from the CSV has a provided probability.
	//
	// Not all variations for a rupture may be specified, in which case they are dynamically
	// calculated inside getVariationProbs. This means there will be realtime validation
	// beyond the validation of CSV variationProbs.
	private Map<ImmutablePair<Integer, Integer>, Set<ImmutablePair<Integer, Double>>> variationProbs;

	private PeakAmplitudesFromDBAPI peakAmplitudes;
	
	private static final boolean D = true;

	/**
	 * Constructor parses command line parameters from main method and initializes
	 * local parameters for use in HazardCurve computation.
	 * @param cmd - Arguments received from user input at CommandLine
	 * @param peakAmplitudes - Needed to get earthquake ruptures from database
	 * @throws MissingOptionException 
	 */
	public HazardCurvePrefRupCalculator(CommandLine cmd, PeakAmplitudesFromDBAPI peakAmplitudes)
			throws MissingOptionException {
		
		this.peakAmplitudes = peakAmplitudes;
		
		// One of run-id, site-name, or site-id is required.
		if (!(cmd.hasOption("run-id")
				|| cmd.hasOption("site-name")
				|| cmd.hasOption("site-id"))) {
			throw new MissingOptionException(
					"One of run-id, site-name, or site-id is required.");
		}
		
		if (!cmd.hasOption("rv-probs-csv")) {
			throw new MissingOptionException("Missing option rv-probs-csv (Rupture Variations CSV Input File)");
		}
		// TODO: Finish parsing the rest of the parameters

		// TODO: Work on input probabilties validation
		// TODO: Update the rupVarProbs Map to utilize residual probabilities

		readRupVarCSV(new File(cmd.getOptionValue("rv-probs-csv")));
		// TODO: Verify if the supplied probabilities are viable
		//		* Do the sum of variation probabilties provided exceed the rupture probability? (read from DB)
		// TODO: Calculate residual probabilities for non-specified rupVars
		//		* (1 - probability-sum-from-csv) / (count-vars-from-db - variations-in-mapping-for-this-rupture)
		// TODO: Move the peakAmplitudes reading into dedicated method for
		//       populating the rupVarProbs mapping.
	}

	/**
	 * Gets the probabilities of the rupture variations for the supplied rupture ID.
	 * This takes into account the CSV file with overriden variation probabilities.
	 * 
	 * HazardCurve calc gets originalProb from DB.
	 * Provided probabilities are validated to ensure they can sum to the originalProb 
	 * with the remainder divided over the residual unspecified variations.
	 * (Unless all variations were specified, in which case they must simply sum to the originalProb)
	 * Otherwise we return a Runtime Exception.
	 *
	 */
	@Override
	public List<Double> getVariationProbs(int sourceID, int rupID, double originalProb, CybershakeRun run,
			CybershakeIM im) {
		// TODO Copy over from RupVarProbModifierTest but override rupVar probs
		try {
			System.out.println(peakAmplitudes.getIM_Values(run.getRunID(), sourceID, rupID, im));
		} catch (SQLException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
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
	 * Populates maps by reading CSV file
	 * CSV Headers: Source_ID,Rupture_ID,Rup_Var_ID,Probability
	 * @param csvFile
	 */
	private void readRupVarCSV(File csvFile) {
		variationProbs = new HashMap<>();
		try (CSVReader reader = new CSVReader(new FileInputStream(csvFile))) {
			// Validate headers
			Row headers = reader.read();
			Preconditions.checkArgument(headers.columns()==4, "Must have two columns in CSV map");
			Preconditions.checkArgument(headers.get(0).equals("Source_ID"),
					"First column header must be named `Source_ID`");
			Preconditions.checkArgument(headers.get(1).equals("Rupture_ID"),
					"Second column header must be named `Rupture_ID`");
			Preconditions.checkArgument(headers.get(2).equals("Rup_Var_ID"),
					"Third column header must be named `Rup_Var_ID`");
			Preconditions.checkArgument(headers.get(3).equals("Probability"),
					"Fourth column header must be named `Probability`");
			// Read data line by line into map
			for (Row row : reader) {
				Preconditions.checkArgument(row.columns()==4, "Must have four columns in CSV map");
				ImmutablePair<Integer, Integer> compositeKey =
						ImmutablePair.of(row.getInt(0), row.getInt(1)); 
				ImmutablePair<Integer, Double> variationProb =
						ImmutablePair.of(row.getInt(2), row.getDouble(3));

				if (!variationProbs.containsKey(compositeKey)) {
					variationProbs.put(compositeKey,
							new HashSet<ImmutablePair<Integer, Double>>());
				}
				variationProbs.get(compositeKey).add(variationProb);
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
		if (D) {
			for (var key : variationProbs.keySet()) {
				System.out.println(key + " has " + variationProbs.get(key).size() + " variations");
			}
		}
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
			
			if (args.length == 1 && (args[0].endsWith("-help") || args[0].endsWith("-?"))) {
				System.out.println("usage:\n"
						+ "  --study <study enum name> --period <periods, comma separated>\n"
						+ "  [--run-id <run id> OR --site-name <site short name> OR --site-id <site id>]\n"
						+ "  --rv-probs-csv <path to CSV input file> --output-dir <output directory>\n");
				System.out.println("One of the run-id, site-name, or site-id must be provided!\n");
				HazardCurvePlotter.printHelp(options, appName);
			}
			
			try {
				CommandLine cmd = parser.parse(options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					HazardCurvePlotter.printHelp(options, appName);
				}


				DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
				PeakAmplitudesFromDBAPI peakAmplitudes = new PeakAmplitudesFromDB(db);

				// This calculator implements the RuptureVariationProbabilityModifier
				// interface and acts as both the CLT and modification logic to the 
				// HazardCurveComputation.
				RuptureVariationProbabilityModifier rupVarModifier =
						new HazardCurvePrefRupCalculator(cmd, peakAmplitudes);

				HazardCurveComputation hazardCurveCalc = new HazardCurveComputation(db);
				// Update the probability of each individual rupture variation
				hazardCurveCalc.setRupVarProbModifier(rupVarModifier);
				
				// TODO: Invoke hazardCurveCalc computeHazardCurve

			} catch (MissingOptionException e) {
				System.err.println(e.getMessage());
				HazardCurvePlotter.printUsage(options, appName);
			}
			
			if (D) System.out.println("HazardCurvePrefRupCalculator Done!");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
