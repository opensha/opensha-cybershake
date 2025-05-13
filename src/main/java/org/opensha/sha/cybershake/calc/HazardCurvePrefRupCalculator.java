package org.opensha.sha.cybershake.calc;

import java.awt.geom.Point2D;
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
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.data.CSVReader;
import org.opensha.commons.data.CSVReader.Row;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDBAPI;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DBAPI;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.gui.infoTools.IMT_Info;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Command Line Tool handles user-provided CSV of rupture variations for
 * HazardCurve calculations with preferred rupture directivity.
 */
public class HazardCurvePrefRupCalculator implements RuptureVariationProbabilityModifier {


	// Maps a <Source_ID + Rupture_ID> composite key to a Map of Rup_Var_ID to Probability
	// Rupture variations are not globally unique, but are unique to a given source and rupture.
	// Each rupture variation from the CSV has a provided probability.
	//
	// Not all variations for a rupture may be specified, in which case they are dynamically
	// calculated inside getVariationProbs. This means there will be realtime validation
	// beyond the validation of CSV variationProbs.
	private Map<ImmutablePair<Integer, Integer>, Map<Integer, Double>> variationProbs;

	private PeakAmplitudesFromDBAPI peakAmplitudes;
	private DBAccess db;
	private CyberShakeStudy study;
	private CybershakeRun run;
	private CybershakeSite site;
	private double[] periods; // TODO: Make a plotter that uses this
	
	private static final boolean D = true;

	/**
	 * Constructor parses command line parameters from main method and initializes
	 * local parameters for use in HazardCurve computation.
	 * @param cmd - Arguments received from user input at CommandLine
	 * @throws MissingOptionException 
	 */
	public HazardCurvePrefRupCalculator(CommandLine cmd) throws MissingOptionException, ParseException {
		
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
		
		// The study is used to determine which database to use
		if (!cmd.hasOption("study")) {
			// TODO: Should we throw a runtime exception, or is this acceptable?
			System.err.println("Missing option study. Using default DBAccess for CyberShake Production.");
			db = Cybershake_OpenSHA_DBApplication.getDB();
			peakAmplitudes = new PeakAmplitudesFromDB(db);
		} else {
			study = CyberShakeStudy.valueOf(cmd.getOptionValue("study"));
			db = study.getDB();
			peakAmplitudes = new PeakAmplitudesFromDB(study.getDB());
		}

		if (!cmd.hasOption("period")) {
			throw new MissingOptionException("Missing option period(s).");
		}
		String periodStr = cmd.getOptionValue("period");
		if (periodStr.contains(",")) {
			String[] periodSplit = periodStr.split(",");
			periods = new double[periodSplit.length];
			for (int p=0; p<periods.length; p++)
				periods[p] = Double.parseDouble(periodSplit[p]);
		} else {
			periods = new double[] { Double.parseDouble(periodStr) };
		}
		
		// TODO: Finish parsing the rest of the parameters (ie output dir)
		
		//  [--run-id <run id> OR --site-name <site short name> OR --site-id <site id>]
	
		// Invocation requires xVals from IMT.
		
		// TODO: Is this correct? 
		// Should we invoke `computeHazardCurve` with a unique set of xVals for each period supplied?

		// TODO: Get xVals for each period to plot
		
		List<List<Double>> xSeries = new ArrayList<>(); // xValues for each plotting
		IMT_Info hazardCurve = new IMT_Info();
		for (double period : periods) {
			List<Double> xVals = new ArrayList<Double>();
			ArbitrarilyDiscretizedFunc func;
			if (period == 0) {
				func = hazardCurve.getDefaultHazardCurve("PGA");
			} else if (period == -1) {
				func = hazardCurve.getDefaultHazardCurve("PGV");
			} else {
				throw new ParseException("Invalid period: `" + period + "`");
			}
			for (Point2D pt : func) {
				xVals.add(pt.getX());
			}
			xSeries.add(xVals);
			
		}
//		ArrayList<Double> xVals = Lists.newArrayList();
//		for (Point2D pt : IMT_Info.getUSGS_SA_Function())
//			xVals.add(pt.getX());

		
		// TODO: Get runDB and siteDB for computeHazardCurve invocation

		Runs2DB runsDB = new Runs2DB(db);
		SiteInfo2DBAPI siteDB = new SiteInfo2DB(db);
		if (cmd.hasOption("run-id")) {
			int run = Integer.parseInt(cmd.getOptionValue("run-id"));
		}
		//		CybershakeIM im = new HazardCurve2DB(db).getIMFromID(21);
		else if (cmd.hasOption("site-id")) {
			site = siteDB.getSiteFromDB(Integer.parseInt(cmd.getOptionValue("site-id")));
		} else if (cmd.hasOption("site-name")) {
			site = siteDB.getSiteFromDB(cmd.getOptionValue("site-name"));
		}
		// read output directory

		readRupVarCSV(new File(cmd.getOptionValue("rv-probs-csv")));


		HazardCurveComputation hazardCurveCalc = new HazardCurveComputation(db);
		// Update the probability of each individual rupture variation
		hazardCurveCalc.setRupVarProbModifier(this);
		
		
		// TODO: Invoke hazardCurveCalc computeHazardCurve for each period

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
		// Get rupture variations from CSV for this source rupture.
		Map<Integer, Double> rupVarBiases =
				variationProbs.getOrDefault(ImmutablePair.of(sourceID, rupID), null);
		// get the number of amps from DB (may be greater than in CSV)
		int numAmps;
		try {
			numAmps = peakAmplitudes.getIM_Values(run.getRunID(), sourceID, rupID, im).size();
		} catch (SQLException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}

		// Validation of provided rupture variation biases for this source+rupture
		double rupVarBiasesProbSum = 0;
		double rupVarBiasesCount = 0;
		if (rupVarBiases != null) {
			rupVarBiasesCount = rupVarBiases.size();
			// All variations are recorded on database, so we must have less in CSV
			Preconditions.checkState(rupVarBiasesCount <= numAmps);
			// The sum of probabilities of our custom rupture variation biases
			// must be less than or equal to the original probability for the rupture
			rupVarBiasesProbSum = rupVarBiases.values()
					.stream()
					.mapToDouble(Double::doubleValue)
					.sum();
			final double TOLERANCE = 1E-7;
			Preconditions.checkState(rupVarBiasesProbSum <= originalProb + TOLERANCE);
		}

//		double bundleFactor = 0.01d;
//		int ampsPerBundle = (int)(bundleFactor * (double)numAmps + 0.5);
//		if (ampsPerBundle < 1)
//			ampsPerBundle = 1;
		
		// double probPerRV = originalProb / (double)numAmps;

		// The probability per rupture variation is the remaining probability
		// divided by remaining total variations.
		double defaultProbPerRV = (numAmps == rupVarBiasesCount)
				? 0
				: (originalProb - rupVarBiasesProbSum) / (double)(numAmps - rupVarBiasesCount);
		Preconditions.checkState(defaultProbPerRV >= 0);
//		double perturb_scale = originalProb * 0.0000001;
		
//		Map<Double, List<Integer>> ret = new HashMap<>();
		
//		int index = 0;
//		while (index < numAmps) {
//			List<Integer> indexes = new ArrayList<>();
//			double prob = 0;
//			for (int i=0; i<ampsPerBundle && index<numAmps; i++) {
//				indexes.add(index++);
//				prob += rupVarBiases.getOrDefault(index, defaultProbPerRV);
//			}
//			// now randomly perturb
//			double perturb = perturb_scale*(Math.random()-0.5);
//			prob += perturb;
//			
//			Preconditions.checkState(!ret.containsKey(prob));
//			
//			ret.put(prob, indexes);
//		}
		
//		double totalProb = 0d;
//		int runningCount = 0;
//		
//		for (double prob : ret.keySet()) {
//			totalProb += prob;
//			runningCount += ret.get(prob).size();
//		}
//		
////		System.out.println("Num Amps: "+numAmps);
//		
//		Preconditions.checkState(runningCount == numAmps);
//		Preconditions.checkState(DataUtils.getPercentDiff(totalProb, originalProb) < 0.01);
//		
//		// convert back to list of probabilities
		List<Double> rvProbs = new ArrayList<>();
		for (int i = 0; i < numAmps; i++) {
			rvProbs.add(rupVarBiases.getOrDefault(i, defaultProbPerRV));
		}
//		for (int i=0; i<numAmps; i++)
//			rvProbs.add(0d);
//		
//		for (Double prob : ret.keySet()) {
//			List<Integer> indexes = ret.get(prob);
//			double probPer = prob/(double)indexes.size();
//			for (int rvIndex : indexes)
//				rvProbs.set(rvIndex, probPer);
//		}
//		
//		System.out.println(rvProbs);
		return rvProbs;
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

				if (!variationProbs.containsKey(compositeKey)) {
					variationProbs.put(compositeKey, new HashMap<Integer, Double>());
				}
				variationProbs.get(compositeKey).put(row.getInt(2), row.getDouble(3));
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


//				DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
//				PeakAmplitudesFromDBAPI peakAmplitudes = new PeakAmplitudesFromDB(db);

				// This calculator implements the RuptureVariationProbabilityModifier
				// interface and acts as both the CLT and modification logic to the 
				// HazardCurveComputation.
				RuptureVariationProbabilityModifier rupVarModifier =
						new HazardCurvePrefRupCalculator(cmd);

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
