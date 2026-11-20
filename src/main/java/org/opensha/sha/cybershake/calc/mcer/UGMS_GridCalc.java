package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.calc.mcer.UGMS_WebToolCalc.SpectraSource;
import org.opensha.sha.cybershake.calc.mcer.UGMS_WebToolCalc.SpectraType;

import com.google.common.base.Preconditions;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Runs the ASCE 7-22 + CyberShake 15.4 UGMS calculation ({@link UGMS_WebToolCalc}) at every multiple of a query
 * spacing inside the v18.4 web-tool region, and writes the spectral ingredients at the CyberShake periods to
 * ingredients.csv for {@link UGMS_GridReport}.
 * <p>
 * Each coordinate runs in this JVM, so the CyberShake grid files are read once. Per coordinate it saves the tool's
 * metadata.xml, the raw USGS reply and its own ingredients rows under coordinates/&lt;lat&gt;_&lt;lon&gt;/. Rerunning
 * resumes: coordinates with ingredients rows are skipped, and a saved USGS reply is reused instead of a new query.
 * Each coordinate is checked: the per-side MCER formula and the merge applied to the ingredients must reproduce the
 * tool's own CyberShake, GMPE and final MCERs.
 * <p>
 * Example, from the UGMS-opensha-backend directory:
 * <pre>
 * java -Xmx12G -cp opensha-cybershake-all.jar org.opensha.sha.cybershake.calc.mcer.UGMS_GridCalc \
 *     --data-dir data --output-dir results/grid_0.05deg_vs30_760 --spacing 0.05 --vs30 760
 * </pre>
 */
public class UGMS_GridCalc {

	/** v18.4 web tool bounds (locationBounds in https://data2.scec.org/ugms-mcerGM-tool_v18.4/) */
	static final LocationList REGION_BORDER = LocationList.of(new Location(34.95597, -118.70810),
			new Location(34.15858, -116.99926), new Location(33.29757, -117.58879), new Location(34.08743, -119.28559));
	/** periods where the CyberShake and GMPE sides are merged */
	static final double[] CS_PERIODS = {2d, 3d, 4d, 5d, 7.5d, 10d};
	static final String[] INGREDIENT_HEADER = {"lat", "lon", "period", "prob_cs", "det_cs", "prob_gmpe", "det_gmpe",
			"det_gmpe_provided", "det_ll", "tool_mcer_cs", "tool_mcer_gmpe", "tool_final"};

	private static final double CS_SPACING = 0.002;
	private static final int VEL_MODEL_ID = 5; // CVM-S4.26, CyberShake 15.4
	private static final double TOL = 1e-9;

	private final File dataDir;
	private final File outputDir;
	private final double vs30;
	private final String siteClass;
	private final GriddedSpectrumInterpolator detInterp;

	private UGMS_GridCalc(File dataDir, File outputDir, double vs30) throws Exception {
		this.dataDir = dataDir;
		this.outputDir = outputDir;
		this.vs30 = vs30;
		// one USGS class per coordinate: other Vs30 values would interpolate two classes' ingredients
		siteClass = UGMS_WebToolCalc.vs30Map.inverse().get(vs30);
		Preconditions.checkArgument(siteClass != null && vs30 > 0,
				"--vs30 must be an ASCE 7-22 site class value %s, got %s", UGMS_WebToolCalc.vs30Map, vs30);
		// Det_CS: the tool takes only the probabilistic spectrum from CyberShake, so read the deterministic grid here
		File detFile = new File(csDataDir(), "mcer_det_spectrum_"+(float)CS_SPACING+".bin");
		Preconditions.checkState(detFile.exists(), "CyberShake deterministic grid not found: %s", detFile);
		detInterp = new GriddedSpectrumInterpolator(
				new BinaryHazardCurveReader(detFile.getAbsolutePath()).getCurveMap(), CS_SPACING);
	}

	private File csDataDir() {
		return new File(dataDir, "cs_study_15_4");
	}

	/**
	 * Multiples of spacing (in degrees) inside the region
	 */
	static List<Location> gridLocations(double spacing) {
		Region region = new Region(REGION_BORDER, BorderType.MERCATOR_LINEAR);
		List<Location> locs = new ArrayList<>();
		int minLat = (int)Math.ceil(region.getMinLat()/spacing), maxLat = (int)Math.floor(region.getMaxLat()/spacing);
		int minLon = (int)Math.ceil(region.getMinLon()/spacing), maxLon = (int)Math.floor(region.getMaxLon()/spacing);
		for (int i=minLat; i<=maxLat; i++) {
			for (int j=minLon; j<=maxLon; j++) {
				Location loc = new Location(round6(i*spacing), round6(j*spacing));
				if (region.contains(loc))
					locs.add(loc);
			}
		}
		return locs;
	}

	private static double round6(double val) {
		return Math.round(val*1e6)/1e6;
	}

	static String coordName(Location loc) {
		return String.format("%.4f_%.4f", loc.getLatitude(), loc.getLongitude());
	}

	/**
	 * Ingredient rows for one coordinate, from its saved rows file or by running the tool
	 */
	private List<List<String>> calcCoordinate(Location loc) throws Exception {
		File dir = new File(new File(outputDir, "coordinates"), coordName(loc));
		File rowsFile = new File(dir, "ingredients.csv");
		if (rowsFile.exists()) {
			CSVFile<String> csv = CSVFile.readFile(rowsFile, true);
			// a file cut short (run killed while writing it) is recomputed
			if (csv.getNumRows() == CS_PERIODS.length+1) {
				List<List<String>> rows = new ArrayList<>();
				for (int r=1; r<csv.getNumRows(); r++)
					rows.add(csv.getLine(r));
				return rows;
			}
		}
		Preconditions.checkState(dir.exists() || dir.mkdirs(), "Couldn't create %s", dir);
		File siteDataDir = new File(dataDir, "site_params");
		String[] toolArgs = {
				"--cs-data-dir", csDataDir().getAbsolutePath(), "--cs-spacing", (float)CS_SPACING+"",
				"--vel-model-id", VEL_MODEL_ID+"",
				"--z10-file", new File(siteDataDir, "CVM4i26_depth_1.0.bin").getAbsolutePath(),
				"--z25-file", new File(siteDataDir, "CVM4i26_depth_2.5.bin").getAbsolutePath(),
				"--latitude", loc.getLatitude()+"", "--longitude", loc.getLongitude()+"", "--vs30", vs30+"",
				"--output-dir", dir.getAbsolutePath()};
		UGMS_WebToolCalc calc = new UGMS_WebToolCalc(new DefaultParser().parse(UGMS_WebToolCalc.createOptions(), toolArgs));
		File savedReply = new File(dir, "usgs_asce7-22_"+siteClass+".json");
		if (savedReply.exists())
			calc.setUSGS(siteClass, USGS_ASCE7_22_Client.parse(Files.readString(savedReply.toPath())));
		USGS_ASCE7_22_Client.Response usgs = calc.getUSGS(siteClass);
		DiscretizedFunc probCS = calc.getCalcSpectrum(SpectraType.MCER_PROB, SpectraSource.CYBERSHAKE);
		DiscretizedFunc toolCS = calc.getCalcSpectrum(SpectraType.MCER, SpectraSource.CYBERSHAKE);
		DiscretizedFunc toolGMPE = calc.getCalcSpectrum(SpectraType.MCER, SpectraSource.GMPE);
		DiscretizedFunc toolFinal = calc.getCalcSpectrum(SpectraType.MCER, SpectraSource.COMBINED);
		calc.writeMetadata();
		DiscretizedFunc detCS = detInterp.getInterpolated(loc);
		ArbitrarilyDiscretizedFunc eightyFourth = usgs.getEightyFourthSpectrum(); // null where USGS gives none

		List<List<String>> rows = new ArrayList<>();
		for (double t : CS_PERIODS) {
			double pc = probCS.getY(t), dc = detCS.getY(t);
			double pg = usgs.getRiskTargetedSpectrum().getY(t), ll = usgs.getDetLowerLimitSpectrum().getY(t);
			// no USGS 84th: no deterministic cap, as in UGMS_WebToolCalc.noDeterministicCap()
			double dg = eightyFourth == null ? Double.POSITIVE_INFINITY : eightyFourth.getY(t);
			// the tool's formula, applied to the ingredients, must give the tool's own MCERs
			double mcerCS = MCErCalcUtils.calcMCER(dg, pc, ll), mcerG = MCErCalcUtils.calcMCER(dg, pg, ll);
			double fin = MCERDataProductsCalc.calcFinalMCER(mcerCS, mcerG, t);
			Preconditions.checkState(Math.abs(mcerCS - toolCS.getY(t)) < TOL && Math.abs(mcerG - toolGMPE.getY(t)) < TOL
					&& Math.abs(fin - toolFinal.getY(t)) < TOL,
					"Ingredients don't reproduce the tool's MCERs at %s, %s s", loc, t);
			rows.add(List.of(String.format("%.4f", loc.getLatitude()), String.format("%.4f", loc.getLongitude()),
					(float)t+"", pc+"", dc+"", pg+"", dg+"", (eightyFourth != null)+"", ll+"",
					toolCS.getY(t)+"", toolGMPE.getY(t)+"", toolFinal.getY(t)+""));
		}
		CSVFile<String> csv = new CSVFile<>(true);
		csv.addLine(INGREDIENT_HEADER);
		for (List<String> row : rows)
			csv.addLine(row);
		csv.writeToFile(rowsFile);
		return rows;
	}

	private record Result(Location loc, List<List<String>> rows, double seconds, String error) {}

	private void run(List<Location> locs, int threads) throws Exception {
		PrintStream log = System.out;
		// the tool prints a page of progress per coordinate; keep only this runner's progress lines
		System.setOut(new PrintStream(OutputStream.nullOutputStream()));
		ExecutorService exec = Executors.newFixedThreadPool(threads);
		List<Future<Result>> futures = new ArrayList<>();
		for (Location loc : locs) {
			futures.add(exec.submit(() -> {
				long start = System.currentTimeMillis();
				try {
					return new Result(loc, calcCoordinate(loc), (System.currentTimeMillis()-start)/1000d, "");
				} catch (Exception e) {
					String msg = e.getClass().getSimpleName()+": "+e.getMessage();
					return new Result(loc, null, (System.currentTimeMillis()-start)/1000d, msg.replace('\n', ' '));
				}
			}));
		}
		List<Result> results = new ArrayList<>();
		long start = System.currentTimeMillis();
		for (int i=0; i<futures.size(); i++) {
			Result r = futures.get(i).get();
			results.add(r);
			if (r.rows == null || (i+1) % 25 == 0 || i == futures.size()-1)
				log.printf("[%d/%d] %s %s %.1fs %s (elapsed %.1f min)%n", i+1, futures.size(), coordName(r.loc),
						r.rows == null ? "failed" : "ok", r.seconds, r.error, (System.currentTimeMillis()-start)/60000d);
		}
		exec.shutdown();
		System.setOut(log);

		CSVFile<String> coords = new CSVFile<>(true);
		coords.addLine("lat", "lon", "status", "seconds", "error");
		CSVFile<String> ingredients = new CSVFile<>(true);
		ingredients.addLine(INGREDIENT_HEADER);
		int ok = 0;
		for (Result r : results) {
			coords.addLine(String.format("%.4f", r.loc.getLatitude()), String.format("%.4f", r.loc.getLongitude()),
					r.rows == null ? "failed" : "ok", String.format("%.1f", r.seconds), r.error);
			if (r.rows != null) {
				ok++;
				for (List<String> row : r.rows)
					ingredients.addLine(row);
			}
		}
		coords.writeToFile(new File(outputDir, "coordinates.csv"));
		ingredients.writeToFile(new File(outputDir, "ingredients.csv"));
		System.out.println(ok+" ok, "+(results.size()-ok)+" failed; wrote coordinates.csv and ingredients.csv to "
				+outputDir.getAbsolutePath());
	}

	private static Options createOptions() {
		Options ops = new Options();
		ops.addOption(Option.builder().longOpt("data-dir").hasArg().required()
				.desc("UGMS-opensha-backend data directory (cs_study_15_4/ and site_params/)").get());
		ops.addOption(Option.builder().longOpt("output-dir").hasArg().required().desc("Output directory").get());
		ops.addOption(Option.builder().longOpt("spacing").hasArg()
				.desc("Query spacing in degrees: every multiple inside the region is run (default 0.05)").get());
		ops.addOption(Option.builder().longOpt("vs30").hasArg()
				.desc("Vs30 for every coordinate, an ASCE 7-22 class value (default 760 = class BC)").get());
		ops.addOption(Option.builder().longOpt("threads").hasArg()
				.desc("Coordinates run in parallel; each queries the public USGS service (default 2)").get());
		ops.addOption(Option.builder().longOpt("limit").hasArg().desc("Only the first N coordinates (for testing)").get());
		ops.addOption(Option.builder().longOpt("extra-location").hasArg()
				.desc("Also run LAT,LON (repeatable)").get());
		return ops;
	}

	public static void main(String[] args) throws Exception {
		Options options = createOptions();
		CommandLine cmd;
		try {
			cmd = new DefaultParser().parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			new HelpFormatter().printHelp(UGMS_GridCalc.class.getSimpleName(), options, true);
			System.exit(2);
			return;
		}
		File dataDir = new File(cmd.getOptionValue("data-dir"));
		File outputDir = new File(cmd.getOptionValue("output-dir"));
		double spacing = Double.parseDouble(cmd.getOptionValue("spacing", "0.05"));
		double vs30 = Double.parseDouble(cmd.getOptionValue("vs30", "760"));
		int threads = Integer.parseInt(cmd.getOptionValue("threads", "2"));
		Preconditions.checkState(outputDir.exists() || outputDir.mkdirs(), "Couldn't create %s", outputDir);

		List<Location> locs = gridLocations(spacing);
		if (cmd.hasOption("limit"))
			locs = new ArrayList<>(locs.subList(0, Math.min(Integer.parseInt(cmd.getOptionValue("limit")), locs.size())));
		if (cmd.hasOption("extra-location")) {
			for (String val : cmd.getOptionValues("extra-location")) {
				String[] parts = val.split(",");
				Location loc = new Location(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
				if (!locs.contains(loc))
					locs.add(loc);
			}
		}
		System.out.println(locs.size()+" coordinates, spacing "+spacing+" deg, Vs30 "+vs30+" m/s, "+threads+" threads");

		// saved coordinates are reused, so an existing output directory must have been run with the same grid and Vs30
		File configFile = new File(outputDir, "run_config.json");
		if (configFile.exists()) {
			JsonObject prev = JsonParser.parseString(Files.readString(configFile.toPath())).getAsJsonObject();
			if (prev.get("spacing_deg").getAsDouble() != spacing || prev.get("vs30").getAsDouble() != vs30) {
				System.err.println(outputDir+" was run with --spacing "+prev.get("spacing_deg")+" --vs30 "+prev.get("vs30")
						+"; use a new --output-dir for --spacing "+spacing+" --vs30 "+vs30);
				System.exit(2);
			}
		}
		JsonObject config = new JsonObject();
		config.addProperty("spacing_deg", spacing);
		config.addProperty("vs30", vs30);
		config.addProperty("threads", threads);
		config.addProperty("n_coordinates", locs.size());
		JsonArray region = new JsonArray();
		for (Location loc : REGION_BORDER) {
			JsonArray pt = new JsonArray();
			pt.add(loc.getLatitude());
			pt.add(loc.getLongitude());
			region.add(pt);
		}
		config.add("region", region);
		config.addProperty("started", LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString());
		// the arguments as run, with absolute directories, so the report can print the command that reproduces it
		JsonArray runArgs = new JsonArray();
		for (int i=0; i<args.length; i++) {
			boolean dirValue = i > 0 && (args[i-1].equals("--data-dir") || args[i-1].equals("--output-dir"));
			runArgs.add(dirValue ? new File(args[i]).getAbsolutePath() : args[i]);
		}
		config.add("args", runArgs);
		Files.writeString(configFile.toPath(),
				new GsonBuilder().setPrettyPrinting().create().toJson(config));

		new UGMS_GridCalc(dataDir, outputDir, vs30).run(locs, threads);
		System.exit(0);
	}
}
