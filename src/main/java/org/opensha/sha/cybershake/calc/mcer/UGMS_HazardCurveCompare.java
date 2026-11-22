package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jfree.data.Range;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.util.MarkdownToPDF;

import com.google.common.base.Preconditions;
import com.itextpdf.text.PageSize;

/**
 * Plots USGS NSHM 2018 hazard curves (the model under ASCE 7-22, from {@link USGS_NSHM_HazardClient}) against
 * CyberShake Study 15.4 RotD50 and RotD100 curves at a few sites, for periods of 2 s and longer.
 *
 * <p>Each site's ASCE 7-22 class is the class nearest its Thompson 2022 Vs30
 * ({@link UGMS_WebToolCalc#getThompsonVs30}), and USGS is queried at that class's Vs30. USGS annual rates are
 * converted to 1-year probabilities to share the CyberShake y-axis. Writes each raw USGS reply, one PNG per site
 * and period, and a README.md, index.html and hazard_curve_comparison.pdf report of the 2% in 50 year ground
 * motions and every plot.</p>
 */
public class UGMS_HazardCurveCompare {

	/**
	 * Default sites, picked (2026-11-21) from a survey of s348, PAS, P24, s351, USC, LADT, SMCA, WNGC, STNI and s552
	 * (Thompson 2022 classes B to DE) as the ones where CyberShake is above, below and crossing USGS
	 */
	static final String DEFAULT_SITES = "s348,s351,USC,STNI";
	static final double[] PERIODS = {2d, 3d, 4d, 5d, 7.5, 10d};
	private static final CyberShakeComponent[] COMPONENTS = {CyberShakeComponent.RotD50, CyberShakeComponent.RotD100};
	private static final PlotLineType[] COMPONENT_LINES = {PlotLineType.SOLID, PlotLineType.DASHED};
	static final double PROB_2_IN_50 = 1d - Math.pow(0.98, 1d/50d); // 1-year probability
	static final String REPORT_PDF = "hazard_curve_comparison.pdf";

	/**
	 * @return the ASCE 7-22 class (excluding Default) whose Vs30 is nearest the given Vs30
	 */
	static String nearestClass(double vs30) {
		String best = null;
		for (Map.Entry<String, Double> e : UGMS_WebToolCalc.vs30Map.entrySet())
			if (e.getValue() > 0 && (best == null
					|| Math.abs(e.getValue() - vs30) < Math.abs(UGMS_WebToolCalc.vs30Map.get(best) - vs30)))
				best = e.getKey();
		return best;
	}

	/**
	 * @return the 2% in 50 year SA (g) of the given 1-year probability curve, or NaN if the curve doesn't span it
	 */
	static double getSA_2in50(DiscretizedFunc curve) {
		if (curve.getMaxY() < PROB_2_IN_50 || curve.getMinY() > PROB_2_IN_50)
			return Double.NaN;
		return curve.getFirstInterpolatedX_inLogXLogYDomain(PROB_2_IN_50);
	}

	/**
	 * @return one sentence on where CyberShake's 2% in 50 year SA is above and below USGS, from the given
	 * CyberShake/USGS ratios at each period (NaN where either is missing)
	 */
	static String describeRatios(double[] periods, double[] ratios) {
		List<String> above = new ArrayList<>(), below = new ArrayList<>();
		double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
		for (int p=0; p<periods.length; p++) {
			if (Double.isNaN(ratios[p]))
				continue;
			min = Math.min(min, ratios[p]);
			max = Math.max(max, ratios[p]);
			if (ratios[p] > 1d)
				above.add((float)periods[p]+"");
			else if (ratios[p] < 1d)
				below.add((float)periods[p]+"");
		}
		if (min > max)
			return "No period has both a CyberShake and a USGS 2% in 50 year value.";
		String str = String.format("CyberShake RotD50 is %.2f to %.2f times USGS at the 2%% in 50 year level", min, max);
		if (below.isEmpty())
			return str+", above USGS at every period.";
		if (above.isEmpty())
			return str+", below USGS at every period.";
		return str+": below USGS at "+String.join(", ", below)+" s and above it at "+String.join(", ", above)+" s.";
	}

	private static String fmt(String format, double val) {
		return Double.isNaN(val) ? "_N/A_" : String.format(format, val);
	}

	private static Options createOptions() {
		Options ops = new Options();
		ops.addOption(Option.builder().longOpt("output-dir").hasArg().required().desc("Output directory").get());
		ops.addOption(Option.builder().longOpt("sites").hasArg()
				.desc("Comma-separated CyberShake site short names. Default: "+DEFAULT_SITES).get());
		ops.addOption(Option.builder().longOpt("thompson-file").hasArg()
				.desc("Path to Thompson 2022 Vs30 data file for local access, otherwise servlet will be used").get());
		ops.addOption(Option.builder().longOpt("thompson-header").hasArg()
				.desc("Path to Thompson 2022 header file, for a grid other than the full data file").get());
		return ops;
	}

	public static void main(String[] args) throws Exception {
		Options options = createOptions();
		CommandLine cmd;
		try {
			cmd = new DefaultParser().parse(options, args);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			new HelpFormatter().printHelp(UGMS_HazardCurveCompare.class.getSimpleName(), options, true);
			System.exit(2);
			return;
		}
		File outputDir = new File(cmd.getOptionValue("output-dir"));
		List<String> siteNames = List.of(cmd.getOptionValue("sites", DEFAULT_SITES).split(","));
		Preconditions.checkState(outputDir.exists() || outputDir.mkdirs(), "Couldn't create %s", outputDir);

		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		int datasetID = study.getDatasetIDs()[0];
		List<CybershakeRun> runs = study.runFetcher().fetch();
		SiteInfo2DB sites2db = new SiteInfo2DB(study.getDB());
		HazardCurve2DB curves2db = new HazardCurve2DB(study.getDB());
		USGS_NSHM_HazardClient usgs = new USGS_NSHM_HazardClient();

		TableBuilder siteTable = MarkdownUtils.tableBuilder();
		siteTable.addLine("Site", "Run ID", "Latitude", "Longitude", "Thompson 2022 Vs30 (m/s)",
				"ASCE 7-22 Class", "Class Vs30 (m/s)");
		TableBuilder ratioTable = MarkdownUtils.tableBuilder();
		TableBuilder valueTable = MarkdownUtils.tableBuilder();
		ratioTable.initNewLine().addColumn("Site");
		valueTable.initNewLine().addColumn("Site");
		for (double period : PERIODS) {
			ratioTable.addColumn((float)period+"s");
			valueTable.addColumn((float)period+"s");
		}
		ratioTable.finalizeLine();
		valueTable.finalizeLine();
		List<String> plotLines = new ArrayList<>();

		try {
			for (String siteName : siteNames) {
				CybershakeSite site = sites2db.getSiteFromDB(siteName);
				Preconditions.checkNotNull(site, "No CyberShake site %s", siteName);
				CybershakeRun run = runs.stream().filter(r -> r.getSiteID() == site.id).findFirst().orElse(null);
				Preconditions.checkNotNull(run, "No %s run for site %s", study.getName(), siteName);
				Location loc = site.createLocation();

				double thompsonVs30 = UGMS_WebToolCalc.getThompsonVs30(loc, cmd.getOptionValue("thompson-file"),
						cmd.getOptionValue("thompson-header"));
				String siteClass = nearestClass(thompsonVs30);
				double classVs30 = UGMS_WebToolCalc.vs30Map.get(siteClass);
				System.out.println(siteName+": run "+run.getRunID()+", Thompson 2022 Vs30 "+thompsonVs30
						+" -> class "+siteClass+" ("+classVs30+" m/s)");
				siteTable.addLine(siteName, run.getRunID(), (float)loc.getLatitude(), (float)loc.getLongitude(),
						Math.round(thompsonVs30), siteClass, Math.round(classVs30));
				ratioTable.initNewLine().addColumn("**"+siteName+"**");
				valueTable.initNewLine().addColumn("**"+siteName+"**");
				double[] ratios = new double[PERIODS.length];
				TableBuilder plotTable = MarkdownUtils.tableBuilder();
				plotTable.initNewLine();

				USGS_NSHM_HazardClient.Response reply = usgs.calculate(loc, classVs30, PERIODS);
				Files.writeString(new File(outputDir, siteName+"_usgs.json").toPath(), reply.getJson());

				for (int p=0; p<PERIODS.length; p++) {
					double period = PERIODS[p];
					List<XY_DataSet> funcs = new ArrayList<>();
					List<PlotCurveCharacterstics> chars = new ArrayList<>();

					DiscretizedFunc rates = reply.getTotalCurves().get(period);
					Preconditions.checkNotNull(rates, "USGS reply for %s has no %ss curve", siteName, period);
					ArbitrarilyDiscretizedFunc usgsCurve = new ArbitrarilyDiscretizedFunc(
							"USGS "+USGS_NSHM_HazardClient.MODEL_NAME+", class "+siteClass+" (RotD50)");
					for (int i=0; i<rates.size(); i++)
						usgsCurve.set(rates.getX(i), 1d - Math.exp(-rates.getY(i)));
					funcs.add(usgsCurve);
					chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLACK));
					double usgsSA = getSA_2in50(usgsCurve);
					double[] csSAs = {Double.NaN, Double.NaN};

					for (int c=0; c<COMPONENTS.length; c++) {
						int curveID = curves2db.getHazardCurveID(run.getRunID(), datasetID,
								CybershakeIM.getSA(COMPONENTS[c], period).getID());
						if (curveID < 0) {
							System.out.println("\tNo CS "+COMPONENTS[c]+" "+period+"s curve for "+siteName+", skipping");
							continue;
						}
						DiscretizedFunc csCurve = curves2db.getHazardCurve(curveID);
						csCurve.setName("CyberShake "+study.getName()+" ("+COMPONENTS[c]+")");
						funcs.add(csCurve);
						csSAs[c] = getSA_2in50(csCurve);
						chars.add(new PlotCurveCharacterstics(COMPONENT_LINES[c], 3f, Color.BLUE));
					}

					ArbitrarilyDiscretizedFunc ref = new ArbitrarilyDiscretizedFunc("2% in 50 years");
					ref.set(1e-3, PROB_2_IN_50);
					ref.set(10d, PROB_2_IN_50);
					funcs.add(ref);
					chars.add(new PlotCurveCharacterstics(PlotLineType.DOTTED, 2f, Color.GRAY));

					ratios[p] = csSAs[0]/usgsSA;
					ratioTable.addColumn(fmt("%.2f", ratios[p]));
					String title = siteName+", class "+siteClass+" (Thompson 2022 Vs30 "+Math.round(thompsonVs30)
							+" m/s), "+(float)period+"s SA";
					PlotSpec spec = new PlotSpec(funcs, chars, title, (float)period+"s SA (g)",
							"Annual Probability of Exceedance");
					spec.setLegendVisible(true);
					HeadlessGraphPanel gp = PlotUtils.initHeadless(PlotPreferences.getDefaultScreenFigurePrefs());
					gp.drawGraphPanel(spec, true, true, new Range(1e-3, 10d), new Range(1e-6, 1d));
					String prefix = siteName+"_"+siteClass+"_"+(float)period+"s";
					PlotUtils.writePlots(outputDir, prefix, gp, 800, 650, true, false, false);

					valueTable.addColumn(fmt("%.3f", csSAs[0])+" / "+fmt("%.3f", csSAs[1])+" / "+fmt("%.3f", usgsSA));
					plotTable.addColumn("!["+(float)period+"s]("+prefix+".png)");
				}
				ratioTable.finalizeLine();
				valueTable.finalizeLine();
				plotLines.add("## "+siteName);
				plotLines.add("");
				plotLines.add(describeRatios(PERIODS, ratios));
				plotLines.add("");
				plotLines.add("Class "+siteClass+", Thompson 2022 Vs30 "+Math.round(thompsonVs30)+" m/s, CyberShake run "
						+run.getRunID()+". Raw USGS reply: ["+siteName+"_usgs.json]("+siteName+"_usgs.json)");
				plotLines.add("");
				plotLines.addAll(plotTable.finalizeLine().wrap(3, 0).build());
				plotLines.add("");
			}
		} finally {
			study.getDB().destroy();
		}

		ratioTable.initNewLine().addColumn("_CyberShake weight when higher_");
		for (double period : PERIODS)
			ratioTable.addColumn(fmt("%.2f", CyberShakeWeightProvider.calcCyberShakeWeight(period)));
		ratioTable.finalizeLine();

		String usgsName = "USGS "+USGS_NSHM_HazardClient.MODEL_NAME;
		String csName = "CyberShake "+study.getName();
		List<String> lines = new ArrayList<>();
		lines.add("# "+csName+" vs "+usgsName+" Hazard Curves");
		lines.add("");
		lines.add("This report compares hazard curves from two models at "+siteNames.size()+" sites, for "
				+PERIODS.length+" spectral periods from "+(float)PERIODS[0]+" to "+(float)PERIODS[PERIODS.length-1]
				+ " s. The UGMS tool builds a second MCER spectrum from CyberShake's RotD100 risk-targeted values, "
				+ "with the deterministic terms taken from the USGS ASCE 7-22 service, and merges it with the USGS "
				+ "MCER: at each period it keeps the USGS value unless CyberShake's is higher, and then takes a "
				+ "log-space weighted average whose CyberShake weight is listed below the ratio table. The ratios "
				+ "here compare RotD50 curves, so they show where CyberShake is likely to raise the MCER rather "
				+ "than deciding it.");
		lines.add("");
		lines.add("* **"+usgsName+"** (black): the hazard model underlying ASCE 7-22, from the USGS web service. "
				+ "Each site uses the ASCE 7-22 site class whose Vs30 is nearest the site's Thompson 2022 Vs30, and "
				+ "USGS is queried at that class's Vs30. Its curves are RotD50, and its annual rates are converted to "
				+ "1-year probabilities (P = 1 - e<sup>-rate</sup>) to share the CyberShake axis.");
		lines.add("* **"+csName+"** (blue): "+study.getDescription()+". RotD50 (solid) and RotD100 (dashed). "
				+ "Basin and site effects come from the 3D velocity model rather than a Vs30 term, so the site "
				+ "class affects only the USGS curve.");
		lines.add("* **2% in 50 years** (gray dotted): a 1-year probability of "+(float)PROB_2_IN_50+" (about "
				+ Math.round(-1d/Math.log(1d - PROB_2_IN_50))+"-year return period), the common reference hazard "
				+ "level used here to compare the curves. The UGMS tool's probabilistic MCER is risk-targeted, "
				+ "computed from the whole curve rather than read at this level.");
		lines.add("");
		lines.add("## Sites");
		lines.add("");
		lines.addAll(siteTable.build());
		lines.add("");
		lines.add("## 2% in 50 Year Ground Motions");
		lines.add("");
		lines.add("Ratio of CyberShake RotD50 to USGS SA at the 2% in 50 year level; both are RotD50. Values above "
				+ "1 mean CyberShake predicts stronger shaking than USGS.");
		lines.add("");
		lines.addAll(ratioTable.build());
		lines.add("");
		lines.add("SA (g) at the 2% in 50 year level, as CyberShake RotD50 / CyberShake RotD100 / USGS:");
		lines.add("");
		lines.addAll(valueTable.build());
		lines.add("");
		lines.addAll(plotLines);
		MarkdownUtils.writeReadmeAndHTML(lines, outputDir);
		// each site's plots on a page of their own
		MarkdownToPDF.write(lines, outputDir, new File(outputDir, REPORT_PDF),
				(level, title) -> level == 2 && siteNames.contains(title) ? PageSize.LETTER : null);
		System.out.println("Wrote report to "+outputDir.getAbsolutePath());
	}

}
