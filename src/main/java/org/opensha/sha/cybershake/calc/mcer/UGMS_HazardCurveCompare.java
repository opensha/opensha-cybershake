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
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;

import com.google.common.base.Preconditions;

/**
 * Plots USGS NSHM 2018 hazard curves (the model under ASCE 7-22, from {@link USGS_NSHM_HazardClient}) against
 * CyberShake Study 15.4 RotD50 and RotD100 curves at a few sites, for periods of 2 s and longer.
 *
 * <p>Each site's ASCE 7-22 class is the class nearest its Thompson 2022 Vs30
 * ({@link UGMS_WebToolCalc#getThompsonVs30}), and USGS is queried at that class's Vs30. USGS annual rates are
 * converted to 1-year probabilities to share the CyberShake y-axis. Writes each raw USGS reply and one PNG per site
 * and period.</p>
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

				USGS_NSHM_HazardClient.Response reply = usgs.calculate(loc, classVs30, PERIODS);
				Files.writeString(new File(outputDir, siteName+"_usgs.json").toPath(), reply.getJson());

				for (double period : PERIODS) {
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
						chars.add(new PlotCurveCharacterstics(COMPONENT_LINES[c], 3f, Color.BLUE));
					}

					ArbitrarilyDiscretizedFunc ref = new ArbitrarilyDiscretizedFunc("2% in 50 years");
					ref.set(1e-3, PROB_2_IN_50);
					ref.set(10d, PROB_2_IN_50);
					funcs.add(ref);
					chars.add(new PlotCurveCharacterstics(PlotLineType.DOTTED, 2f, Color.GRAY));

					String title = siteName+", class "+siteClass+" (Thompson 2022 Vs30 "+Math.round(thompsonVs30)
							+" m/s), "+(float)period+"s SA";
					PlotSpec spec = new PlotSpec(funcs, chars, title, (float)period+"s SA (g)",
							"Annual Probability of Exceedance");
					spec.setLegendVisible(true);
					HeadlessGraphPanel gp = PlotUtils.initHeadless(PlotPreferences.getDefaultScreenFigurePrefs());
					gp.drawGraphPanel(spec, true, true, new Range(1e-3, 10d), new Range(1e-6, 1d));
					String prefix = siteName+"_"+siteClass+"_"+(float)period+"s";
					PlotUtils.writePlots(outputDir, prefix, gp, 800, 650, true, false, false);
				}
			}
		} finally {
			study.getDB().destroy();
		}

		System.out.println("Wrote plots to "+outputDir.getAbsolutePath());
	}

}
