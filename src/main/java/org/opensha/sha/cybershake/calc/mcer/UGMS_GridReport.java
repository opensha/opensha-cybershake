package org.opensha.sha.cybershake.calc.mcer;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.RenderingHints;
import java.awt.TexturePaint;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

import org.jfree.chart.annotations.XYAnnotation;
import org.jfree.chart.annotations.XYBoxAnnotation;
import org.jfree.chart.annotations.XYLineAnnotation;
import org.jfree.chart.annotations.XYPolygonAnnotation;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.ui.RectangleAnchor;
import org.jfree.chart.ui.TextAnchor;
import org.jfree.data.Range;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.gui.plot.GeographicMapMaker;
import org.opensha.commons.mapping.PoliticalBoundariesData;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.util.MarkdownToPDF;
import org.opensha.sha.faultSurface.FaultSection;
import org.opensha.sha.faultSurface.FaultTrace;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.itextpdf.text.PageSize;

import scratch.UCERF3.enumTreeBranches.FaultModels;

/**
 * Builds the MCER report (index.html and a PDF, with figures) from a {@link UGMS_GridCalc} output directory.
 * <p>
 * At each coordinate and CyberShake period it evaluates the GMPE-side MCER, the CyberShake-side MCER under three
 * candidate formulas, and the final MCER after the {@link UGMS_WebToolCalc} merge
 * ({@link MCERDataProductsCalc#calcFinalMCER(double, double, double)}):
 * <pre>
 * option 1 (current ASCE 7-22 tool)  MCER_CS   = min[Prob_CS, max(Det_GMPE, Det_LL)]
 * option 2                           MCER_CS   = Prob_CS
 * option 3                           MCER_CS   = min[Prob_CS, max(Det_CS, Det_LL)]
 * GMPE side (all options)            MCER_GMPE = min[Prob_GMPE, max(Det_GMPE, Det_LL)]
 * </pre>
 * Each period gets one page section with all six maps. The report is only written if option 1 reproduces the tool's
 * own CyberShake, GMPE and final MCERs at every coordinate.
 * <p>
 * A run of the ASCE 7-16 tool ({@link Legacy_ASCE7_16_UGMS_WebToolCalc}, run_config.json standard ASCE7-16) has only
 * options 2 and 3: option 3 is that tool's formula, with the CyberShake Det_LL, and option 1 needs Det_GMPE, which
 * its precomputed GMPE grids don't store. Its GMPE side is the tool's GMPE MCER with no controlling term, and its
 * option 3 MCER_CS is the tool's own (read from its MCER grid; mostly within 0.5% of the formula applied to the
 * ingredient grids, but up to ~11% where the deterministic term controls; the report gives the difference). With a Vs30 map (run_config.json vs30 "map") the report adds a map of the Vs30 used.
 * <p>
 * Usage: UGMS_GridReport &lt;grid output dir&gt; [author]
 */
public class UGMS_GridReport {

	enum Term {
		PROB(new Color(42, 120, 214)),
		DET(new Color(227, 73, 72)),
		DET_LL(new Color(0, 131, 0));

		final Color color;

		Term(Color color) {
			this.color = color;
		}

		/** translucent fill, so the basemap shows through */
		Paint solid() {
			return withAlpha(color, CELL_ALPHA);
		}

		/** hatched fill, for final-map coordinates where the GMPE value is kept */
		Paint hatched() {
			return UGMS_GridReport.hatched(color);
		}
	}

	/** hatched fill in a colour */
	static Paint hatched(Color color) {
		int n = 32; // pixels per tile, drawn over HATCH_SPACING points
		BufferedImage img = new BufferedImage(n, n, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g = img.createGraphics();
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setColor(withAlpha(color, 0.15));
		g.fillRect(0, 0, n, n);
		g.setColor(withAlpha(color, 0.85));
		g.setStroke(new BasicStroke(n/7f));
		for (int off=-n; off<=n; off+=n)
			g.drawLine(off, n, off+n, 0); // 45 degree lines that join across tiles
		g.dispose();
		return new TexturePaint(img, new Rectangle2D.Double(0, 0, HATCH_SPACING, HATCH_SPACING));
	}

	private static final int[] OPTIONS_7_22 = {1, 2, 3}, OPTIONS_7_16 = {2, 3};
	/** Vs30 map bins (m/s): upper bounds, and each bin's colour from soft soil (brown) to rock (teal) */
	private static final double[] VS30_BINS = {180, 270, 360, 490, 620, 760};
	private static final Color[] VS30_COLORS = {new Color(122, 59, 22), new Color(184, 100, 26), new Color(222, 160, 60),
			new Color(232, 214, 140), new Color(150, 196, 180), new Color(58, 140, 150), new Color(36, 86, 90)};
	private static final Color VS30_NONE = new Color(150, 150, 150);
	private static final Color[] OPTION_COLORS = {new Color(36, 86, 90), new Color(184, 100, 26), new Color(106, 76, 156)};
	private static final double TOL = 1e-9;
	/** reference places drawn on every map */
	private static final Map<String, Location> PLACES = new TreeMap<>(Map.ofEntries(
			Map.entry("Los Angeles", new Location(34.052, -118.244)), Map.entry("Long Beach", new Location(33.770, -118.194)),
			Map.entry("Santa Ana", new Location(33.745, -117.868)), Map.entry("Riverside", new Location(33.953, -117.396)),
			Map.entry("San Bernardino", new Location(34.108, -117.290)), Map.entry("Lancaster", new Location(34.698, -118.137)),
			Map.entry("Palmdale", new Location(34.579, -118.116)), Map.entry("Santa Clarita", new Location(34.392, -118.543)),
			Map.entry("Oxnard", new Location(34.198, -119.177)), Map.entry("Thousand Oaks", new Location(34.171, -118.838)),
			Map.entry("Pasadena", new Location(34.148, -118.144)), Map.entry("Pomona", new Location(34.055, -117.750))));
	/** map image size: print sizing, so labels keep their point size */
	private static final double MAP_WIDTH_INCHES = 4.6;
	private static final int MAP_DPI = 300;
	private static final Color FAULT_COLOR = new Color(150, 150, 150);
	// Esri World Light Gray Canvas colours, as in the earlier Python maps
	private static final Color LAND = new Color(239, 239, 239);
	private static final Color OCEAN = new Color(208, 207, 212);
	private static final Color LABEL_COLOR = new Color(92, 92, 87);
	private static final double CELL_ALPHA = 0.6;
	private static final double HATCH_SPACING = 3d; // points
	private static final int BASEMAP_ZOOM = 10, LABELS_ZOOM = 8; // label tiles one level coarser read larger

	/**
	 * Which term sets min(P, max(D, L)); ties go to Prob, then Det
	 */
	static Term control(double p, double d, double l) {
		if (p <= Math.max(d, l))
			return Term.PROB;
		return d >= l ? Term.DET : Term.DET_LL;
	}

	/**
	 * One coordinate at one period: its ingredients, both sides' MCERs and controlling terms, and the final MCER for
	 * each option (index 1-3; index 0 unused). For ASCE 7-16 the GMPE term and option 1 are null/NaN.
	 */
	static class Point {
		final Location loc;
		final double period;
		final double vs30;
		final boolean detGMPEProvided;
		final double detLL, detLLCS, probCS, detCS, detGMPE;
		final double gmpe;
		final Term gmpeTerm;
		final double[] cs = new double[4], fin = new double[4];
		final Term[] csTerm = new Term[4];
		final boolean[] raised = new boolean[4];
		final double toolCS, toolGMPE, toolFinal;
		/** option 3 formula applied to the ingredients; for ASCE 7-16 it can differ slightly from the tool's MCER_CS */
		final double cs3Formula;

		Point(List<String> row, boolean asce7_16) {
			loc = new Location(Double.parseDouble(row.get(0)), Double.parseDouble(row.get(1)));
			period = Double.parseDouble(row.get(2));
			vs30 = Double.parseDouble(row.get(3));
			double pc = Double.parseDouble(row.get(4)), dc = Double.parseDouble(row.get(5));
			double llc = Double.parseDouble(row.get(6));
			double pg = Double.parseDouble(row.get(7)), dg = Double.parseDouble(row.get(8));
			detGMPEProvided = Boolean.parseBoolean(row.get(9));
			double ll = Double.parseDouble(row.get(10));
			detLL = ll;
			detLLCS = llc;
			probCS = pc;
			detCS = dc;
			detGMPE = dg;
			toolCS = Double.parseDouble(row.get(11));
			toolGMPE = Double.parseDouble(row.get(12));
			toolFinal = Double.parseDouble(row.get(13));

			cs[2] = pc;
			csTerm[2] = Term.PROB;
			cs3Formula = MCErCalcUtils.calcMCER(dc, pc, llc);
			csTerm[3] = control(pc, dc, llc);
			if (asce7_16) {
				gmpe = toolGMPE;
				gmpeTerm = null;
				cs[1] = Double.NaN;
				cs[3] = toolCS;
			} else {
				gmpe = MCErCalcUtils.calcMCER(dg, pg, ll);
				gmpeTerm = control(pg, dg, ll);
				cs[1] = MCErCalcUtils.calcMCER(dg, pc, ll);
				csTerm[1] = control(pc, dg, ll);
				cs[3] = cs3Formula;
			}
			for (int o : asce7_16 ? OPTIONS_7_16 : OPTIONS_7_22) {
				fin[o] = MCERDataProductsCalc.calcFinalMCER(cs[o], gmpe, period);
				raised[o] = cs[o] > gmpe; // calcFinalMCER keeps the GMPE value when it is >= CyberShake
			}
		}
	}

	private final File outputDir;
	private final JsonObject config;
	private final boolean asce7_16, vs30Map;
	/** options this tool's data supports, and the one the tool implements */
	private final int[] options;
	private final int toolOption;
	private final double spacing;
	private final Region region;
	private final Map<Double, List<Point>> byPeriod = new TreeMap<>();
	private final int numFailed;
	/** failed coordinates by reason */
	private final Map<String, Integer> failReasons = new TreeMap<>();
	private final List<? extends FaultSection> faults = FaultModels.FM3_1.getFaultSections();
	// Esri basemap layers over mapRegion, or null when the tiles can't be fetched (drawn land and labels instead)
	private BufferedImage basemap, basemapLabels;

	private UGMS_GridReport(File outputDir) throws IOException {
		this.outputDir = outputDir;
		config = JsonParser.parseString(Files.readString(new File(outputDir, "run_config.json").toPath()))
				.getAsJsonObject();
		spacing = config.get("spacing_deg").getAsDouble();
		// runs from before the standard was recorded are ASCE 7-22
		asce7_16 = config.has("standard") && config.get("standard").getAsString().equals("ASCE7-16");
		vs30Map = config.get("vs30").getAsString().equals(UGMS_GridCalc.VS30_MAP);
		options = asce7_16 ? OPTIONS_7_16 : OPTIONS_7_22;
		toolOption = asce7_16 ? 3 : 1;
		region = new Region(UGMS_GridCalc.REGION_BORDER, BorderType.MERCATOR_LINEAR);
		CSVFile<String> csv = CSVFile.readFile(new File(outputDir, "ingredients.csv"), true);
		Preconditions.checkState(csv.getLine(0).equals(List.of(UGMS_GridCalc.INGREDIENT_HEADER)),
				"Unexpected ingredients.csv header; rerun UGMS_GridCalc");
		for (int r=1; r<csv.getNumRows(); r++) {
			Point p = new Point(csv.getLine(r), asce7_16);
			byPeriod.computeIfAbsent(p.period, k -> new ArrayList<>()).add(p);
		}
		for (double t : UGMS_GridCalc.CS_PERIODS)
			Preconditions.checkState(byPeriod.containsKey(t), "ingredients.csv has no rows at %s s", t);
		CSVFile<String> coords = CSVFile.readFile(new File(outputDir, "coordinates.csv"), true);
		int failed = 0;
		for (int r=1; r<coords.getNumRows(); r++) {
			if (!coords.get(r, 2).equals("ok")) {
				failed++;
				// known offshore stops: the ASCE 7-22 tool with no Thompson 2022 Vs30, the ASCE 7-16 tool beyond its
				// onshore GMPE grids
				String error = coords.get(r, 4);
				String reason = error.contains("Bad Vs30: NaN") ? "the Thompson 2022 map has no Vs30"
						: error.contains("closest grid loc") ? "the tool's GMPE grid has no value nearby" : "other errors";
				failReasons.merge(reason, 1, Integer::sum);
			}
		}
		numFailed = failed;
	}

	private List<Point> at(double t) {
		return byPeriod.get(t);
	}

	private static double share(List<Point> pts, Predicate<Point> pred) {
		return 100d*pts.stream().filter(pred).count()/pts.size();
	}

	private static String pct(double val) {
		return String.format("%.0f", val);
	}

	private static String tStr(double t) {
		return (float)t == (int)t ? (int)t+"" : (float)t+"";
	}

	private static double median(List<Point> pts, ToDoubleFunction<Point> val) {
		return DataUtils.median(pts.stream().mapToDouble(val).toArray());
	}

	/**
	 * Max differences between the tool's option evaluated here and the tool's own MCERs (CyberShake, GMPE, final).
	 * For ASCE 7-16 the GMPE MCER is the tool's own (difference 0), and the CyberShake entry is instead the max relative
	 * difference between the option 3 formula and the tool's MCER_CS, which is reported but not required to vanish.
	 */
	private double[] validate() {
		double[] diffs = new double[3];
		for (List<Point> pts : byPeriod.values()) {
			for (Point p : pts) {
				int o = toolOption;
				diffs[0] = Math.max(diffs[0], asce7_16 ? formulaDiff(p) : Math.abs(p.cs[o] - p.toolCS));
				diffs[1] = Math.max(diffs[1], Math.abs(p.gmpe - p.toolGMPE));
				diffs[2] = Math.max(diffs[2], Math.abs(p.fin[o] - p.toolFinal));
			}
		}
		Preconditions.checkState((asce7_16 || diffs[0] < TOL) && diffs[1] < TOL && diffs[2] < TOL,
				"Option %s doesn't reproduce the tool's MCERs (max differences CS %s, GMPE %s, final %s)",
				toolOption, diffs[0], diffs[1], diffs[2]);
		return diffs;
	}

	/** ASCE 7-16: relative difference between the option 3 formula and the tool's MCER_CS */
	private static double formulaDiff(Point p) {
		return Math.abs(p.cs3Formula/p.toolCS - 1);
	}

	/** ASCE 7-16: share (%) of coordinate-periods matching pred whose formula is within 0.5% of the tool's MCER_CS */
	private double shareWithinHalfPct(Predicate<Point> pred) {
		return share(byPeriod.values().stream().flatMap(List::stream).filter(pred).toList(), p -> formulaDiff(p) < 0.005);
	}

	/** ASCE 7-16: coordinate-periods where the option 3 formula, instead of the tool's MCER_CS, flips raised/kept */
	private long formulaFlips() {
		return byPeriod.values().stream().flatMap(List::stream)
				.filter(p -> (p.cs3Formula > p.gmpe) != p.raised[3]).count();
	}

	// ------------------------------------------------------------------ figures

	private File resourcesDir() {
		return new File(new File(outputDir, "report"), "resources");
	}

	private static Color withAlpha(Color c, double alpha) {
		return new Color(c.getRed(), c.getGreen(), c.getBlue(), (int)Math.round(255*alpha));
	}

	private Region mapRegion() {
		return new Region(new Location(region.getMinLat()-0.15, region.getMinLon()-0.15),
				new Location(region.getMaxLat()+0.15, region.getMaxLon()+0.3)); // room for east-edge labels
	}

	/**
	 * Fetches (or reads from the tile cache) the Esri base and label layers once for all maps
	 */
	private void loadBasemap() {
		Region r = mapRegion();
		EsriBasemap esri = new EsriBasemap(new File(outputDir, ".tile_cache"));
		try {
			basemap = esri.image(EsriBasemap.BASE, BASEMAP_ZOOM, r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
			basemapLabels = esri.image(EsriBasemap.LABELS, LABELS_ZOOM, r.getMinLat(), r.getMaxLat(), r.getMinLon(),
					r.getMaxLon());
		} catch (IOException | InterruptedException e) {
			System.err.println("WARNING: no Esri basemap ("+e.getMessage()+"); drawing land and place labels instead");
			basemap = null;
			basemapLabels = null;
		}
	}

	private XYAnnotation basemapAnnotation(BufferedImage image) {
		Region r = mapRegion();
		return new EsriBasemap.ImageAnnotation(image, r.getMinLat(), r.getMaxLat(), r.getMinLon(), r.getMaxLon());
	}

	/**
	 * One map, styled like the earlier Python maps: each coordinate is a translucent square with a thin white outline,
	 * filled by its category (an index into fills), over the Esri light-grey basemap
	 */
	private void map(String prefix, List<Point> pts, ToIntFunction<Point> category, List<Paint> fills,
			List<String> legend) throws IOException {
		Region mapRegion = mapRegion();
		GeographicMapMaker mapMaker = new GeographicMapMaker(mapRegion);
		mapMaker.setPoliticalBoundaryChar(new PlotCurveCharacterstics(PlotLineType.SOLID, 0.5f, LAND));
		mapMaker.setAxisLabelsVisible(false);

		// everything is drawn as annotations, in this order: basemap (or drawn land: the chart renderer can't fill
		// the mainland outline), fault traces, cells, region outline, basemap labels (or ours), legend
		List<XYAnnotation> anns = new ArrayList<>();
		if (basemap != null) {
			anns.add(basemapAnnotation(basemap));
		} else {
			for (XY_DataSet outline : PoliticalBoundariesData.loadDefaultOutlines(mapRegion)) {
				double[] xy = new double[2*outline.size()];
				for (int i=0; i<outline.size(); i++) {
					xy[2*i] = outline.getX(i);
					xy[2*i+1] = outline.getY(i);
				}
				anns.add(new XYPolygonAnnotation(xy, null, null, LAND));
			}
		}
		BasicStroke faultStroke = new BasicStroke(0.5f);
		for (FaultSection sect : faults) {
			FaultTrace trace = sect.getFaultTrace();
			for (int i=1; i<trace.size(); i++)
				anns.add(new XYLineAnnotation(trace.get(i-1).getLongitude(), trace.get(i-1).getLatitude(),
						trace.get(i).getLongitude(), trace.get(i).getLatitude(), faultStroke, FAULT_COLOR));
		}
		double h = spacing/2;
		BasicStroke cellEdge = new BasicStroke(0.3f);
		for (Point p : pts) {
			double lat = p.loc.getLatitude(), lon = p.loc.getLongitude();
			anns.add(new XYPolygonAnnotation(new double[] {lon-h, lat-h, lon+h, lat-h, lon+h, lat+h, lon-h, lat+h},
					cellEdge, Color.WHITE, fills.get(category.applyAsInt(p))));
		}
		double[] outline = new double[2*UGMS_GridCalc.REGION_BORDER.size()];
		for (int i=0; i<UGMS_GridCalc.REGION_BORDER.size(); i++) {
			outline[2*i] = UGMS_GridCalc.REGION_BORDER.get(i).getLongitude();
			outline[2*i+1] = UGMS_GridCalc.REGION_BORDER.get(i).getLatitude();
		}
		anns.add(new XYPolygonAnnotation(outline, new BasicStroke(0.8f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER,
				10f, new float[] {4f, 3f}, 0f), Color.BLACK, null));
		if (basemapLabels != null) {
			anns.add(basemapAnnotation(basemapLabels));
		} else {
			for (String name : PLACES.keySet()) {
				Location loc = PLACES.get(name);
				XYTextAnnotation ann = new XYTextAnnotation(name, loc.getLongitude(), loc.getLatitude());
				ann.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 6));
				ann.setPaint(LABEL_COLOR);
				anns.add(ann);
			}
		}
		// legend in the empty north-east corner (GeographicMapMaker legends fail alongside annotations-only data)
		double legendLon = -117.55, legendLat = region.getMaxLat()+0.08, step = 0.075, box = 0.05;
		// null legend entries are categories that can't occur on this map
		List<Integer> shown = new ArrayList<>();
		for (int i=0; i<legend.size(); i++)
			if (legend.get(i) != null)
				shown.add(i);
		// white backing so basemap labels don't show through the legend
		anns.add(new XYBoxAnnotation(legendLon-0.03, legendLat-(shown.size()+0.5)*step, mapRegion.getMaxLon()-0.02,
				legendLat+step/2, null, null, withAlpha(Color.WHITE, 0.85)));
		for (int row=0; row<shown.size(); row++) {
			int i = shown.get(row);
			double y = legendLat - row*step;
			anns.add(new XYBoxAnnotation(legendLon, y-box/2, legendLon+box, y+box/2, new BasicStroke(0.3f),
					Color.GRAY, fills.get(i)));
			XYTextAnnotation ann = new XYTextAnnotation(legend.get(i), legendLon+box+0.03, y);
			ann.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 6));
			ann.setTextAnchor(TextAnchor.CENTER_LEFT);
			anns.add(ann);
		}
		// fault-trace key below the colour legend
		double y = legendLat - shown.size()*step;
		anns.add(new XYLineAnnotation(legendLon, y, legendLon+box, y, new BasicStroke(0.5f), FAULT_COLOR));
		XYTextAnnotation faultAnn = new XYTextAnnotation("UCERF3 FM3.1 faults", legendLon+box+0.03, y);
		faultAnn.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 6));
		faultAnn.setTextAnchor(TextAnchor.CENTER_LEFT);
		anns.add(faultAnn);
		mapMaker.setAnnotations(anns);

		mapMaker.plot(resourcesDir(), prefix, mapMaker.buildPlot(" ", true), MAP_WIDTH_INCHES, MAP_DPI, gp -> {
			gp.getPlot().setBackgroundPaint(OCEAN);
			gp.getPlot().setDomainGridlinesVisible(false);
			gp.getPlot().setRangeGridlinesVisible(false);
			((NumberAxis)gp.getXAxis()).setNumberFormatOverride(new DecimalFormat("0.#\u00b0E;0.#\u00b0W"));
			((NumberAxis)gp.getYAxis()).setNumberFormatOverride(new DecimalFormat("0.#\u00b0N;0.#\u00b0S"));
		});
	}

	/** term names by side: index 0 Prob, 1 Det, 2 Det_LL */
	private static final List<String> CS_OPT1_TERMS = List.of("Prob_CS", "Det_GMPE", "Det_LL");
	private static final List<String> CS_OPT3_TERMS = List.of("Prob_CS", "Det_CS", "Det_LL");
	private static final List<String> GMPE_TERMS = List.of("Prob_GMPE", "Det_GMPE", "Det_LL");

	private static List<String> csTerms(int option) {
		return option == 3 ? CS_OPT3_TERMS : CS_OPT1_TERMS;
	}

	private void sideMap(String prefix, List<Point> pts, ToIntFunction<Point> term, List<String> names)
			throws IOException {
		List<Paint> fills = new ArrayList<>();
		for (Term t : Term.values())
			fills.add(t.solid());
		map(prefix, pts, term, fills, names);
	}

	private void finalMap(String prefix, List<Point> pts, int o) throws IOException {
		List<Paint> fills = new ArrayList<>();
		List<String> legend = new ArrayList<>();
		for (Term t : Term.values()) {
			fills.add(t.solid());
			// option 2's CyberShake side is Prob_CS only
			legend.add(o == 2 && t != Term.PROB ? null : "CS raised, "+csTerms(o).get(t.ordinal()));
		}
		if (asce7_16) {
			// no GMPE controlling term: one hatched category
			fills.add(hatched(Color.GRAY));
			legend.add("GMPE kept");
			map(prefix, pts, p -> p.raised[o] ? p.csTerm[o].ordinal() : 3, fills, legend);
			return;
		}
		for (Term t : Term.values()) {
			fills.add(t.hatched());
			legend.add("GMPE kept, "+GMPE_TERMS.get(t.ordinal()));
		}
		map(prefix, pts, p -> p.raised[o] ? p.csTerm[o].ordinal() : 3 + p.gmpeTerm.ordinal(), fills, legend);
	}

	/** Vs30 bin index of a coordinate, or the last index where the map has no value */
	private static int vs30Bin(double vs30) {
		if (!Double.isFinite(vs30))
			return VS30_BINS.length+1;
		int i = 0;
		while (i < VS30_BINS.length && vs30 >= VS30_BINS[i])
			i++;
		return i;
	}

	private static List<String> vs30BinNames() {
		List<String> names = new ArrayList<>();
		names.add("< "+(int)VS30_BINS[0]+" m/s");
		for (int i=1; i<VS30_BINS.length; i++)
			names.add((int)VS30_BINS[i-1]+"-"+(int)VS30_BINS[i]+" m/s");
		names.add(">= "+(int)VS30_BINS[VS30_BINS.length-1]+" m/s");
		names.add("no map value");
		return names;
	}

	private void vs30MapFigure(String prefix, List<Point> pts) throws IOException {
		List<Paint> fills = new ArrayList<>();
		for (Color c : VS30_COLORS)
			fills.add(withAlpha(c, 0.75));
		fills.add(withAlpha(VS30_NONE, 0.75));
		List<String> legend = new ArrayList<>(vs30BinNames());
		if (pts.stream().allMatch(p -> Double.isFinite(p.vs30)))
			legend.set(legend.size()-1, null);
		map(prefix, pts, p -> vs30Bin(p.vs30), fills, legend);
	}

	private void overviewChart(String prefix, String title, Predicate<Point>[] preds) throws IOException {
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		PlotLineType[] lines = {PlotLineType.SOLID, PlotLineType.DASHED, PlotLineType.DOTTED};
		PlotSymbol[] symbols = {PlotSymbol.FILLED_CIRCLE, PlotSymbol.FILLED_SQUARE, PlotSymbol.FILLED_TRIANGLE};
		for (int o : options) {
			ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc("Option "+o);
			for (double t : UGMS_GridCalc.CS_PERIODS)
				func.set(t, share(at(t), preds[o]));
			funcs.add(func);
			chars.add(new PlotCurveCharacterstics(lines[o-1], 3f, symbols[o-1], 7f, OPTION_COLORS[o-1]));
		}
		PlotSpec spec = new PlotSpec(funcs, chars, title, "Period (s)", "Coordinates (%)");
		spec.setLegendInset(RectangleAnchor.RIGHT);
		HeadlessGraphPanel gp = PlotUtils.initHeadless(PlotPreferences.getDefaultScreenFigurePrefs());
		gp.drawGraphPanel(spec, false, false, new Range(1.5, 10.5), new Range(0d, 100d));
		PlotUtils.writePlots(resourcesDir(), prefix, gp, 700, 500, false, true, 2d, true, 1d, false);
	}

	// ------------------------------------------------------------------ report

	/**
	 * PDF name from the run's inputs, e.g. mcer_report_asce7-16_vs30-wills2015_0.05deg.pdf or
	 * mcer_report_asce7-22_vs30-760_0.05deg.pdf
	 */
	String pdfName() {
		String vs30 = vs30Map ? (asce7_16 ? "wills2015" : "thompson2022")
				: new DecimalFormat("0.##").format(config.get("vs30").getAsDouble());
		return "mcer_report_"+(asce7_16 ? "asce7-16" : "asce7-22")+"_vs30-"+vs30+"_"
				+ new DecimalFormat("0.####").format(spacing)+"deg.pdf";
	}

	/** e.g. "ASCE 7-16, Vs30 from Wills 2015" */
	private String caseName() {
		String vs30 = vs30Map ? (asce7_16 ? "Vs30 from Wills 2015" : "Vs30 from Thompson 2022")
				: "Vs30 = "+(int)config.get("vs30").getAsDouble()+" m/s";
		return (asce7_16 ? "ASCE 7-16" : "ASCE 7-22")+", "+vs30;
	}

	/** "a, b and c g" for values that are the same at every coordinate, else "from min to max g" per period */
	private String perPeriodValues(ToDoubleFunction<Point> val) {
		List<String> vals = new ArrayList<>();
		boolean constant = true;
		for (double t : UGMS_GridCalc.CS_PERIODS) {
			double[] v = at(t).stream().mapToDouble(val).toArray();
			constant &= DataUtils.min(v) == DataUtils.max(v);
			vals.add(String.format("%.3g-%.3g", DataUtils.min(v), DataUtils.max(v)));
		}
		if (constant) {
			vals.clear();
			for (double t : UGMS_GridCalc.CS_PERIODS)
				vals.add((float)val.applyAsDouble(at(t).get(0))+"");
			return String.join(", ", vals)+" g at the six periods";
		}
		return "it ranges over "+String.join(", ", vals)+" g at the six periods";
	}

	private void write(String author) throws IOException {
		double[] diffs = validate();
		loadBasemap();
		Preconditions.checkState(resourcesDir().exists() || resourcesDir().mkdirs());
		double[] periods = UGMS_GridCalc.CS_PERIODS;
		List<Point> first = at(periods[0]);
		int n = first.size();
		long no84 = first.stream().filter(p -> !p.detGMPEProvided).count();
		long noVs30 = first.stream().filter(p -> !Double.isFinite(p.vs30)).count();
		List<String> weights = new ArrayList<>();
		for (double t : periods)
			weights.add((float)CyberShakeWeightProvider.calcCyberShakeWeight(t)+"");
		String tool = asce7_16 ? "`Legacy_ASCE7_16_UGMS_WebToolCalc` (ASCE 7-16 with CyberShake 15.4)"
				: "`UGMS_WebToolCalc` (ASCE 7-22 with CyberShake 15.4)";
		String failedText;
		if (numFailed == 0) {
			failedText = "Every coordinate completed.";
		} else {
			List<String> reasons = new ArrayList<>();
			failReasons.forEach((reason, count) -> reasons.add(count+(reason.equals("other errors") ? " with " : " where ")
					+ reason));
			failedText = numFailed+" coordinates stop the tool and are excluded ("+String.join("; ", reasons)
					+ (failReasons.containsKey("other errors") ? "" : "; all offshore")+"), leaving "+n+".";
		}

		List<String> lines = new ArrayList<>();
		lines.add("# MCE<sub>R</sub> controlling terms: CyberShake 15.4 with "+caseName());
		lines.add("");
		lines.add(author+", "+LocalDate.now().format(DateTimeFormatter.ofPattern("MMMM d, yyyy")));
		lines.add("");
		int tocIndex = lines.size();

		lines.add("## Purpose");
		lines.add("");
		lines.add("This document presents, at each queried coordinate and CyberShake period, which term sets the "
				+ (asce7_16 ? "CyberShake-side MCE<sub>R</sub>, and whether CyberShake raises the final MCE<sub>R</sub> "
						+ "above the GMPE-side value when the two are merged, under two candidate formulas for the "
						+ "CyberShake side. "
						: "CyberShake-side and GMPE-side MCE<sub>R</sub> and the final MCE<sub>R</sub> after the two are "
						+ "merged, under three candidate formulas for the CyberShake side. ")
				+ "It uses "+caseName()+". [Results](#results) gives summary tables and one section of maps per period; "
				+ "[Validation](#validation) records the checks against the tool.");
		lines.add("");

		lines.add("## Data and methods");
		lines.add("");
		lines.add("### Coordinates");
		lines.add("");
		String vs30Text;
		if (!vs30Map && !asce7_16)
			vs30Text = "Every coordinate uses Vs30 = "+(int)config.get("vs30").getAsDouble()+" m/s, which is exactly "
					+ "ASCE 7-22 site class BC, so each needs one USGS service call and no Vs30 interpolation.";
		else if (!vs30Map)
			vs30Text = "Every coordinate uses Vs30 = "+(int)config.get("vs30").getAsDouble()+" m/s, which is exactly "
					+ "ASCE 7-16 site class BC, so the GMPE side is read from one precomputed class grid, with no Vs30 "
					+ "interpolation.";
		else if (asce7_16)
			vs30Text = "Vs30 is the Wills 2015 map value at each coordinate, as the ASCE 7-16 tool uses when no site "
					+ "class or Vs30 is given (see [Vs30](#vs30)). The GMPE side is then read from the tool's "
					+ "precomputed Wills 2015 GMPE grid, which was computed with the map's Vs30 at each grid point, so "
					+ "there is no interpolation between site classes."
					+ (noVs30 > 0 ? " At "+noVs30+" coordinates (offshore) the Wills map has no value; the tool still "
							+ "returns GMPE values there, from the nearest defined point of its Wills GMPE grid." : "");
		else
			vs30Text = "Vs30 is the Thompson 2022 map value at each coordinate, as the ASCE 7-22 tool uses when no site "
					+ "class or Vs30 is given (see [Vs30](#vs30)). The USGS service takes site classes only, so the tool "
					+ "queries the two classes whose Vs30 bound the coordinate's value and interpolates every GMPE "
					+ "ingredient log-log in Vs30 (one class where the value is a class value, or outside 150-1500 m/s, "
					+ "where it is clamped).";
		lines.add("We ran "+tool+" at every multiple of "+(float)spacing+"&deg; in latitude and longitude inside the "
				+ "v18.4 web-tool region (the dashed outline in every map), "+(n+numFailed)+" coordinates in all. This query spacing "
				+ "is our choice; it is not the resolution of the underlying data (the CyberShake grids are at "
				+ "0.002&deg;"+(asce7_16 ? ", the ASCE 7-16 GMPE grids at "+(float)0.01+"&deg;" : "")+"). "+vs30Text+" "
				+ failedText
				+ (asce7_16 ? "" : " At "+no84+" coordinates the USGS service returns no 84th-percentile spectrum; it "
						+ "applies no deterministic cap there, so its MCE<sub>R</sub> equals the probabilistic spectrum. "
						+ "We treat Det_GMPE as unbounded at those coordinates."));
		lines.add("");
		if (vs30Map) {
			lines.add("### Vs30");
			lines.add("");
			vs30MapFigure("vs30_map", first);
			lines.add("![Vs30](resources/vs30_map.png)");
			lines.add("");
			TableBuilder table = MarkdownUtils.tableBuilder();
			List<String> names = vs30BinNames();
			table.initNewLine();
			for (int i=0; i<names.size(); i++)
				table.addColumn(names.get(i));
			table.finalizeLine();
			table.initNewLine();
			for (int i=0; i<names.size(); i++) {
				int bin = i;
				table.addColumn(count(first, p -> vs30Bin(p.vs30) == bin));
			}
			table.finalizeLine();
			lines.addAll(table.build());
			lines.add("");
			double[] v = first.stream().mapToDouble(p -> p.vs30).filter(Double::isFinite).toArray();
			lines.add("Vs30 used at each coordinate ("+(asce7_16 ? "Wills 2015" : "Thompson 2022")+"), in m/s: "
					+ "median "+(int)DataUtils.median(v)+", range "+(int)DataUtils.min(v)+" to "+(int)DataUtils.max(v)
					+ ". Coordinates per bin, with their share in brackets.");
			lines.add("");
		}
		lines.add("### Spectral ingredients");
		lines.add("");
		lines.add("All are evaluated at the CyberShake periods "+String.join(", ", tStrs(periods))+" s, where the two "
				+ "sides are merged.");
		lines.add("");
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Term", "Meaning", "Source");
		table.addLine("Prob_CS", "CyberShake risk-targeted probabilistic",
				"CyberShake 15.4 grid (0.002&deg;), as used by the tool");
		table.addLine("Det_CS", "CyberShake 84th-percentile deterministic", "CyberShake 15.4 deterministic grid (0.002&deg;)"
				+ (asce7_16 ? ", as used by the tool" : ""));
		if (asce7_16) {
			table.addLine("Det_LL", "Deterministic lower limit", "CyberShake 15.4 lower-limit grid (0.002&deg;), as used by "
					+ "the tool");
			table.addLine("MCER_GMPE", "GMPE-side MCE<sub>R</sub>", "The tool's precomputed UCERF3 GMPE MCER grid ("
					+ (vs30Map ? "Wills 2015 Vs30" : "class BC")+", 0.01&deg;); its ingredients are not stored");
		} else {
			table.addLine("Prob_GMPE", "GMPE risk-targeted probabilistic", "USGS ASCE 7-22 service, `riskTargetedSpectrum`");
			table.addLine("Det_GMPE", "GMPE 84th-percentile deterministic", "USGS service, `eightyFourthSpectrum`");
			table.addLine("Det_LL", "Deterministic lower limit", "USGS service, `metadata.sadFloor`"
					+ (vs30Map ? "" : " (class BC)"));
		}
		lines.addAll(table.build());
		lines.add("");
		if (asce7_16)
			lines.add("Det_LL is the CyberShake lower-limit grid the ASCE 7-16 tool reads; "
					+ perPeriodValues(p -> p.detLLCS)+". It does not change with the Vs30 used for the GMPE side.");
		else if (vs30Map)
			lines.add("Det_LL depends on site class, so here it varies with the Vs30 at each coordinate; "
					+ perPeriodValues(p -> p.detLL)+".");
		else
			lines.add("Det_LL depends on site class but not location; at class BC it is "
					+ perPeriodValues(p -> p.detLL)+".");
		lines.add("");
		lines.add("### Per-side MCER and the "+(asce7_16 ? "two" : "three")+" options");
		lines.add("");
		if (asce7_16) {
			lines.add("The CyberShake-side MCE<sub>R</sub> is the smaller of its probabilistic spectrum and the larger of "
					+ "its deterministic spectrum and the lower limit (`MCErCalcUtils.calcMCER`). The GMPE side is the "
					+ "tool's GMPE MCE<sub>R</sub> in every case; its ingredients are not stored, so its controlling term "
					+ "is not shown. Option 1 of the ASCE 7-22 reports needs Det_GMPE and is not available here. The two "
					+ "candidate formulas for the CyberShake side are:");
		} else {
			lines.add("Each side's MCE<sub>R</sub> is the smaller of its probabilistic spectrum and the larger of a "
					+ "deterministic spectrum and the lower limit (`MCErCalcUtils.calcMCER`). The GMPE side is the same "
					+ "in every case, and the three candidate formulas for the CyberShake side are:");
		}
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.addLine("", "Formula");
		if (asce7_16) {
			table.addLine("GMPE side (every option)", "`MCER_GMPE` from the tool's GMPE grid");
			table.addLine("Option 2", "`MCER_CS = Prob_CS`");
			table.addLine("Option 3 (current ASCE 7-16 tool)", "`MCER_CS = min[Prob_CS, max(Det_CS, Det_LL)]`");
		} else {
			table.addLine("GMPE side (every option)", "`MCER_GMPE = min[Prob_GMPE, max(Det_GMPE, Det_LL)]`");
			table.addLine("Option 1 (current ASCE 7-22 tool)", "`MCER_CS = min[Prob_CS, max(Det_GMPE, Det_LL)]`");
			table.addLine("Option 2", "`MCER_CS = Prob_CS`");
			table.addLine("Option 3", "`MCER_CS = min[Prob_CS, max(Det_CS, Det_LL)]`");
		}
		lines.addAll(table.build());
		lines.add("");
		if (asce7_16) {
			lines.add("The ASCE 7-16 tool reads its MCER_CS from its own precomputed CyberShake MCER grid rather than "
					+ "applying the formula to the ingredient grids. The two agree to within 0.5% at "
					+ pct(shareWithinHalfPct(p -> true))+"% of coordinate-periods, but differ by up to "
					+ String.format("%.2g", 100*diffs[0])+"%, mostly where the deterministic term controls (within 0.5% "
					+ "at only "+pct(shareWithinHalfPct(p -> p.csTerm[3] != Term.PROB))+"% of those); see "
					+ "[Validation](#validation). We use the tool's value for option 3 and the ingredients only to find "
					+ "the term that sets it, so that term is less certain where Det_CS or Det_LL controls.");
			lines.add("");
		}
		lines.add("### Combining the two sides");
		lines.add("");
		lines.add("The final MCE<sub>R</sub> follows `MCERDataProductsCalc.calcFinalMCER`: the GMPE value is kept unless "
				+ "the CyberShake value is higher, in which case the two are averaged in log space with CyberShake "
				+ "weight w(T):");
		lines.add("");
		lines.add("`Final = MCER_GMPE` if `MCER_GMPE >= MCER_CS`, otherwise "
				+ "`Final = exp[w ln(MCER_CS) + (1 - w) ln(MCER_GMPE)]`");
		lines.add("");
		lines.add("with w = "+String.join(", ", weights)+" at T = "+String.join(", ", tStrs(periods))
				+ " s (0.2 at 2 s rising linearly to 0.5 at 5 s).");
		lines.add("");
		lines.add("### Map conventions");
		lines.add("");
		lines.add("Each coloured cell is one queried coordinate, centred on it and one query step wide; its colour is "
				+ "the result at that coordinate, not an aggregate. In maps of a single side, the colour is the term "
				+ "that sets that side's MCE<sub>R</sub>: blue the probabilistic term, red the deterministic term, green "
				+ "Det_LL (ties go to the probabilistic term). In maps of the final MCE<sub>R</sub>, a solid cell "
				+ "means CyberShake raised the final value, coloured by the CyberShake term that set MCER_CS; a hatched "
				+ "cell means the GMPE value was kept"
				+ (asce7_16 ? " (grey, as the GMPE term is not known)" : ", coloured by the GMPE term that set MCER_GMPE")
				+ ". Solid blue therefore marks the coordinates where CyberShake simulation values reach the final "
				+ "MCE<sub>R</sub>. Thin grey lines are UCERF3 FM3.1 fault traces, for reference. Basemap: "
				+ (basemap != null ? EsriBasemap.ATTRIBUTION+"." : "coastline from OpenSHA's political boundaries "
						+ "(the Esri basemap could not be downloaded)."));
		lines.add("");

		// results
		lines.add("## Results");
		lines.add("");
		lines.add("### Overview");
		lines.add("");
		@SuppressWarnings("unchecked")
		Predicate<Point>[] raisedAny = new Predicate[4], raisedProb = new Predicate[4];
		for (int o : options) {
			raisedAny[o] = p -> p.raised[o];
			raisedProb[o] = p -> p.raised[o] && p.csTerm[o] == Term.PROB;
		}
		overviewChart("overview_raised_any", "CyberShake raises the final MCER", raisedAny);
		overviewChart("overview_raised_prob", "Prob_CS raises the final MCER", raisedProb);
		table = MarkdownUtils.tableBuilder();
		table.addLine("(a) CyberShake raises the final MCE<sub>R</sub>", "(b) Prob_CS raises the final MCE<sub>R</sub>");
		table.addLine("![Raised](resources/overview_raised_any.png)", "![Raised by Prob_CS](resources/overview_raised_prob.png)");
		lines.addAll(table.build());
		lines.add("");
		lines.add("Share of coordinates where the final MCE<sub>R</sub> is raised above the GMPE value (a) by CyberShake "
				+ "through any term and (b) by Prob_CS, i.e. where CyberShake simulation values reach the final "
				+ "MCE<sub>R</sub>.");
		lines.add("");

		lines.add("**Controlling term of "+(asce7_16 ? "the CyberShake-side" : "each side's")+" MCE<sub>R</sub>** "
				+ "(% of coordinates). Under option 2, MCER_CS = Prob_CS at every coordinate.");
		lines.add("");
		List<String> sideNames = asce7_16 ? List.of("Option 3 CS") : List.of("Option 1 CS", "GMPE", "Option 3 CS");
		List<ToIntFunction<Point>> sides = asce7_16 ? List.of(p -> p.csTerm[3].ordinal())
				: List.of(p -> p.csTerm[1].ordinal(), p -> p.gmpeTerm.ordinal(), p -> p.csTerm[3].ordinal());
		table = MarkdownUtils.tableBuilder();
		table.initNewLine().addColumn("T (s)");
		for (String side : sideNames)
			for (String name : side.equals("GMPE") ? GMPE_TERMS : side.startsWith("Option 1") ? CS_OPT1_TERMS
					: CS_OPT3_TERMS)
				table.addColumn(side+": "+name);
		table.finalizeLine();
		for (double t : periods) {
			table.initNewLine().addColumn(tStr(t));
			for (ToIntFunction<Point> side : sides)
				for (Term term : Term.values())
					table.addColumn(pct(share(at(t), p -> side.applyAsInt(p) == term.ordinal())));
			table.finalizeLine();
		}
		lines.addAll(table.build());
		lines.add("");
		lines.add("**Final MCE<sub>R</sub>** (% of coordinates): the GMPE value is kept, or CyberShake raises it through "
				+ "Prob_CS, the deterministic term, or Det_LL. Each option's four columns sum to 100.");
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.initNewLine().addColumn("T (s)");
		for (int o : options)
			table.addColumns("Opt "+o+": GMPE kept", "Opt "+o+": "+csTerms(o).get(0),
					"Opt "+o+": "+(o == 2 ? "n/a" : csTerms(o).get(1)), "Opt "+o+": "+(o == 2 ? "n/a" : csTerms(o).get(2)));
		table.finalizeLine();
		for (double t : periods) {
			table.initNewLine().addColumn(tStr(t));
			for (int o : options) {
				table.addColumn(pct(share(at(t), p -> !p.raised[o])));
				for (Term term : Term.values())
					table.addColumn(o == 2 && term != Term.PROB ? "-"
							: pct(share(at(t), p -> p.raised[o] && p.csTerm[o] == term)));
			}
			table.finalizeLine();
		}
		lines.addAll(table.build());
		lines.add("");

		lines.add("### Maps by period");
		lines.add("");
		lines.addAll(mapsGuide());
		for (double t : periods) {
			List<Point> pts = at(t);
			String tag = "T"+tStr(t).replace('.', 'p')+"s";
			lines.add("#### T = "+tStr(t)+" s: maps");
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			if (asce7_16) {
				sideMap(tag+"_cs_opt3", pts, p -> p.csTerm[3].ordinal(), CS_OPT3_TERMS);
				finalMap(tag+"_final_opt3", pts, 3);
				finalMap(tag+"_final_opt2", pts, 2);
				table.addLine("(a) MCER_CS, option 3", "(b) Final MCE<sub>R</sub>, option 3",
						"(c) Final MCE<sub>R</sub>, option 2");
				table.addLine("![a](resources/"+tag+"_cs_opt3.png)", "![b](resources/"+tag+"_final_opt3.png)",
						"![c](resources/"+tag+"_final_opt2.png)");
			} else {
				sideMap(tag+"_cs_opt1", pts, p -> p.csTerm[1].ordinal(), CS_OPT1_TERMS);
				sideMap(tag+"_gmpe", pts, p -> p.gmpeTerm.ordinal(), GMPE_TERMS);
				sideMap(tag+"_cs_opt3", pts, p -> p.csTerm[3].ordinal(), CS_OPT3_TERMS);
				for (int o : options)
					finalMap(tag+"_final_opt"+o, pts, o);
				table.addLine("(a) MCER_CS, option 1", "(b) MCER_GMPE (all options)", "(c) MCER_CS, option 3");
				table.addLine("![a](resources/"+tag+"_cs_opt1.png)", "![b](resources/"+tag+"_gmpe.png)",
						"![c](resources/"+tag+"_cs_opt3.png)");
				table.addLine("(d) Final MCE<sub>R</sub>, option 1", "(e) Final MCE<sub>R</sub>, option 2",
						"(f) Final MCE<sub>R</sub>, option 3");
				table.addLine("![d](resources/"+tag+"_final_opt1.png)", "![e](resources/"+tag+"_final_opt2.png)",
						"![f](resources/"+tag+"_final_opt3.png)");
			}
			lines.addAll(table.build());
			lines.add("");

			lines.add("##### T = "+tStr(t)+" s: statistics");
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Panel", "Coordinates", "Prob", "Det_GMPE", "Det_CS", "Det_LL", "Total", "Median MCER (g)",
					"Final / GMPE, raised: median", "max");
			String any = "all: term that sets it";
			if (asce7_16) {
				addRow(table, "(a) MCER_CS, option 3", any, pts, p -> true, p -> p.csTerm[3], true, false,
						median(pts, p -> p.cs[3]), null);
				addRow(table, "MCER_GMPE", "all", pts, p -> true, null, false, false, median(pts, p -> p.gmpe), null);
				String[] panels = {null, null, "(c) Final MCER, option 2", "(b) Final MCER, option 3"};
				for (int o : new int[] {3, 2}) {
					double[] ratios = pts.stream().filter(p -> p.raised[o]).mapToDouble(p -> p.fin[o]/p.gmpe).toArray();
					addRow(table, panels[o], "1. CS raised: CS term", pts, p -> p.raised[o], p -> p.csTerm[o], o == 3,
							o == 2, median(pts, p -> p.fin[o]), ratios);
					addRow(table, "", "2. GMPE kept", pts, p -> !p.raised[o], null, false, false, Double.NaN, null);
				}
			} else {
				addRow(table, "(a) MCER_CS, option 1", any, pts, p -> true, p -> p.csTerm[1], false, false,
						median(pts, p -> p.cs[1]), null);
				addRow(table, "(b) MCER_GMPE", any, pts, p -> true, p -> p.gmpeTerm, false, false,
						median(pts, p -> p.gmpe), null);
				addRow(table, "(c) MCER_CS, option 3", any, pts, p -> true, p -> p.csTerm[3], true, false,
						median(pts, p -> p.cs[3]), null);
				for (int o : options) {
					double[] ratios = pts.stream().filter(p -> p.raised[o]).mapToDouble(p -> p.fin[o]/p.gmpe).toArray();
					String panel = "("+(char)('c'+o)+") Final MCER, option "+o;
					addRow(table, panel, "1. CS raised: CS term", pts, p -> p.raised[o],
							p -> p.csTerm[o], o == 3, o == 2, median(pts, p -> p.fin[o]), ratios);
					addRow(table, "", "2. GMPE kept: GMPE term", pts, p -> !p.raised[o], p -> p.gmpeTerm,
							false, false, Double.NaN, null);
					addRow(table, "", "3. CS raised: GMPE term", pts, p -> p.raised[o],
							p -> p.gmpeTerm, false, false, Double.NaN, null);
				}
			}
			lines.addAll(table.build());
			lines.add("");
			lines.add("T = "+tStr(t)+" s, "+pts.size()+" coordinates.");
			lines.add("");
		}

		lines.add("## Notes on the data");
		lines.add("");
		String notes = "Each cell is a point sample at its centre; features smaller than the "+(float)spacing+"&deg; query "
				+ "spacing are not resolved. ";
		if (asce7_16)
			notes += (vs30Map ? "The Wills 2015 Vs30 changes only the GMPE side; " : "All coordinates use site class BC, "
					+ "which sets only the GMPE side; ")
					+ "the ASCE 7-16 tool's CyberShake side, including Det_LL, comes from fixed precomputed grids. The "
					+ "GMPE side's controlling term cannot be shown, because the tool's GMPE grids store only the "
					+ "MCE<sub>R</sub>. MCER_CS is the tool's precomputed value, which can differ slightly from the "
					+ "formula applied to the ingredient grids, so near a tie its controlling term is uncertain.";
		else
			notes += (vs30Map ? "Vs30 follows the Thompson 2022 map, so Prob_GMPE, Det_GMPE and Det_LL vary with it, "
					+ "interpolated between the two bounding site classes. " : "All coordinates use site class BC; other "
					+ "classes change Prob_GMPE, Det_GMPE and Det_LL. Det_LL is the ASCE 7-22 lower limit for the site "
					+ "class and does not vary across the region. ")
					+ "Where USGS gives no 84th-percentile spectrum ("+no84+" coordinates), Det_GMPE is unbounded, so "
					+ "option 1 and the GMPE side are set by their probabilistic terms.";
		lines.add(notes);
		lines.add("");

		lines.add("## Validation");
		lines.add("");
		if (asce7_16) {
			lines.add("Option 3 is the formula `Legacy_ASCE7_16_UGMS_WebToolCalc` implements, but the tool reads its "
					+ "result from its own precomputed CyberShake MCER grid, so the formula applied to the ingredient "
					+ "grids reproduces it only approximately. The merge must reproduce the tool's final values exactly; "
					+ "the report is only built if it does at every coordinate and period (tolerance "+TOL+" g):");
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Check (all coordinates and periods)", "Max. difference");
			table.addLine("Option 3 formula from the ingredients vs. the tool's CyberShake MCE<sub>R</sub>, max (reported "
					+ "only)", String.format("%.2g%%", 100*diffs[0]));
			table.addLine("Coordinate-periods where they agree within 0.5% (reported only)",
					pct(shareWithinHalfPct(p -> true))+"%");
			table.addLine("Coordinate-periods where using the formula instead would change raised / kept (reported only)",
					formulaFlips()+" of "+byPeriod.values().stream().mapToInt(List::size).sum());
			table.addLine("Final MCE<sub>R</sub> from the merge rule vs. the tool's final MCE<sub>R</sub>",
					String.format("%.1e g", diffs[2]));
		} else {
			lines.add("Option 1 is the formula `UGMS_WebToolCalc` implements, so evaluating it from the ingredients must "
					+ "reproduce the tool's own values at full precision. The report is only built if all three checks "
					+ "pass at every coordinate and period (tolerance "+TOL+" g):");
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Check (option 1, all coordinates and periods)", "Max. difference (g)");
			table.addLine("MCER_CS from the ingredients vs. the tool's CyberShake MCE<sub>R</sub>", String.format("%.1e", diffs[0]));
			table.addLine("MCER_GMPE from the ingredients vs. the tool's GMPE MCE<sub>R</sub>", String.format("%.1e", diffs[1]));
			table.addLine("Final MCE<sub>R</sub> from the merge rule vs. the tool's final MCE<sub>R</sub>",
					String.format("%.1e", diffs[2]));
		}
		lines.addAll(table.build());
		lines.add("");

		lines.addAll(reproduction(author));

		List<String> toc = new ArrayList<>(MarkdownUtils.buildTOC(lines, 2, 3));
		toc.add(0, "## Table of Contents");
		toc.add("");
		lines.addAll(tocIndex, toc);
		File reportDir = new File(outputDir, "report");
		MarkdownUtils.writeHTML(lines, new File(reportDir, "index.html"));
		// style the HTML like the PDF (MarkdownUtils copies a fresh GitHub markdown.css on every write)
		try (InputStream css = UGMS_GridReport.class.getResourceAsStream("UGMS_GridReport.css")) {
			Files.write(new File(reportDir, "markdown.css").toPath(), css.readAllBytes(), StandardOpenOption.APPEND);
		}
		// same content as PDF: Results and Notes start portrait pages; each period's maps, and its statistics, get a
		// landscape page
		File pdf = new File(reportDir, pdfName());
		MarkdownToPDF.write(lines, reportDir, pdf, (level, title) -> {
			if (level >= 4)
				return PageSize.LETTER.rotate();
			if (level == 2 && (title.equals("Results") || title.equals("Notes on the data")))
				return PageSize.LETTER;
			return null;
		});
		System.out.println("Wrote index.html and "+pdf.getName()+" to "+reportDir.getAbsolutePath());
	}

	/**
	 * Introduction to the per-period maps and statistics: what each map shows, a diagram of how the final MCER is
	 * formed, a worked example from the data, and how to read the statistics tables
	 */
	private List<String> mapsGuide() throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("For each period there is one page of "+(asce7_16 ? "three" : "six")+" maps and one statistics table. "
				+ "Each map cell is one coordinate.");
		lines.add("");
		lines.add("**What each map shows**");
		lines.add("");
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Panel", "Shows", "Cell colour");
		String finalColours = "**solid**: CS raised, coloured by the term that set MCER_CS · **hatched**: GMPE kept"
				+ (asce7_16 ? " (grey)" : ", coloured by the term that set MCER_GMPE");
		if (asce7_16) {
			table.addLine("(a) MCER_CS, option 3", "Which term sets the CyberShake-side MCE<sub>R</sub>",
					"blue Prob_CS · red Det_CS · green Det_LL");
			table.addLine("(b) (c) Final MCE<sub>R</sub>, options 3, 2", "Whether CyberShake raised the final value",
					finalColours);
		} else {
			table.addLine("(a) MCER_CS, option 1", "Which term sets the CyberShake-side MCE<sub>R</sub>",
					"blue Prob_CS · red Det_GMPE · green Det_LL");
			table.addLine("(b) MCER_GMPE", "Which term sets the GMPE-side MCE<sub>R</sub>; the same for every option",
					"blue Prob_GMPE · red Det_GMPE · green Det_LL");
			table.addLine("(c) MCER_CS, option 3", "Which term sets the CyberShake-side MCE<sub>R</sub>",
					"blue Prob_CS · red Det_CS · green Det_LL");
			table.addLine("(d) (e) (f) Final MCE<sub>R</sub>, options 1, 2, 3", "Whether CyberShake raised the final value",
					finalColours);
		}
		lines.addAll(table.build());
		lines.add("");

		lines.add("**From ingredients to the final MCE<sub>R</sub>**");
		lines.add("");
		MergeDiagram.write(resourcesDir(), "merge_diagram", weightsText(), asce7_16);
		lines.add("![From ingredients to the final MCER](resources/merge_diagram.png)");
		lines.add("");
		lines.addAll(workedExample());

		lines.add("**Reading the statistics tables**");
		lines.add("");
		if (asce7_16) {
			lines.add("* Row (a) counts every coordinate by the term that sets MCER_CS; the MCER_GMPE row gives its "
					+ "median only, as its term is not known.");
			lines.add("* **1. CS raised: CS term**: coordinates where MCER_CS is higher, so the final is the blend, "
					+ "counted by the term that set MCER_CS. Its Prob cell is where CyberShake simulation values reach "
					+ "the final.");
			lines.add("* **2. GMPE kept**: coordinates where the final equals MCER_GMPE (total only).");
			lines.add("* Rows 1 and 2 cover every coordinate once. Each cell is a count, with its share of all "
					+ "coordinates in brackets; \"-\" means the term does not exist for that row. Median MCER is over "
					+ "all coordinates; Final / GMPE is over the raised coordinates only, and shows how much the merge "
					+ "raises the GMPE value.");
		} else {
			lines.add("* Rows (a) to (c) count every coordinate by the term that sets that side's MCE<sub>R</sub>.");
			lines.add("* **1. CS raised: CS term**: coordinates where MCER_CS is higher, so the final is the blend, counted "
					+ "by the term that set MCER_CS. Its Prob cell is where CyberShake simulation values reach the final.");
			lines.add("* **2. GMPE kept: GMPE term**: coordinates where the final equals MCER_GMPE, counted by the term "
					+ "that set MCER_GMPE.");
			lines.add("* **3. CS raised: GMPE term**: the same coordinates as row 1, counted by the term that set "
					+ "MCER_GMPE, the other input to the blend.");
			lines.add("* Rows 1 and 2 cover every coordinate once; row 3 is a second view of row 1. Prob means Prob_CS in "
					+ "row 1 and Prob_GMPE in rows 2 and 3. Each cell is a count, with its share of all coordinates in "
					+ "brackets; \"-\" means the term does not exist for that row. Median MCER is over all coordinates; "
					+ "Final / GMPE is over the raised coordinates only, and shows how much the merge raises the GMPE "
					+ "value.");
		}
		lines.add("");
		return lines;
	}

	private static String weightsText() {
		List<String> ws = new ArrayList<>();
		for (double t : UGMS_GridCalc.CS_PERIODS)
			ws.add((float)CyberShakeWeightProvider.calcCyberShakeWeight(t)+"");
		return "w = "+String.join(", ", ws)+" at T = "+String.join(", ", tStrs(UGMS_GridCalc.CS_PERIODS))+" s";
	}

	/**
	 * One coordinate at the first period where the tool's capped MCER_CS still raised the final value, written out
	 */
	private List<String> workedExample() {
		double t = UGMS_GridCalc.CS_PERIODS[0];
		int o = toolOption;
		for (Point p : at(t)) {
			// a clear margin so the rounded arithmetic reads correctly, and for ASCE 7-16 a tool MCER_CS close to
			// the formula
			if (!p.raised[o] || p.csTerm[o] != Term.DET || p.cs[o] < 1.02*p.gmpe || asce7_16 && formulaDiff(p) >= 0.005)
				continue;
			double w = CyberShakeWeightProvider.calcCyberShakeWeight(t);
			String loc = String.format("%.2f&deg;N, %.2f&deg;W", p.loc.getLatitude(), -p.loc.getLongitude());
			String det = csTerms(o).get(1);
			String gmpe = asce7_16 ? "MCER_GMPE = "+g(p.gmpe)+" (the tool's GMPE grid)"
					: "MCER_GMPE = "+GMPE_TERMS.get(p.gmpeTerm.ordinal())+" "+g(p.gmpe);
			return List.of("Example, "+loc+", T = "+tStr(t)+" s, option "+o+": Prob_CS "+g(p.probCS)+" is above "+det+" "
					+ g(o == 3 ? p.detCS : p.detGMPE)+", so MCER_CS is capped at "
					+ (asce7_16 ? g(p.cs3Formula)+" (the tool's MCER grid gives "+g(p.cs[o])+")" : g(p.cs[o]))+". "+gmpe+". "
					+ g(p.cs[o])+" > "+g(p.gmpe)+", so CS raised: Final = "+g(p.cs[o])+"<sup>"+(float)w+"</sup> × "
					+ g(p.gmpe)+"<sup>"+(float)(1-w)+"</sup> = "+g(p.fin[o])+". It is counted in row 1 under "+det
					+ (asce7_16 ? "." : " and in row 3 under "+GMPE_TERMS.get(p.gmpeTerm.ordinal()).replace("_GMPE", "")+"."),
					"");
		}
		return List.of();
	}

	private static String g(double val) {
		return String.format("%.3f", val);
	}

	/**
	 * Commands that rebuild this report: the grid run's own arguments from run_config.json, then this report
	 */
	private List<String> reproduction(String author) {
		List<String> args = new ArrayList<>();
		if (config.has("args")) {
			config.getAsJsonArray("args").forEach(arg -> args.add(arg.getAsString()));
		} else {
			// run_config.json from before the arguments were recorded
			args.addAll(List.of("--data-dir", "<UGMS-opensha-backend>/data", "--output-dir", outputDir.getAbsolutePath(),
					"--spacing", config.get("spacing_deg").getAsString(), "--vs30", config.get("vs30").getAsString(),
					"--threads", config.get("threads").getAsString()));
		}
		String jar = "build/libs/opensha-cybershake-all.jar";
		String pkg = UGMS_GridCalc.class.getPackageName();
		List<String> calc = new ArrayList<>(List.of("java -Xmx12G -Djava.awt.headless=true -cp "+jar+" \\",
				"    "+pkg+".UGMS_GridCalc"));
		for (int i=0; i<args.size(); i++) {
			// one option and its value per line
			String line = "    "+shellArg(args.get(i));
			if (i+1 < args.size() && !args.get(i+1).startsWith("--"))
				line += " "+shellArg(args.get(++i));
			calc.set(calc.size()-1, calc.get(calc.size()-1)+" \\");
			calc.add(line);
		}

		List<String> lines = new ArrayList<>();
		lines.add("## Reproduction");
		lines.add("");
		lines.add("The results come from two command-line tools in opensha-cybershake, package `"+pkg+"`. "
				+ "`UGMS_GridCalc` runs `UGMS_WebToolCalc` at every coordinate and writes the ingredients; "
				+ "`UGMS_GridReport` builds this report from them. Each coordinate's raw USGS reply is saved under "
				+ "`coordinates/` in the output directory, and rerunning `UGMS_GridCalc` into the same directory reuses "
				+ "those replies and skips completed coordinates, so it reproduces these results exactly. A new output "
				+ "directory queries the USGS service again, which gives the same results only while the service's model "
				+ "is unchanged.");
		lines.add("");
		lines.add("From the opensha-cybershake directory"
				+ (config.has("started") ? " (grid run started "+config.get("started").getAsString().replace('T', ' ')+")" : "")
				+ ":");
		lines.add("");
		lines.add("```bash");
		lines.add("./gradlew fatJar");
		lines.addAll(calc);
		lines.add("java -Xmx4G -Djava.awt.headless=true -cp "+jar+" \\");
		lines.add("    "+pkg+".UGMS_GridReport \\");
		lines.add("    "+shellArg(outputDir.getAbsolutePath())+" "+shellArg(author));
		lines.add("```");
		lines.add("");
		lines.add("The report is written to `report/` in the output directory as `index.html` and `"
				+ pdfName()+"`.");
		lines.add("");
		return lines;
	}

	/** quotes a shell argument if needed, and shortens the home directory to ~ */
	private static String shellArg(String arg) {
		String home = System.getProperty("user.home");
		if (arg.startsWith(home+File.separator))
			arg = "~"+arg.substring(home.length());
		return arg.matches("[\\w@%+=:,./~-]+") ? arg : "\""+arg.replace("\"", "\\\"")+"\"";
	}

	private static String count(List<Point> pts, Predicate<Point> pred) {
		long n = pts.stream().filter(pred).count();
		return n+" ("+pct(100d*n/pts.size())+"%)";
	}

	/**
	 * One statistics row: the coordinates matching inRow, split by term. The deterministic term goes in the Det_CS
	 * column when detIsCS, else Det_GMPE; probOnly marks option 2, whose CyberShake side has only Prob_CS. A null term
	 * gives the total only.
	 */
	private static void addRow(TableBuilder table, String panel, String quantity, List<Point> pts,
			Predicate<Point> inRow, Function<Point, Term> term, boolean detIsCS, boolean probOnly, double medianMCER,
			double[] ratios) {
		if (term == null) {
			// no controlling term (ASCE 7-16 GMPE side): total only
			table.initNewLine().addColumns(panel, quantity, "-", "-", "-", "-", count(pts, inRow),
					Double.isNaN(medianMCER) ? "" : String.format("%.3f", medianMCER), "", "");
			table.finalizeLine();
			return;
		}
		String det = count(pts, p -> inRow.test(p) && term.apply(p) == Term.DET);
		String ll = count(pts, p -> inRow.test(p) && term.apply(p) == Term.DET_LL);
		table.initNewLine().addColumns(panel, quantity, count(pts, p -> inRow.test(p) && term.apply(p) == Term.PROB),
				probOnly || detIsCS ? "-" : det, probOnly || !detIsCS ? "-" : det, probOnly ? "-" : ll,
				count(pts, inRow), Double.isNaN(medianMCER) ? "" : String.format("%.3f", medianMCER));
		if (ratios == null)
			table.addColumns("", "");
		else
			table.addColumns(ratios.length == 0 ? "-" : String.format("%.2f", DataUtils.median(ratios)),
					ratios.length == 0 ? "-" : String.format("%.2f", DataUtils.max(ratios)));
		table.finalizeLine();
	}

	private static List<String> tStrs(double[] periods) {
		List<String> strs = new ArrayList<>();
		for (double t : periods)
			strs.add(tStr(t));
		return strs;
	}

	public static void main(String[] args) throws IOException {
		Preconditions.checkArgument(args.length == 1 || args.length == 2,
				"Usage: UGMS_GridReport <UGMS_GridCalc output dir> [author]");
		new UGMS_GridReport(new File(args[0])).write(args.length > 1 ? args[1] : System.getProperty("user.name"));
		System.exit(0);
	}
}
