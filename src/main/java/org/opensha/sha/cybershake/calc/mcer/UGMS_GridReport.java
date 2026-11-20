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
	}

	private static final int[] OPTIONS = {1, 2, 3};
	private static final Color[] OPTION_COLORS = {new Color(36, 86, 90), new Color(184, 100, 26), new Color(106, 76, 156)};
	private static final double TOL = 1e-9;
	private static final String PDF_NAME = "mcer_report.pdf";
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
	 * each option (index 1-3; index 0 unused)
	 */
	static class Point {
		final Location loc;
		final double period;
		final boolean detGMPEProvided;
		final double detLL, probCS, detGMPE;
		final double gmpe;
		final Term gmpeTerm;
		final double[] cs = new double[4], fin = new double[4];
		final Term[] csTerm = new Term[4];
		final boolean[] raised = new boolean[4];
		final double toolCS, toolGMPE, toolFinal;

		Point(List<String> row) {
			loc = new Location(Double.parseDouble(row.get(0)), Double.parseDouble(row.get(1)));
			period = Double.parseDouble(row.get(2));
			double pc = Double.parseDouble(row.get(3)), dc = Double.parseDouble(row.get(4));
			double pg = Double.parseDouble(row.get(5)), dg = Double.parseDouble(row.get(6));
			detGMPEProvided = Boolean.parseBoolean(row.get(7));
			double ll = Double.parseDouble(row.get(8));
			detLL = ll;
			probCS = pc;
			detGMPE = dg;
			toolCS = Double.parseDouble(row.get(9));
			toolGMPE = Double.parseDouble(row.get(10));
			toolFinal = Double.parseDouble(row.get(11));

			gmpe = MCErCalcUtils.calcMCER(dg, pg, ll);
			gmpeTerm = control(pg, dg, ll);
			cs[1] = MCErCalcUtils.calcMCER(dg, pc, ll);
			csTerm[1] = control(pc, dg, ll);
			cs[2] = pc;
			csTerm[2] = Term.PROB;
			cs[3] = MCErCalcUtils.calcMCER(dc, pc, ll);
			csTerm[3] = control(pc, dc, ll);
			for (int o : OPTIONS) {
				fin[o] = MCERDataProductsCalc.calcFinalMCER(cs[o], gmpe, period);
				raised[o] = cs[o] > gmpe; // calcFinalMCER keeps the GMPE value when it is >= CyberShake
			}
		}
	}

	private final File outputDir;
	private final JsonObject config;
	private final double spacing;
	private final Region region;
	private final Map<Double, List<Point>> byPeriod = new TreeMap<>();
	private final int numFailed;
	private final List<? extends FaultSection> faults = FaultModels.FM3_1.getFaultSections();
	// Esri basemap layers over mapRegion, or null when the tiles can't be fetched (drawn land and labels instead)
	private BufferedImage basemap, basemapLabels;

	private UGMS_GridReport(File outputDir) throws IOException {
		this.outputDir = outputDir;
		config = JsonParser.parseString(Files.readString(new File(outputDir, "run_config.json").toPath()))
				.getAsJsonObject();
		spacing = config.get("spacing_deg").getAsDouble();
		region = new Region(UGMS_GridCalc.REGION_BORDER, BorderType.MERCATOR_LINEAR);
		CSVFile<String> csv = CSVFile.readFile(new File(outputDir, "ingredients.csv"), true);
		Preconditions.checkState(csv.getLine(0).equals(List.of(UGMS_GridCalc.INGREDIENT_HEADER)),
				"Unexpected ingredients.csv header; rerun UGMS_GridCalc");
		for (int r=1; r<csv.getNumRows(); r++) {
			Point p = new Point(csv.getLine(r));
			byPeriod.computeIfAbsent(p.period, k -> new ArrayList<>()).add(p);
		}
		for (double t : UGMS_GridCalc.CS_PERIODS) {
			Preconditions.checkState(byPeriod.containsKey(t), "ingredients.csv has no rows at %s s", t);
			// Det_LL depends on site class, not location, so it is reported as one value per period
			double ll = byPeriod.get(t).get(0).detLL;
			Preconditions.checkState(byPeriod.get(t).stream().allMatch(p -> p.detLL == ll),
					"Det_LL varies by coordinate at %s s", t);
		}
		CSVFile<String> coords = CSVFile.readFile(new File(outputDir, "coordinates.csv"), true);
		int failed = 0;
		for (int r=1; r<coords.getNumRows(); r++)
			if (!coords.get(r, 2).equals("ok"))
				failed++;
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
	 * Max differences between option 1 evaluated here and the tool's own MCERs (CyberShake, GMPE, final)
	 */
	private double[] validate() {
		double[] diffs = new double[3];
		for (List<Point> pts : byPeriod.values()) {
			for (Point p : pts) {
				diffs[0] = Math.max(diffs[0], Math.abs(p.cs[1] - p.toolCS));
				diffs[1] = Math.max(diffs[1], Math.abs(p.gmpe - p.toolGMPE));
				diffs[2] = Math.max(diffs[2], Math.abs(p.fin[1] - p.toolFinal));
			}
		}
		Preconditions.checkState(diffs[0] < TOL && diffs[1] < TOL && diffs[2] < TOL,
				"Option 1 doesn't reproduce the tool's MCERs (max differences CS %s, GMPE %s, final %s)",
				diffs[0], diffs[1], diffs[2]);
		return diffs;
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
		for (Term t : Term.values()) {
			fills.add(t.hatched());
			legend.add("GMPE kept, "+GMPE_TERMS.get(t.ordinal()));
		}
		map(prefix, pts, p -> p.raised[o] ? p.csTerm[o].ordinal() : 3 + p.gmpeTerm.ordinal(), fills, legend);
	}

	private void overviewChart(String prefix, String title, Predicate<Point>[] preds) throws IOException {
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		PlotLineType[] lines = {PlotLineType.SOLID, PlotLineType.DASHED, PlotLineType.DOTTED};
		PlotSymbol[] symbols = {PlotSymbol.FILLED_CIRCLE, PlotSymbol.FILLED_SQUARE, PlotSymbol.FILLED_TRIANGLE};
		for (int o : OPTIONS) {
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

	private void write(String author) throws IOException {
		double[] diffs = validate();
		loadBasemap();
		Preconditions.checkState(resourcesDir().exists() || resourcesDir().mkdirs());
		double[] periods = UGMS_GridCalc.CS_PERIODS;
		List<Point> first = at(periods[0]);
		int n = first.size();
		long no84 = first.stream().filter(p -> !p.detGMPEProvided).count();
		List<String> weights = new ArrayList<>(), llVals = new ArrayList<>();
		for (double t : periods) {
			weights.add((float)CyberShakeWeightProvider.calcCyberShakeWeight(t)+"");
			llVals.add((float)at(t).get(0).detLL+"");
		}

		List<String> lines = new ArrayList<>();
		lines.add("# MCE<sub>R</sub> controlling terms: CyberShake 15.4 with ASCE 7-22");
		lines.add("");
		lines.add(author+", "+LocalDate.now().format(DateTimeFormatter.ofPattern("MMMM d, yyyy")));
		lines.add("");
		int tocIndex = lines.size();

		lines.add("## Purpose");
		lines.add("");
		lines.add("This document presents, at each queried coordinate and CyberShake period, which term sets the "
				+ "CyberShake-side and GMPE-side MCE<sub>R</sub> and the final MCE<sub>R</sub> after the two are merged, "
				+ "under three candidate formulas for the CyberShake side. [Results](#results) gives summary tables and "
				+ "one section of maps per period; [Validation](#validation) records the checks against the tool.");
		lines.add("");

		lines.add("## Data and methods");
		lines.add("");
		lines.add("### Coordinates");
		lines.add("");
		lines.add("We queried the USGS ASCE 7-22 service and the CyberShake 15.4 grid files at every multiple of "
				+ (float)spacing+"&deg; in latitude and longitude inside the v18.4 web-tool region (the dashed outline in "
				+ "every map), "+n+" coordinates in all. This query spacing is our choice; it is not the resolution of "
				+ "the underlying data (the CyberShake grids are at 0.002&deg;). Every coordinate uses Vs30 = "
				+ (int)config.get("vs30").getAsDouble()+" m/s, which is exactly ASCE 7-22 site class BC, so each needs "
				+ "one USGS service call and no Vs30 interpolation. Each coordinate was run through `UGMS_WebToolCalc` "
				+ "(ASCE 7-22 with CyberShake 15.4); "
				+ (numFailed > 0 ? numFailed+" coordinates failed and are excluded." : "every coordinate completed.")
				+ " At "+no84+" coordinates the USGS service returns no 84th-percentile spectrum; it applies no "
				+ "deterministic cap there, so its MCE<sub>R</sub> equals the probabilistic spectrum. We treat "
				+ "Det_GMPE as unbounded at those coordinates.");
		lines.add("");
		lines.add("### Spectral ingredients");
		lines.add("");
		lines.add("All are evaluated at the CyberShake periods "+String.join(", ", tStrs(periods))+" s, where the two "
				+ "sides are merged.");
		lines.add("");
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Term", "Meaning", "Source");
		table.addLine("Prob_CS", "CyberShake risk-targeted probabilistic",
				"CyberShake 15.4 grid (0.002&deg;), as used by the tool");
		table.addLine("Det_CS", "CyberShake 84th-percentile deterministic", "CyberShake 15.4 deterministic grid (0.002&deg;)");
		table.addLine("Prob_GMPE", "GMPE risk-targeted probabilistic", "USGS ASCE 7-22 service, `riskTargetedSpectrum`");
		table.addLine("Det_GMPE", "GMPE 84th-percentile deterministic", "USGS service, `eightyFourthSpectrum`");
		table.addLine("Det_LL", "Deterministic lower limit", "USGS service, `metadata.sadFloor` (class BC)");
		lines.addAll(table.build());
		lines.add("");
		lines.add("Det_LL depends on site class but not location; at class BC it is "+String.join(", ", llVals)
				+ " g at the six periods.");
		lines.add("");
		lines.add("### Per-side MCER and the three options");
		lines.add("");
		lines.add("Each side's MCE<sub>R</sub> is the smaller of its probabilistic spectrum and the larger of a "
				+ "deterministic spectrum and the lower limit (`MCErCalcUtils.calcMCER`). The GMPE side is the same in "
				+ "every case, and the three candidate formulas for the CyberShake side are:");
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.addLine("", "Formula");
		table.addLine("GMPE side (every option)", "`MCER_GMPE = min[Prob_GMPE, max(Det_GMPE, Det_LL)]`");
		table.addLine("Option 1 (current ASCE 7-22 tool)", "`MCER_CS = min[Prob_CS, max(Det_GMPE, Det_LL)]`");
		table.addLine("Option 2", "`MCER_CS = Prob_CS`");
		table.addLine("Option 3", "`MCER_CS = min[Prob_CS, max(Det_CS, Det_LL)]`");
		lines.addAll(table.build());
		lines.add("");
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
				+ "cell means the GMPE value was kept, coloured by the GMPE term that set MCER_GMPE. Solid blue "
				+ "therefore marks the coordinates where CyberShake simulation values reach the final MCE<sub>R</sub>. "
				+ "Thin grey lines are UCERF3 FM3.1 fault traces, for reference. Basemap: "
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
		for (int o : OPTIONS) {
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

		lines.add("**Controlling term of each side's MCE<sub>R</sub>** (% of coordinates). Under option 2, MCER_CS = "
				+ "Prob_CS at every coordinate.");
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.initNewLine().addColumn("T (s)");
		for (String side : List.of("Option 1 CS", "GMPE", "Option 3 CS"))
			for (String name : side.equals("GMPE") ? GMPE_TERMS : side.startsWith("Option 1") ? CS_OPT1_TERMS
					: CS_OPT3_TERMS)
				table.addColumn(side+": "+name);
		table.finalizeLine();
		for (double t : periods) {
			table.initNewLine().addColumn(tStr(t));
			for (ToIntFunction<Point> side : List.<ToIntFunction<Point>>of(p -> p.csTerm[1].ordinal(),
					p -> p.gmpeTerm.ordinal(), p -> p.csTerm[3].ordinal()))
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
		for (int o : OPTIONS)
			table.addColumns("Opt "+o+": GMPE kept", "Opt "+o+": "+csTerms(o).get(0),
					"Opt "+o+": "+(o == 2 ? "n/a" : csTerms(o).get(1)), "Opt "+o+": "+(o == 2 ? "n/a" : csTerms(o).get(2)));
		table.finalizeLine();
		for (double t : periods) {
			table.initNewLine().addColumn(tStr(t));
			for (int o : OPTIONS) {
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
			sideMap(tag+"_cs_opt1", pts, p -> p.csTerm[1].ordinal(), CS_OPT1_TERMS);
			sideMap(tag+"_gmpe", pts, p -> p.gmpeTerm.ordinal(), GMPE_TERMS);
			sideMap(tag+"_cs_opt3", pts, p -> p.csTerm[3].ordinal(), CS_OPT3_TERMS);
			for (int o : OPTIONS)
				finalMap(tag+"_final_opt"+o, pts, o);
			table = MarkdownUtils.tableBuilder();
			table.addLine("(a) MCER_CS, option 1", "(b) MCER_GMPE (all options)", "(c) MCER_CS, option 3");
			table.addLine("![a](resources/"+tag+"_cs_opt1.png)", "![b](resources/"+tag+"_gmpe.png)",
					"![c](resources/"+tag+"_cs_opt3.png)");
			table.addLine("(d) Final MCE<sub>R</sub>, option 1", "(e) Final MCE<sub>R</sub>, option 2",
					"(f) Final MCE<sub>R</sub>, option 3");
			table.addLine("![d](resources/"+tag+"_final_opt1.png)", "![e](resources/"+tag+"_final_opt2.png)",
					"![f](resources/"+tag+"_final_opt3.png)");
			lines.addAll(table.build());
			lines.add("");

			lines.add("##### T = "+tStr(t)+" s: statistics");
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Panel", "Coordinates", "Prob", "Det_GMPE", "Det_CS", "Det_LL", "Total", "Median MCER (g)",
					"Final / GMPE, raised: median", "max");
			String any = "all: term that sets it";
			addRow(table, "(a) MCER_CS, option 1", any, pts, p -> true, p -> p.csTerm[1], false, false,
					median(pts, p -> p.cs[1]), null);
			addRow(table, "(b) MCER_GMPE", any, pts, p -> true, p -> p.gmpeTerm, false, false,
					median(pts, p -> p.gmpe), null);
			addRow(table, "(c) MCER_CS, option 3", any, pts, p -> true, p -> p.csTerm[3], true, false,
					median(pts, p -> p.cs[3]), null);
			for (int o : OPTIONS) {
				double[] ratios = pts.stream().filter(p -> p.raised[o]).mapToDouble(p -> p.fin[o]/p.gmpe).toArray();
				String panel = "("+(char)('c'+o)+") Final MCER, option "+o;
				addRow(table, panel, "1. CS raised: CS term", pts, p -> p.raised[o],
						p -> p.csTerm[o], o == 3, o == 2, median(pts, p -> p.fin[o]), ratios);
				addRow(table, "", "2. GMPE kept: GMPE term", pts, p -> !p.raised[o], p -> p.gmpeTerm,
						false, false, Double.NaN, null);
				addRow(table, "", "3. CS raised: GMPE term", pts, p -> p.raised[o],
						p -> p.gmpeTerm, false, false, Double.NaN, null);
			}
			lines.addAll(table.build());
			lines.add("");
			lines.add("T = "+tStr(t)+" s, "+pts.size()+" coordinates.");
			lines.add("");
		}

		lines.add("## Notes on the data");
		lines.add("");
		lines.add("Each cell is a point sample at its centre; features smaller than the "+(float)spacing+"&deg; query "
				+ "spacing are not resolved. All coordinates use site class BC; other classes change Prob_GMPE, "
				+ "Det_GMPE and Det_LL. Det_LL is the ASCE 7-22 lower limit for the site class and does not vary across "
				+ "the region. Where USGS gives no 84th-percentile spectrum ("+no84+" coordinates), Det_GMPE is "
				+ "unbounded, so option 1 and the GMPE side are set by their probabilistic terms.");
		lines.add("");

		lines.add("## Validation");
		lines.add("");
		lines.add("Option 1 is the formula `UGMS_WebToolCalc` implements, so evaluating it from the ingredients must "
				+ "reproduce the tool's own values at full precision. The report is only built if all three checks pass "
				+ "at every coordinate and period (tolerance "+TOL+" g):");
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.addLine("Check (option 1, all coordinates and periods)", "Max. difference (g)");
		table.addLine("MCER_CS from the ingredients vs. the tool's CyberShake MCE<sub>R</sub>", String.format("%.1e", diffs[0]));
		table.addLine("MCER_GMPE from the ingredients vs. the tool's GMPE MCE<sub>R</sub>", String.format("%.1e", diffs[1]));
		table.addLine("Final MCE<sub>R</sub> from the merge rule vs. the tool's final MCE<sub>R</sub>",
				String.format("%.1e", diffs[2]));
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
		File pdf = new File(reportDir, PDF_NAME);
		MarkdownToPDF.write(lines, reportDir, pdf, (level, title) -> {
			if (level >= 4)
				return PageSize.LETTER.rotate();
			if (level == 2 && (title.equals("Results") || title.equals("Notes on the data")))
				return PageSize.LETTER;
			return null;
		});
		System.out.println("Wrote index.html and "+PDF_NAME+" to "+reportDir.getAbsolutePath());
	}

	/**
	 * Introduction to the per-period maps and statistics: what each map shows, a diagram of how the final MCER is
	 * formed, a worked example from the data, and how to read the statistics tables
	 */
	private List<String> mapsGuide() throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("For each period there is one page of six maps and one statistics table. Each map cell is one "
				+ "coordinate.");
		lines.add("");
		lines.add("**What each map shows**");
		lines.add("");
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Panel", "Shows", "Cell colour");
		table.addLine("(a) MCER_CS, option 1", "Which term sets the CyberShake-side MCE<sub>R</sub>",
				"blue Prob_CS · red Det_GMPE · green Det_LL");
		table.addLine("(b) MCER_GMPE", "Which term sets the GMPE-side MCE<sub>R</sub>; the same for every option",
				"blue Prob_GMPE · red Det_GMPE · green Det_LL");
		table.addLine("(c) MCER_CS, option 3", "Which term sets the CyberShake-side MCE<sub>R</sub>",
				"blue Prob_CS · red Det_CS · green Det_LL");
		table.addLine("(d) (e) (f) Final MCE<sub>R</sub>, options 1, 2, 3", "Whether CyberShake raised the final value",
				"**solid**: CS raised, coloured by the term that set MCER_CS · **hatched**: GMPE kept, coloured by the "
				+ "term that set MCER_GMPE");
		lines.addAll(table.build());
		lines.add("");

		lines.add("**From ingredients to the final MCE<sub>R</sub>**");
		lines.add("");
		MergeDiagram.write(resourcesDir(), "merge_diagram", weightsText());
		lines.add("![From ingredients to the final MCER](resources/merge_diagram.png)");
		lines.add("");
		lines.addAll(workedExample());

		lines.add("**Reading the statistics tables**");
		lines.add("");
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
	 * One coordinate at the first period where option 1's capped MCER_CS still raised the final value, written out
	 */
	private List<String> workedExample() {
		double t = UGMS_GridCalc.CS_PERIODS[0];
		for (Point p : at(t)) {
			if (!p.raised[1] || p.csTerm[1] != Term.DET)
				continue;
			double w = CyberShakeWeightProvider.calcCyberShakeWeight(t);
			String loc = String.format("%.2f&deg;N, %.2f&deg;W", p.loc.getLatitude(), -p.loc.getLongitude());
			return List.of("Example, "+loc+", T = "+tStr(t)+" s, option 1: Prob_CS "+g(p.probCS)+" is above Det_GMPE "
					+ g(p.detGMPE)+", so MCER_CS is capped at "+g(p.cs[1])+". MCER_GMPE = Prob_GMPE "+g(p.gmpe)+". "
					+ g(p.cs[1])+" > "+g(p.gmpe)+", so CS raised: Final = "+g(p.cs[1])+"<sup>"+(float)w+"</sup> × "
					+ g(p.gmpe)+"<sup>"+(float)(1-w)+"</sup> = "+g(p.fin[1])+". It is counted in row 1 under Det_GMPE "
					+ "and in row 3 under Prob.", "");
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
				+ PDF_NAME+"`.");
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
	 * column when detIsCS, else Det_GMPE; probOnly marks option 2, whose CyberShake side has only Prob_CS.
	 */
	private static void addRow(TableBuilder table, String panel, String quantity, List<Point> pts,
			Predicate<Point> inRow, Function<Point, Term> term, boolean detIsCS, boolean probOnly, double medianMCER,
			double[] ratios) {
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
