package org.opensha.sha.cybershake.plot;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.ui.RectangleEdge;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.DataUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Simple tool for plotting comparison hazard curves from CSV files containing X,Y data.
 * This is useful for comparing different existing outputs without needing database access.
 */
public class ComparisonCurvePlotter {

    public static final int PLOT_WIDTH_DEFAULT = 800;
    public static final int PLOT_HEIGHT_DEFAULT = 600;

    private int plotWidth = PLOT_WIDTH_DEFAULT;
    private int plotHeight = PLOT_HEIGHT_DEFAULT;

    private HeadlessGraphPanel gp;

    // Default colors for curves
    private static final List<Color> DEFAULT_COLORS = Lists.newArrayList(
            Color.BLUE, Color.RED, Color.GREEN, Color.MAGENTA,
            Color.ORANGE, Color.CYAN, Color.PINK, new Color(139, 69, 19)); // brown

    public ComparisonCurvePlotter() {
        PlotPreferences plotPrefs = PlotPreferences.getDefault();
        plotPrefs.setAxisLabelFontSize(18);
        plotPrefs.setTickLabelFontSize(18);
        plotPrefs.setPlotLabelFontSize(20);
        gp = new HeadlessGraphPanel(plotPrefs);
        gp.setBackgroundColor(Color.WHITE);
        gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
    }

    /**
     * Read a CSV file with X,Y columns into a DiscretizedFunc
     *
     * @param csvFile CSV file with X,Y columns (can use space or tab delimiters, supports # comments)
     * @param skipLines number of header lines to skip (default 0)
     * @return the loaded curve
     * @throws IOException
     */
    public static DiscretizedFunc loadCurveFromCSV(File csvFile, int skipLines) throws IOException {
        Preconditions.checkArgument(csvFile.exists(), "CSV file does not exist: %s", csvFile);

        ArbitrarilyDiscretizedFunc curve = new ArbitrarilyDiscretizedFunc();
        
        // Read file line by line to handle whitespace delimiters and comments
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            int lineNumber = 0;
            int dataLineNumber = 0;
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                
                // Trim whitespace
                line = line.trim();
                
                // Skip empty lines and comments
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                
                // Skip header lines
                if (dataLineNumber < skipLines) {
                    dataLineNumber++;
                    continue;
                }
                
                dataLineNumber++;
                
                // Split by whitespace (tabs or spaces)
                String[] parts = line.split("\\s+");
                
                if (parts.length < 2) {
                    System.err.println("WARNING: Skipping line " + lineNumber + " in " + csvFile.getName() +
                            " (insufficient columns: " + parts.length + ")");
                    continue;
                }

                try {
                    double x = Double.parseDouble(parts[0].trim());
                    double y = Double.parseDouble(parts[1].trim());
                    curve.set(x, y);
                } catch (NumberFormatException e) {
                    System.err.println("WARNING: Skipping line " + lineNumber + " in " + csvFile.getName() +
                            " (invalid number format)");
                }
            }
        }

        Preconditions.checkState(curve.size() > 0, "No valid data points found in %s", csvFile);
        return curve;
    }

    /**
     * Plot multiple curves from CSV files
     *
     * @param csvFiles list of CSV files to load
     * @param curveNames names for each curve (must match number of CSV files)
     * @param title plot title
     * @param xAxisLabel x-axis label
     * @param yAxisLabel y-axis label
     * @param xLog use log scale for x-axis
     * @param yLog use log scale for y-axis
     * @param skipLines number of header lines to skip in CSV files
     * @return list of loaded curves
     * @throws IOException
     */
    public List<DiscretizedFunc> plotCurves(List<File> csvFiles, List<String> curveNames,
                                            String title, String xAxisLabel, String yAxisLabel,
                                            boolean xLog, boolean yLog, int skipLines) throws IOException {

        Preconditions.checkArgument(csvFiles.size() == curveNames.size(),
                "Number of CSV files (%s) must match number of names (%s)",
                csvFiles.size(), curveNames.size());
        Preconditions.checkArgument(!csvFiles.isEmpty(), "Must provide at least one CSV file");

        List<DiscretizedFunc> curves = new ArrayList<>();
        List<PlotCurveCharacterstics> chars = new ArrayList<>();

        // Load curves from CSV files
        for (int i = 0; i < csvFiles.size(); i++) {
            System.out.println("Loading curve from: " + csvFiles.get(i));
            DiscretizedFunc curve = loadCurveFromCSV(csvFiles.get(i), skipLines);
            curve.setName(curveNames.get(i));
            curves.add(curve);

            // Assign color and characteristics
            Color color = DEFAULT_COLORS.get(i % DEFAULT_COLORS.size());
            chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, null, 4f, color));
        }

        // Create plot spec
        PlotSpec spec = new PlotSpec(curves, chars, title, xAxisLabel, yAxisLabel);
        spec.setLegendVisible(true);
        spec.setLegendLocation(RectangleEdge.BOTTOM);

        // Draw plot
        gp.drawGraphPanel(spec, xLog, yLog, null, null);
        gp.setVisible(true);
        gp.validate();
        gp.repaint();

        return curves;
    }

    /**
     * Save the current plot as PDF
     */
    public void saveAsPDF(String filename) throws IOException {
        System.out.println("Saving PDF to: " + filename);
        gp.saveAsPDF(filename, plotWidth, plotHeight);
    }

    /**
     * Save the current plot as PNG
     */
    public void saveAsPNG(String filename) throws IOException {
        System.out.println("Saving PNG to: " + filename);
        ChartUtils.saveChartAsPNG(new File(filename), gp.getChartPanel().getChart(), plotWidth, plotHeight);
    }

    /**
     * Save the current plot as JPG
     */
    public void saveAsJPG(String filename) throws IOException {
        System.out.println("Saving JPG to: " + filename);
        ChartUtils.saveChartAsJPEG(new File(filename), gp.getChartPanel().getChart(), plotWidth, plotHeight);
    }

    public void setPlotSize(int width, int height) {
        this.plotWidth = width;
        this.plotHeight = height;
    }

    private static Options createOptions() {
        Options ops = new Options();

        Option csvFiles = new Option("csv", "csv-files", true,
                "Comma-separated list of CSV files to plot (each with X,Y columns)");
        csvFiles.setRequired(true);
        ops.addOption(csvFiles);

        Option names = new Option("n", "names", true,
                "Comma-separated list of names for each curve (must match number of CSV files)");
        names.setRequired(true);
        ops.addOption(names);

        Option title = new Option("title", "title", true, "Plot title");
        title.setRequired(true);
        ops.addOption(title);

        Option xAxis = new Option("x", "x-axis", true, "X-axis label");
        xAxis.setRequired(true);
        ops.addOption(xAxis);

        Option yAxis = new Option("y", "y-axis", true, "Y-axis label");
        yAxis.setRequired(true);
        ops.addOption(yAxis);

        Option output = new Option("o", "output", true, "Output file path (without extension)");
        output.setRequired(true);
        ops.addOption(output);

        Option type = new Option("t", "type", true,
                "Output type(s): PDF, PNG, JPG (comma-separated, default: PDF)");
        ops.addOption(type);

        Option xLog = new Option("xlog", "x-log", false, "Use log scale for x-axis");
        ops.addOption(xLog);

        Option yLog = new Option("ylog", "y-log", false, "Use log scale for y-axis");
        ops.addOption(yLog);

        Option skip = new Option("skip", "skip-lines", true,
                "Number of header lines to skip in CSV files (default: 0)");
        ops.addOption(skip);

        Option width = new Option("w", "width", true,
                "Plot width in pixels (default: " + PLOT_WIDTH_DEFAULT + ")");
        ops.addOption(width);

        Option height = new Option("h", "height", true,
                "Plot height in pixels (default: " + PLOT_HEIGHT_DEFAULT + ")");
        ops.addOption(height);

        Option help = new Option("?", "help", false, "Display this help message");
        ops.addOption(help);

        return ops;
    }

    public static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String appName = ClassUtils.getClassNameWithoutPackage(ComparisonCurvePlotter.class);
        formatter.printHelp(120, appName, null, options, null, true);
        System.exit(2);
    }

    public static void main(String[] args) {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();

        if (args.length == 0) {
            printHelp(options);
            return;
        }

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help") || cmd.hasOption("?")) {
                printHelp(options);
                return;
            }

            // Parse CSV files
            List<String> csvPaths = DataUtils.commaSplit(cmd.getOptionValue("csv-files"));
            List<File> csvFiles = new ArrayList<>();
            for (String path : csvPaths) {
                File f = new File(path.trim());
                if (!f.exists()) {
                    System.err.println("ERROR: CSV file does not exist: " + path);
                    System.exit(1);
                }
                csvFiles.add(f);
            }

            // Parse curve names
            List<String> curveNames = DataUtils.commaSplit(cmd.getOptionValue("names"));
            if (curveNames.size() != csvFiles.size()) {
                System.err.println("ERROR: Number of names (" + curveNames.size() +
                        ") must match number of CSV files (" + csvFiles.size() + ")");
                System.exit(1);
            }

            // Get other parameters
            String title = cmd.getOptionValue("title");
            String xAxisLabel = cmd.getOptionValue("x-axis");
            String yAxisLabel = cmd.getOptionValue("y-axis");
            String outputPath = cmd.getOptionValue("output");

            boolean xLog = cmd.hasOption("x-log");
            boolean yLog = cmd.hasOption("y-log");

            int skipLines = 0;
            if (cmd.hasOption("skip-lines")) {
                skipLines = Integer.parseInt(cmd.getOptionValue("skip-lines"));
            }

            // Parse output types
            List<PlotType> types = new ArrayList<>();
            if (cmd.hasOption("type")) {
                types = PlotType.fromExtensions(DataUtils.commaSplit(cmd.getOptionValue("type")));
            } else {
                types.add(PlotType.PDF);
            }

            // Create plotter
            ComparisonCurvePlotter plotter = new ComparisonCurvePlotter();

            if (cmd.hasOption("width") || cmd.hasOption("height")) {
                int width = cmd.hasOption("width") ?
                        Integer.parseInt(cmd.getOptionValue("width")) : PLOT_WIDTH_DEFAULT;
                int height = cmd.hasOption("height") ?
                        Integer.parseInt(cmd.getOptionValue("height")) : PLOT_HEIGHT_DEFAULT;
                plotter.setPlotSize(width, height);
            }

            // Plot curves
            System.out.println("Plotting " + csvFiles.size() + " curves...");
            plotter.plotCurves(csvFiles, curveNames, title, xAxisLabel, yAxisLabel,
                    xLog, yLog, skipLines);

            // Save plots
            for (PlotType type : types) {
                String filename = outputPath + "." + type.getExtension();
                switch (type) {
                    case PDF:
                        plotter.saveAsPDF(filename);
                        break;
                    case PNG:
                        plotter.saveAsPNG(filename);
                        break;
                    case JPG:
                    case JPEG:
                        plotter.saveAsJPG(filename);
                        break;
                    default:
                        System.err.println("Unsupported plot type: " + type);
                }
            }

            System.out.println("Done!");

        } catch (ParseException e) {
            System.err.println("Error parsing command line: " + e.getMessage());
            printHelp(options);
        } catch (IOException e) {
            System.err.println("Error reading/writing files: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
