package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.calc.mcer.DeterministicResult;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.cybershake.plot.PlotType;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DeterministicResultPlotter {
	
	private static final PlotType PLOT_TYPE_DEFAULT = PlotType.PNG;
	private List<PlotType> plotTypes;
	
	private CyberShakeComponent comp;
	
	private Map<CyberShakeComponent, Map<String, DiscretizedFunc>> csSpectrumMaps;
	private Map<CyberShakeComponent, Map<String, List<DiscretizedFunc>>> gmpeSpectrumMaps;
	
	private boolean velPlot = false;
	
	private File outputDir;
	
	public DeterministicResultPlotter(CommandLine cmd) throws FileNotFoundException, IOException {
		velPlot = cmd.hasOption("velocities");
		outputDir = new File(cmd.getOptionValue("output-dir"));
		Preconditions.checkArgument((outputDir.exists() && outputDir.isDirectory()) || outputDir.mkdir(),
				"Output dir does not exist and could not be created");
		
		if (cmd.hasOption("t")) {
			String typeStr = cmd.getOptionValue("t");
			plotTypes = PlotType.fromExtensions(DataUtils.commaSplit(typeStr));
		} else {
			plotTypes = new ArrayList<PlotType>();
			plotTypes.add(PLOT_TYPE_DEFAULT);
		}
		
		comp = CybershakeIM.fromShortName(cmd.getOptionValue("component"), CyberShakeComponent.class);
		
		File inputFile = new File(cmd.getOptionValue("input-file"));
		Preconditions.checkArgument(inputFile.exists(), "Input file does not exist: "+inputFile.getAbsolutePath());
		
		csSpectrumMaps = Maps.newHashMap();
		gmpeSpectrumMaps = Maps.newHashMap();
		
		loadData(inputFile, comp, csSpectrumMaps, gmpeSpectrumMaps);
	}
	
	static void loadData(File inputFile, CyberShakeComponent comp,
			Map<CyberShakeComponent, Map<String, DiscretizedFunc>> csSpectrumMaps,
			Map<CyberShakeComponent, Map<String, List<DiscretizedFunc>>> gmpeSpectrumMaps)
					throws FileNotFoundException, IOException {
		HSSFWorkbook wb;
		try {
			POIFSFileSystem fs = new POIFSFileSystem(new FileInputStream(inputFile));
			wb = new HSSFWorkbook(fs);
		} catch (Exception e1) {
			System.err.println("Couldn't load input file. Make sure it's an xls file and NOT an xlsx file.");
			throw ExceptionUtils.asRuntimeException(e1);
		}
		
		int colsPer = 4;
		int headerCols = 1;
		int csValCol = headerCols+colsPer-1;
		
		for (int sheetIndex=0; sheetIndex<wb.getNumberOfSheets(); sheetIndex++) {
			HSSFSheet sheet = wb.getSheetAt(sheetIndex);
			String siteName = sheet.getSheetName();
			
			FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();
			
			System.out.println("Loading values for "+siteName);
			
			Map<String, DiscretizedFunc> csSpectrumMap = Maps.newHashMap();
			csSpectrumMaps.put(comp, csSpectrumMap);
			Map<String, List<DiscretizedFunc>> gmpeSpectrumMap = null;
			
			int col = csValCol;
			
			HSSFRow header = sheet.getRow(0);
			
			int gmpeCount = 0;
			
			while (col <= header.getLastCellNum()) {
				if (col > csValCol && gmpeSpectrumMaps == null)
					break;
				String cellName = header.getCell(col).getStringCellValue();
				String calcName = cellName;
				Preconditions.checkState(calcName.toLowerCase().contains("value"),
						"Expected Value cell at column "+col+" (0 based index)");
				calcName = calcName.substring(0, calcName.toLowerCase().indexOf("value")).trim();
				
				if (col == csValCol)
					Preconditions.checkState(calcName.equals("CyberShake"), "Expected CyberShake value at column "
							+col+" (0 based index), got "+cellName);
				
				ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc(calcName);
				
				for (int i=1; i<=sheet.getLastRowNum(); i++) {
					HSSFRow row = sheet.getRow(i);
					HSSFCell valCell = row.getCell(col);
					if (valCell == null)
						continue;
					double val;
					if (valCell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
						val = valCell.getNumericCellValue();
					} else if (valCell.getCellType() == HSSFCell.CELL_TYPE_FORMULA) {
						val = evaluator.evaluate(valCell).getNumberValue();
//						System.out.println("Evaluated: "+valCell.getCellFormula()+" = "+val);
					} else {
						try {
							val = Double.parseDouble(valCell.getStringCellValue());
						} catch (Exception e) {
							continue;
						}
					}
					double period = row.getCell(0).getNumericCellValue();
					func.set(period, val);
				}
				
				Preconditions.checkState(func.size() > 2, "Too few points for func at "+cellName
						+", lastRowNum: "+sheet.getLastRowNum());
				
				if (col == csValCol) {
					csSpectrumMap.put(siteName, func);
				} else {
					if (gmpeSpectrumMap == null) {
						gmpeSpectrumMap = Maps.newHashMap();
						gmpeSpectrumMaps.put(comp, gmpeSpectrumMap);
						gmpeSpectrumMap.put(siteName, new ArrayList<DiscretizedFunc>());
					}
					List<DiscretizedFunc> gmpeFuncs = gmpeSpectrumMap.get(comp);
					gmpeFuncs.add(func);
					gmpeCount++;
				}
				
				col += colsPer;
			}
			
			System.out.println("Loaded CyberShake & "+gmpeCount+" GMPEs for "+siteName);
		}
	}
	
	private void plot() throws IOException {
		for (CyberShakeComponent comp : csSpectrumMaps.keySet())
			plot(comp, csSpectrumMaps.get(comp), gmpeSpectrumMaps.get(comp), plotTypes, velPlot, outputDir);
	}
	
	static void plot(CyberShakeComponent comp, Map<String, DiscretizedFunc> csSpectrumMap,
			Map<String, List<DiscretizedFunc>> gmpeSpectrumMap,
			List<PlotType> plotTypes, boolean velPlot, File outputDir) throws IOException {
		for (String siteName : csSpectrumMap.keySet()) {
			DiscretizedFunc csSpectrum = csSpectrumMap.get(siteName);
			List<DiscretizedFunc> gmpeSpectrums = gmpeSpectrumMap.get(siteName);
			
			String namePrefix = siteName+"_deterministic";
			String dateStr = ProbabilisticResultPlotter.dateFormat.format(new Date());
			PlotSpec spec = null;
			PlotSpec velSpec = null;
			
			for (PlotType type : plotTypes) {
				String name = namePrefix+"_"+comp.getShortName()+"_";
				if (velPlot)
					name += "vel_";
				name += dateStr+"."+type.getExtension();
				File outputFile = new File(outputDir, name);
				switch (type) {
				case PDF:
					if (spec == null) {
						spec = ProbabilisticResultPlotter.getSpectrumPlot(siteName, comp, csSpectrum, gmpeSpectrums, "Determinisic", "(g)");
						if (velPlot)
							velSpec = ProbabilisticResultPlotter.getSpectrumPlot(siteName, comp, MCErCalcUtils.saToPsuedoVel(csSpectrum),
									ProbabilisticResultPlotter.sasToPsuedoVel(gmpeSpectrums), "Determinisic PSV", "(cm/sec)");
					}
					ProbabilisticResultPlotter.writeSpec(spec, outputFile, type,
							ProbabilisticResultPlotter.xLog, ProbabilisticResultPlotter.yLog, ProbabilisticResultPlotter.xRangeSA, ProbabilisticResultPlotter.yRangeSA);
					if (velPlot)
						ProbabilisticResultPlotter.writeSpec(velSpec, outputFile, type,
								ProbabilisticResultPlotter.xLog, ProbabilisticResultPlotter.yLog, ProbabilisticResultPlotter.xRangeVel, ProbabilisticResultPlotter.yRangeVel);
					break;
				case PNG:
					if (spec == null) {
						spec = ProbabilisticResultPlotter.getSpectrumPlot(siteName, comp, csSpectrum, gmpeSpectrums, "Determinisic", "(g)");
						if (velPlot)
							velSpec = ProbabilisticResultPlotter.getSpectrumPlot(siteName, comp, MCErCalcUtils.saToPsuedoVel(csSpectrum),
									ProbabilisticResultPlotter.sasToPsuedoVel(gmpeSpectrums), "Determinisic PSV", "(cm/sec)");
					}
					ProbabilisticResultPlotter.writeSpec(spec, outputFile, type,
							ProbabilisticResultPlotter.xLog, ProbabilisticResultPlotter.yLog, ProbabilisticResultPlotter.xRangeSA, ProbabilisticResultPlotter.yRangeSA);
					if (velPlot)
						ProbabilisticResultPlotter.writeSpec(velSpec, outputFile, type,
								ProbabilisticResultPlotter.xLog, ProbabilisticResultPlotter.yLog, ProbabilisticResultPlotter.xRangeVel, ProbabilisticResultPlotter.yRangeVel);
					break;

				default:
					throw new IllegalArgumentException("Unsupported plot type: "+type.getExtension());
				}
			}
		}
	}
	
	static void writeCSV(List<Double> periods, List<DeterministicResult> csDeterms,
			List<? extends ScalarIMR> gmpes, List<List<DeterministicResult>> gmpeDeterms,
			File outputFile, boolean vel) throws IOException {
		writeCSV(periods, csDeterms, null, gmpes, gmpeDeterms, outputFile, vel);
	}
	
	static void writeCSV(List<Double> periods, List<DeterministicResult> csDeterms, List<DeterministicResult> avgDeterms,
			List<? extends ScalarIMR> gmpes, List<List<DeterministicResult>> gmpeDeterms,
			File outputFile, boolean vel) throws IOException {
		String units = "(g)";
		if (vel) {
			units = "(cm/s)";
			csDeterms = DeterministicResult.getPsuedoVels(csDeterms, periods);
			if (gmpeDeterms != null) {
				List<List<DeterministicResult>> velGMPEDeterms = Lists.newArrayList();
				for (List<DeterministicResult> gmpeDeterm : gmpeDeterms)
					velGMPEDeterms.add(DeterministicResult.getPsuedoVels(gmpeDeterm, periods));
				gmpeDeterms = velGMPEDeterms;
			}
			if (avgDeterms != null)
				avgDeterms = DeterministicResult.getPsuedoVels(avgDeterms, periods);
		}
		CSVFile<String> csv = new CSVFile<String>(true);
		List<String> header = Lists.newArrayList();
		header.add("Period");
		if (csDeterms != null) {
			header.add("CyberShake Source ID");
			header.add("CyberShake Rup ID");
			header.add("CyberShake Name");
			header.add("CyberShake Value "+units);
		}
		if (avgDeterms != null) {
			header.add("Weight Averaged Value");
			Preconditions.checkState(avgDeterms.size() == periods.size());
		}
		if (gmpes == null)
			gmpes = Lists.newArrayList();
		for (int i=0; i<gmpes.size(); i++) {
			List<DeterministicResult> determs = gmpeDeterms.get(i);
			Preconditions.checkNotNull(determs);
			Preconditions.checkState(determs.size() == periods.size());
			ScalarIMR gmpe = gmpes.get(i);
			header.add(gmpe.getShortName()+" Source ID");
			header.add(gmpe.getShortName()+" Rup ID");
			header.add(gmpe.getShortName()+" Name");
			header.add(gmpe.getShortName()+" Value "+units);
		}
		csv.addLine(header);
		
		Preconditions.checkState(csDeterms.size() == periods.size());
		
		for (int i=0; i<periods.size(); i++) {
			double period = periods.get(i);
			List<String> line = Lists.newArrayList();
			line.add(period+"");
			if (csDeterms != null) {
				DeterministicResult csDeterm = csDeterms.get(i);
				if (csDeterm == null) {
					line.add("");
					line.add("");
					line.add("");
					line.add("");
				} else {
					line.add(csDeterm.getSourceID()+"");
					line.add(csDeterm.getRupID()+"");
					line.add(csDeterm.getSourceName()+" (M="+(float)csDeterm.getMag()+")");
					line.add(csDeterm.getVal()+"");
				}
			}
			if (avgDeterms != null) {
				DeterministicResult avgDeterm = avgDeterms.get(i);
				line.add(avgDeterm.getVal()+"");
			}
			for (int j=0; j<gmpes.size(); j++) {
				DeterministicResult detVal = gmpeDeterms.get(j).get(i);
				line.add(detVal.getSourceID()+"");
				line.add(detVal.getRupID()+"");
				line.add(detVal.getSourceName()+" (M="+(float)detVal.getMag()+")");
				line.add(detVal.getVal()+"");
			}
			csv.addLine(line);
		}
		csv.writeToFile(outputFile);
	}
	
	private static Options createOptions() {
		Options ops = new Options();
		
		Option input = new Option("i", "input-file", true, "Input .xls file");
		input.setRequired(true);
		ops.addOption(input);
		
		Option output = new Option("o", "output-dir", true, "Output directory");
		output.setRequired(true);
		ops.addOption(output);
		
		Option vel = new Option("v", "velocities", false,
				"Generate velocity plot as well as SA (if appropriate plot types selected)");
		vel.setRequired(false);
		ops.addOption(vel);
		
		Option component = new Option("cmp", "component", true, "Intensity measure component.");
		component.setRequired(true);
		ops.addOption(component);
		
		Option help = new Option("?", "help", false, "Display this message");
		help.setRequired(false);
		ops.addOption(help);
		
		Option type = new Option("t", "type", true, "Plot save type. Options are png and pdf. "
				+"Multiple types can be comma separated (default is "+PLOT_TYPE_DEFAULT.getExtension()+")");
		ops.addOption(type);
		
		return ops;
	}
	
	public static void printHelp(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( appName, options, true );
		System.exit(2);
	}
	
	public static void printUsage(Options options, String appName) {
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter pw = new PrintWriter(System.out);
		formatter.printUsage(pw, 80, appName, options);
		pw.flush();
		System.exit(2);
	}

	public static void main(String[] args) {
		try {
			Options options = createOptions();
			
			String appName = ClassUtils.getClassNameWithoutPackage(DeterministicResultPlotter.class);
			
			CommandLineParser parser = new GnuParser();
			
			if (args.length == 0) {
				printUsage(options, appName);
			}
			
			try {
				CommandLine cmd = parser.parse( options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					printHelp(options, appName);
				}
				
				DeterministicResultPlotter calc = new DeterministicResultPlotter(cmd);
				
				calc.plot();
			} catch (MissingOptionException e) {
				Options helpOps = new Options();
				helpOps.addOption(new Option("h", "help", false, "Display this message"));
				try {
					CommandLine cmd = parser.parse( helpOps, args);
					
					if (cmd.hasOption("help")) {
						printHelp(options, appName);
					}
				} catch (ParseException e1) {}
				System.err.println(e.getMessage());
				printUsage(options, appName);
//			e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
				printUsage(options, appName);
			}
			
			System.out.println("Done!");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
