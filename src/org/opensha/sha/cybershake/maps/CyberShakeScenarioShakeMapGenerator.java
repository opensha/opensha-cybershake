package org.opensha.sha.cybershake.maps;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.data.siteData.SiteDataValueList;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.param.impl.DoubleParameter;
import org.opensha.commons.param.impl.StringParameter;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGA_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGV_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.util.SiteTranslator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class CyberShakeScenarioShakeMapGenerator {
	
	private CyberShakeStudy study;
	private int sourceID;
	private int rupID;
	private Integer rvID;
	private double[] periods;
	private CybershakeIM[] ims;
	private ScalarIMR gmpe;
	private File outputDir;
	private double spacing;
	
	private GeoDataSet[] customIntensities;
	
	private static final double SPACING_DEFAULT = 0.01;
	
	private static final Vs30_Source VS_SOURCE_DEFAULT = Vs30_Source.Wills2015;
	private Vs30_Source vsSource = VS_SOURCE_DEFAULT;
	
	private PeakAmplitudesFromDB amps2db;
	private SiteInfo2DB sites2db;
	
	private List<CybershakeRun> runs;
	
	private Double cbMin = null;
	private Double cbMax = null;
	
	private boolean noPlot;
	
	private List<Site> gmpeSites;
	
	public CyberShakeScenarioShakeMapGenerator(CommandLine cmd) {
		study = CyberShakeStudy.valueOf(cmd.getOptionValue("study"));
		sourceID = Integer.parseInt(cmd.getOptionValue("source-id"));
		rupID = Integer.parseInt(cmd.getOptionValue("rupture-id"));
		rvID = cmd.hasOption("rupture-var-id") ? Integer.parseInt(cmd.getOptionValue("rupture-var-id")) : null;
		String periodStr = cmd.getOptionValue("period");
		if (periodStr.contains(",")) {
			String[] periodSplit = periodStr.split(",");
			periods = new double[periodSplit.length];
			for (int p=0; p<periods.length; p++)
				periods[p] = Double.parseDouble(periodSplit[p]);
		} else {
			periods = new double[] { Double.parseDouble(periodStr) };
		}
		
		noPlot = cmd.hasOption("no-plot");
		
		if (cmd.hasOption("colorbar-min"))
			cbMin = Double.parseDouble(cmd.getOptionValue("colorbar-min"));
		if (cmd.hasOption("colorbar-max"))
			cbMax = Double.parseDouble(cmd.getOptionValue("colorbar-max"));
		
		if (cmd.hasOption("gmpe")) {
			gmpe = AttenRelRef.valueOf(cmd.getOptionValue("gmpe")).instance(null);
			if (cmd.hasOption("gmpe-sites")) {
				File sitesFile = new File(cmd.getOptionValue("gmpe-sites"));
				System.out.println("Loading GMPE site data from: "+sitesFile.getAbsolutePath());
				Preconditions.checkState(sitesFile.exists(), "File doesn't exist: "+sitesFile.getAbsolutePath());
				try {
					loadGMPESites(sitesFile);
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			}
			if (cmd.hasOption("vs30-source"))
				vsSource = Vs30_Source.valueOf(cmd.getOptionValue("vs30-source"));
		}
		
		outputDir = new File(cmd.getOptionValue("output-dir"));
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		spacing = cmd.hasOption("spacing") ? Double.parseDouble(cmd.getOptionValue("spacing")) : SPACING_DEFAULT;
		
		amps2db = new PeakAmplitudesFromDB(study.getDB());
		sites2db = new SiteInfo2DB(study.getDB());
		
		if (cmd.hasOption("intensity-file")) {
			String imStr = cmd.getOptionValue("intensity-file");
			String[] imFiles = imStr.contains(",") ? imStr.split(",") : new String[] {imStr};
			Preconditions.checkArgument(imFiles.length == periods.length, "Supplied %s periods but %s intensity files!",
					periods.length, imFiles.length);
			customIntensities = new GeoDataSet[imFiles.length];
			for (int i=0; i<imFiles.length; i++) {
				File file = new File(imFiles[i]);
				Preconditions.checkArgument(file.exists(), "Intensity file doesn't exist: %s", imFiles[i]);
				try {
					customIntensities[i] = ArbDiscrGeoDataSet.loadXYZFile(file.getAbsolutePath(), true);
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			}
		} else {
			ims = new CybershakeIM[periods.length];
			for (int p=0; p<periods.length; p++)
				if (periods[p] > 0d)
					ims[p] = CybershakeIM.getSA(CyberShakeComponent.RotD50, periods[p]);
				else
					Preconditions.checkState(periods[p] == 0d || periods[p] == -1d);
			
			runs = study.runFetcher().fetch();
			System.out.println("Have runs for "+runs.size()+" sites");
		}
	}
	
	public void plot() throws SQLException, IOException, ClassNotFoundException {
		GeoDataSet[] csXYZs = customIntensities != null ? customIntensities : calcCyberShake();
		
		GeoDataSet[] gmpeXYZs = gmpe == null ? null : calcGMPE();
		
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		InterpDiffMapType[] typesToPlot;
		if (gmpe == null)
			typesToPlot = new InterpDiffMapType[] { InterpDiffMapType.INTERP_MARKS, InterpDiffMapType.INTERP_NOMARKS };
		else
			typesToPlot = new InterpDiffMapType[] { InterpDiffMapType.BASEMAP, InterpDiffMapType.RATIO, InterpDiffMapType.DIFF,
				InterpDiffMapType.INTERP_MARKS, InterpDiffMapType.INTERP_NOMARKS };
		
//		CPT cpt = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
//				"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance();
		
		for (int p=0; p<periods.length; p++) {
			String prefix = "cs_shakemap_src_"+sourceID+"_rup_"+rupID;
			if (rvID == null)
				prefix += "_all_rvs";
			else
				prefix += "_rv_"+rvID;
			if (periods[p] > 0d)
				prefix += "_"+(float)periods[p]+"s";
			else if (periods[p] == 0d)
				prefix += "_pga";
			else if (periods[p] == -1d)
				prefix += "_pgv";
			
			if (customIntensities == null && csXYZs[p] != null)
				ArbDiscrGeoDataSet.writeXYZFile(csXYZs[p], new File(outputDir, prefix+"_cs_amps.txt"));
			
			if (gmpe != null)
				ArbDiscrGeoDataSet.writeXYZFile(gmpeXYZs[p], new File(outputDir, prefix+"_gmpe_amps.txt"));
			
			if (noPlot)
				continue;
			
			double maxZ = csXYZs[p] == null ? 0d : csXYZs[p].getMaxZ();
			if (gmpe != null)
				maxZ = Math.max(maxZ, gmpeXYZs[p].getMaxZ());
			
			cpt = cpt.rescale(0d, maxZ);
			
			String title = study.getName()+", Source "+sourceID+", Rupture "+rupID;
			if (rvID != null)
				title += ", RV "+rvID;
			if (periods[p] > 0)
				title += ", "+(float)periods[p]+"s SA (g)";
			else if (periods[p] == 0d)
				title += ", PGA (g)";
			else if (periods[p] == -1d)
				title += ", PGV (cm/s)";
			
			InterpDiffMapType[] myTypes;
			if (customIntensities == null && ims[p] == null) {
				// just GMPE
				myTypes = new InterpDiffMapType[] { InterpDiffMapType.BASEMAP };
				Preconditions.checkState(gmpe != null, "Can't plot period "+periods[p]+" without a GMPE!");
			} else {
				myTypes = typesToPlot;
			}
			
			InterpDiffMap map = new InterpDiffMap(study.getRegion(), gmpe == null ? null : gmpeXYZs[p], spacing, cpt, csXYZs[p],
					interpSettings, myTypes);
			map.setCustomLabel(title);
			map.setTopoResolution(TopographicSlopeFile.CA_THREE);
			map.setLogPlot(false);
			map.setDpi(300);
			map.setXyzFileName("base_map.xyz");
			map.setCustomScaleMin(cbMin == null ? 0d : cbMin);
			map.setCustomScaleMax(cbMax == null ? maxZ : cbMax);
			
			String metadata = title;
			
			System.out.println("Making map...");
			String addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
			
			System.out.println("Done, downloading");
			
			for (InterpDiffMapType type : myTypes) {
				File pngFile = new File(outputDir, prefix+"_"+type.getPrefix()+".png");
				if (!addr.endsWith("/"))
					addr += "/";
				FileUtils.downloadURL(addr+type.getPrefix()+".150.png", pngFile);
			}
		}
	}
	
	private GeoDataSet[] calcCyberShake() {
		System.out.println("Fetching CyberShake IMs");
		GeoDataSet[] xyz = new GeoDataSet[periods.length];
		
		for (int p=0; p<periods.length; p++) {
			if (ims[p] == null) {
				System.out.println("Skipping period "+periods[p]+" for CyberShake");
				continue;
			}
			xyz[p] = new ArbDiscrGeoDataSet(true);
		}
		
		runLoop:
		for (CybershakeRun run : runs) {
			Location loc = sites2db.getLocationForSiteID(run.getSiteID());
			for (int p=0; p<periods.length; p++) {
				if (ims[p] == null)
					continue;
				double value;
				if (rvID == null) {
					List<Double> values;
					try {
						values = amps2db.getIM_Values(run.getRunID(), sourceID, rupID, ims[p]);
					} catch (SQLException e) {
						continue runLoop;
					}
					double logMean = 0d;
					for (Double v : values)
						logMean += Math.log(v);
					value = Math.exp(logMean/values.size());
				} else {
					try {
						value = amps2db.getIM_Value(run.getRunID(), sourceID, rupID, rvID, ims[p]);
					} catch (SQLException e) {
						continue runLoop;
					}
				}
				Preconditions.checkState(Double.isFinite(value), "Couldn't fetch IM for run "+run.getRunID());
				value /= HazardCurveComputation.CONVERSION_TO_G;
				xyz[p].set(loc, value);
			}
		}
		
		int numCS = -1;
		for (GeoDataSet periodXYZ : xyz) {
			if (periodXYZ != null) {
				numCS = periodXYZ.size();
				break;
			}
		}
		if (numCS >= 0)
			System.out.println("Got IMs for "+numCS+"/"+runs.size()+" sites");
		else
			System.out.println("No CyberShake IM matches, skipping");
		System.out.println("DONE Fetching CyberShake IMs");
		
		return xyz;
	}
	
	private void loadGMPESites(File sitesFile) throws IOException {
		CSVFile<String> csv = CSVFile.readFile(sitesFile, true);
		ParameterList siteParams = gmpe.getSiteParams();
		Preconditions.checkState(csv.getNumCols() == 2+siteParams.size(),
				"Unexpected number of site parameters in GMPE sites CSV file. Was it created for a different GMPE?");
		for (int i=0; i<siteParams.size(); i++) {
			String expected = siteParams.getByIndex(i).getName();
			int col = i+2;
			String name = csv.get(0, col);
			Preconditions.checkState(expected.equals(name), "Expected site parameter "+expected+" at column "+col+", got "+name);
		}
		
		gmpeSites = new ArrayList<>();
		for (int row=1; row<csv.getNumRows(); row++) {
			Location loc = new Location(csv.getDouble(row, 0), csv.getDouble(row, 1));
			Site site = new Site(loc);
			for (int i=0; i<siteParams.size(); i++) {
				Parameter<?> param = (Parameter<?>)siteParams.getByIndex(i).clone();
				String valStr = csv.get(row, i+2);
				if (param instanceof DoubleParameter)
					((DoubleParameter)param).setValue(Double.parseDouble(valStr));
				else if (param instanceof StringParameter)
					((StringParameter)param).setValue(valStr);
				else
					throw new IllegalStateException("Parameter "+param.getName()+" can't be set from string value");
				site.addParameter(param);
			}
			gmpeSites.add(site);
		}
	}
	
	private void writeGMPESites(File sitesFile) throws IOException {
		Site site0 = gmpeSites.get(0);
		CSVFile<String> csv = new CSVFile<>(true);
		
		List<String> header = new ArrayList<>();
		header.add("Latitude");
		header.add("Longitude");
		for (Parameter<?> param : site0)
			header.add(param.getName());
		csv.addLine(header);
		
		for (Site site : gmpeSites) {
			List<String> line = new ArrayList<>();
			line.add(site.getLocation().getLatitude()+"");
			line.add(site.getLocation().getLongitude()+"");
			for (Parameter<?> param : site)
				line.add(param.getValue().toString());
			csv.addLine(line);
		}
		
		csv.writeToFile(sitesFile);
	}
	
	private GeoDataSet[] calcGMPE() throws IOException {
		System.out.println("Calculating GMPE");
		
		GriddedRegion region = new GriddedRegion(study.getRegion(), spacing, null);
		
		GeoDataSet[] xyz = new GeoDataSet[periods.length];
		for (int p=0; p<xyz.length; p++)
			xyz[p] = new GriddedGeoDataSet(region, true);
		
		gmpe.setParamDefaults();
		ProbEqkRupture rup = study.getERF().getSource(sourceID).getRupture(rupID);
		
		gmpe.setEqkRupture(rup);
		
		if (gmpeSites == null) {
			System.out.println("Loading GMPE site data with Vs30_Source="+vsSource);
			CyberShakeSiteBuilder siteBuild = new CyberShakeSiteBuilder(vsSource, study.getVelocityModelID());
			OrderedSiteDataProviderList provs = siteBuild.getMapProviders();
			
			// fetch site data
			System.out.println("Fetching GMPE site data...");
			ArrayList<SiteDataValueList<?>> datas = provs.getAllAvailableData(region.getNodeList());
			System.out.println("DONE Fetching GMPE site data...");
			
			SiteTranslator trans = new SiteTranslator();
			
			gmpeSites = new ArrayList<>();
			for (int i=0; i<region.getNodeCount(); i++) {
				Location loc = region.getLocation(i);
				Site site = new Site(loc);
				for (Parameter<?> param : gmpe.getSiteParams()) {
					param = (Parameter<?>)param.clone();
					List<SiteDataValue<?>> siteData = new ArrayList<>();
					for (SiteDataValueList<?> data : datas)
						siteData.add(data.getValue(i));
					trans.setParameterValue(param, siteData);
					site.addParameter(param);
				}
				gmpeSites.add(site);
			}
			
			File sitesFile = new File(outputDir, "cs_gmpe_sites_"+study.name()+"_"+(float)spacing+".csv");
			System.out.println("Writing GMPE site data to "+sitesFile);
			writeGMPESites(sitesFile);
		} else {
			Preconditions.checkState(gmpeSites.size() == region.getNodeCount(),
					"Loaded GMPE sites are unexpected size. different region or spacing?");
		}
		
		for (int i=0; i<region.getNodeCount(); i++) {
			gmpe.setSite(gmpeSites.get(i));
			for (int p=0; p<periods.length; p++) {
				if (periods[p] > 0) {
					gmpe.setIntensityMeasure(SA_Param.NAME);
					SA_Param saParam = (SA_Param)gmpe.getIntensityMeasure();
					SA_Param.setPeriodInSA_Param(saParam, periods[p]);
				} else if (periods[p] == 0d) {
					gmpe.setIntensityMeasure(PGA_Param.NAME);
				} else if (periods[p] == -1d) {
					gmpe.setIntensityMeasure(PGV_Param.NAME);
				}
				double value = Math.exp(gmpe.getMean());
				xyz[p].set(i, value);
			}
		}
		
		return xyz;
	}
	
	private static String getStudyList() {
		List<String> names = new ArrayList<>();
		for (CyberShakeStudy study : CyberShakeStudy.values())
			if (!study.name().toLowerCase().contains("rsqsim"))
				names.add(study.name());
		return Joiner.on(", ").join(names);
	}
	
	private static String getVsSourceList() {
		List<String> names = new ArrayList<>();
		for (Vs30_Source source : Vs30_Source.values())
			names.add(source.name());
		return Joiner.on(", ").join(names);
	}
	
	private static String getGMPEList() {
		List<String> names = new ArrayList<>();
		names.add(AttenRelRef.NGAWest_2014_AVG_NOIDRISS.name());
		names.add(AttenRelRef.NGAWest_2014_AVG.name());
		names.add(AttenRelRef.ASK_2014.name());
		names.add(AttenRelRef.BSSA_2014.name());
		names.add(AttenRelRef.CB_2014.name());
		names.add(AttenRelRef.CY_2014.name());
		return Joiner.on(", ").join(names);
	}
	
	private static Options createOptions() {
		Options ops = new Options();
		
		Option studyOp = new Option("st", "study", true, "CyberShake study. One of: "+getStudyList());
		studyOp.setRequired(true);
		ops.addOption(studyOp);
		
		Option sourceOp = new Option("src", "source-id", true, "Source ID");
		sourceOp.setRequired(true);
		ops.addOption(sourceOp);
		
		Option rupOp = new Option("rup", "rupture-id", true, "Rupture ID");
		rupOp.setRequired(true);
		ops.addOption(rupOp);
		
		Option rvOp = new Option("rv", "rupture-var-id", true, "Optional Rupture Variation ID. If omitted, log-mean value across all RVs is plotted");
		rvOp.setRequired(false);
		ops.addOption(rvOp);
		
		Option periodsOp = new Option("p", "period", true, "Period(s) to plot, multiple can be comma separated. O for PGA, -1 for PGV");
		periodsOp.setRequired(true);
		ops.addOption(periodsOp);
		
		Option gmpeOp = new Option("g", "gmpe", true, "GMPE to use for basemap (optional). One of: "+getGMPEList());
		gmpeOp.setRequired(false);
		ops.addOption(gmpeOp);
		
		Option vsOp = new Option("vs", "vs30-source", true, "Optional Vs30 Source. One of: "+getVsSourceList()+". Default: "+VS_SOURCE_DEFAULT.name());
		vsOp.setRequired(false);
		ops.addOption(vsOp);
		
		Option outputDirOp = new Option("o", "output-dir", true, "Output directory");
		outputDirOp.setRequired(true);
		ops.addOption(outputDirOp);
		
		Option spacingOp = new Option("sp", "spacing", true, "Optional grid spacing in decimal degrees. Default: "+(float)SPACING_DEFAULT);
		spacingOp.setRequired(false);
		ops.addOption(spacingOp);

		Option cbMinOp = new Option("cbmin", "colorbar-min", true, "Optional plot colorbar min value");
		cbMinOp.setRequired(false);
		ops.addOption(cbMinOp);
		
		Option cbMaxOp = new Option("cbmax", "colorbar-max", true, "Optional plot colorbar max value");
		cbMaxOp.setRequired(false);
		ops.addOption(cbMaxOp);
		
		Option gmpeSitesOp = new Option("gs", "gmpe-sites", true,
				"Optional path to GMPE sites CSV file (to avoid hitting the site data server over and over again)");
		gmpeSitesOp.setRequired(false);
		ops.addOption(gmpeSitesOp);
		
		Option noPlotOp = new Option("np", "no-plot", false, "Skip plotting and only write out data files");
		noPlotOp.setRequired(false);
		ops.addOption(noPlotOp);
		
		Option customIMsOp = new Option("if", "intensity-file", true,
				"Use the supplied custom intensity file(s) instead of the CyberShake database. Multiple comma separated files must be supplied "
				+ "if multiple periods are supplied. Format is ASCII, each line should contain <lat> <lon> <value>.");
		customIMsOp.setRequired(false);
		ops.addOption(customIMsOp);
		
		Option helpOp = new Option("?", "help", false, "Show this message");
		helpOp.setRequired(false);
		ops.addOption(helpOp);
		
		return ops;
	}

	public static void main(String[] args) {
		if (args.length == 1 && args[0].equals("--hardcoded")) {
			System.out.println("HARDCODED");
//			String argz = "--study STUDY_15_4 --period 0,-1,3 --source-id 69 --rupture-id 6 --rupture-var-id 14 --output-dir /tmp/cs_shakemap "
//					+ "--gmpe "+AttenRelRef.NGAWest_2014_AVG_NOIDRISS.name();
			String argz = "--gmpe "+AttenRelRef.NGAWest_2014_AVG_NOIDRISS.name()+" --study STUDY_15_4 --period 0,-1,3 --source-id 69 --rupture-id 6"
					+ "--rupture-var-id 14 --output-dir /tmp/cs_shakemap";
//			String argz = "--study STUDY_15_4 --period -1 --colorbar-min 1 --colorbar-max 7 --source-id 69 --rupture-id 6 --rupture-var-id 14 --output-dir /tmp/cs_shakemap "
//					+ "--gmpe "+AttenRelRef.NGAWest_2014_AVG_NOIDRISS.name();
//			String argz = "--study STUDY_15_12 --period 0.2 --source-id 69 --rupture-id 6 --output-dir /tmp "
//					+ "--gmpe "+AttenRelRef.NGAWest_2014_AVG_NOIDRISS.name()+" --spacing 0.005";
//			argz += " --intensity-file /tmp/cs_shakemap_src_69_rup_6_all_rvs_0.2s_cs_amps.txt";
//			String argz = "--study STUDY_15_12 --period 0.2 --source-id 69 --rupture-id 6 --output-dir /tmp "
//					+ "--gmpe "+AttenRelRef.NGAWest_2014_AVG_NOIDRISS.name()+" --spacing 0.01 --vs30-source Thompson2020"
//							+ " --gmpe-sites /tmp/cs_gmpe_sites_STUDY_15_12_0.01.csv";
//			String argz = "--help";
			args = argz.split(" ");
		}
		try {
			Options options = createOptions();
			
			String appName = ClassUtils.getClassNameWithoutPackage(CyberShakeScenarioShakeMapGenerator.class);
			
			CommandLineParser parser = new DefaultParser();
			
			if (args.length == 0) {
				HazardCurvePlotter.printUsage(options, appName);
			}
			
			try {
				CommandLine cmd = parser.parse( options, args);
				
				if (cmd.hasOption("help") || cmd.hasOption("?")) {
					HazardCurvePlotter.printHelp(options, appName);
				}
				
				CyberShakeScenarioShakeMapGenerator plotter = new CyberShakeScenarioShakeMapGenerator(cmd);
				
				plotter.plot();
				
				plotter.study.getDB().destroy();
			} catch (MissingOptionException e) {
//				Options helpOps = new Options();
//				helpOps.addOption(new Option("h", "help", false, "Display this message"));
//				CommandLine cmd = parser.parse( helpOps, args);
//				
//				if (cmd.hasOption("help")) {
//					HazardCurvePlotter.printHelp(options, appName);
//				}
				System.err.println(e.getMessage());
				HazardCurvePlotter.printUsage(options, appName);
//			e.printStackTrace();
			}
			
			System.out.println("Done!");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
