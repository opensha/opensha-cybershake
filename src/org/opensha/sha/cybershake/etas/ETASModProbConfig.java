package org.opensha.sha.cybershake.etas;

import java.awt.Color;
import java.awt.Font;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import org.apache.commons.math3.stat.StatUtils;
import org.dom4j.DocumentException;
import org.jfree.chart.annotations.XYTextAnnotation;
import org.jfree.ui.TextAnchor;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.AbstractModProbConfig;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.RuptureVariationProbabilityModifier;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.eew.ZeroProbMod;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.CompoundSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.magdist.IncrementalMagFreqDist;
import org.opensha.sha.magdist.SummedMagFreqDist;

import scratch.UCERF3.FaultSystemRupSet;
import scratch.UCERF3.FaultSystemSolution;
import scratch.UCERF3.enumTreeBranches.FaultModels;
import scratch.UCERF3.erf.ETAS.ETAS_CatalogIO;
import scratch.UCERF3.erf.ETAS.ETAS_EqkRupture;
import scratch.UCERF3.erf.ETAS.ETAS_MultiSimAnalysisTools;
import scratch.UCERF3.erf.ETAS.ETAS_PrimaryEventSampler;
import scratch.UCERF3.erf.ETAS.ETAS_SimAnalysisTools;
import scratch.UCERF3.erf.ETAS.ETAS_Utils;
import scratch.UCERF3.erf.ETAS.FaultSystemSolutionERF_ETAS;
import scratch.UCERF3.erf.ETAS.IntegerPDF_FunctionSampler;
import scratch.UCERF3.erf.ETAS.ETAS_Params.ETAS_ParameterList;
import scratch.UCERF3.erf.ETAS.ETAS_Params.U3ETAS_ProbabilityModelOptions;
import scratch.UCERF3.erf.utils.ProbabilityModelsCalc;
import scratch.UCERF3.griddedSeismicity.AbstractGridSourceProvider;
import scratch.UCERF3.utils.FaultSystemIO;
import scratch.UCERF3.utils.IDPairing;
import scratch.UCERF3.utils.MatrixIO;
import scratch.UCERF3.utils.RELM_RegionUtils;
import scratch.UCERF3.utils.ModUCERF2.ModMeanUCERF2_FM2pt1;
import scratch.kevin.ucerf3.etas.MPJ_ETAS_Simulator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Doubles;

public class ETASModProbConfig extends AbstractModProbConfig {
	
	public enum ETAS_CyberShake_Scenarios {
		PARKFIELD("Parkfield Scenario", 8, triggerMap(FaultModels.FM3_1, 30473, FaultModels.FM2_1, 2099)),
		BOMBAY_BEACH_M6("Bombay Beach Pt Scenario", 9, new Location(33.31833333333334,-115.72833333333335,5.8), 6.0),
		BOMBAY_BEACH_BRAWLEY_FAULT_M6("Bombay Beach Scenario", -1, triggerMap(FaultModels.FM3_1, 238408)),
		BOMBAY_BEACH_2016_SWARM("Bombay Beach 2016 Swarm", -1, new Location(33.298, -115.7126667, 2.38), 4.3) {
			@Override
			public long getOT() {
				return 1474990200000l;
			}
		},
		MOJAVE_S_POINT_M6("Mojave M6 Point Scenario", -1, new Location(34.42295,-117.80177,5.8), 6.0),
		TEST_BOMBAY_M6_SUBSET("Bombay Beach M6 Scenario 50%", -1),
		TEST_BOMBAY_M6_SUBSET_FIRST("Bombay Beach M6 Scenario First Half", -1),
		TEST_BOMBAY_M6_SUBSET_SECOND("Bombay Beach M6 Scenario Second Half", -1),
		TEST_NEGLIGABLE("Test Negligable Scenario", -1),
		MAPPED_UCERF2("UCERF2 Time Indep", 10),
		MAPPED_UCERF2_TIMEDEP("UCERF2 Time Dep, no ETAS", 11);
		
		private int probModelID;
		private String name;
		private Map<FaultModels, Integer> triggerRupIndexes;
		private Location triggerLoc;
		private double triggerMag;
		
		private static Map<FaultModels, Integer> triggerMap(FaultModels fm, int index) {
			Map<FaultModels, Integer> triggerRupIndexes = Maps.newHashMap();
			triggerRupIndexes.put(fm, index);
			return triggerRupIndexes;
		}
		
		private static Map<FaultModels, Integer> triggerMap(FaultModels fm1, int index1, FaultModels fm2, int index2) {
			Map<FaultModels, Integer> triggerRupIndexes = Maps.newHashMap();
			triggerRupIndexes.put(fm1, index1);
			triggerRupIndexes.put(fm2, index2);
			return triggerRupIndexes;
		}
		
		private ETAS_CyberShake_Scenarios(String name, int probModelID) {
			this(name, probModelID, null);
		}
		
		private ETAS_CyberShake_Scenarios(String name, int probModelID, Map<FaultModels, Integer> triggerRupIndexes) {
			this(name, probModelID, triggerRupIndexes, null, Double.NaN);
		}
		
		private ETAS_CyberShake_Scenarios(String name, int probModelID, Location triggerLoc, double triggerMag) {
			this(name, probModelID, null, triggerLoc, triggerMag);
		}
		
		private ETAS_CyberShake_Scenarios(String name, int probModelID, Map<FaultModels, Integer> triggerRupIndexes,
				Location triggerLoc, double triggerMag) {
			this.triggerRupIndexes = triggerRupIndexes;
			this.triggerLoc = triggerLoc;
			this.triggerMag = triggerMag;
			this.probModelID = probModelID;
			this.name = name;
		}
		
		public boolean isETAS() {
			return triggerRupIndexes != null || triggerLoc != null;
		}
		
		@Override
		public String toString() {
			return name;
		}
		
		public int getProbModelID() {
			return probModelID;
		}

		public int getTriggerRupIndex(FaultModels fm) {
			if (triggerRupIndexes == null)
				return -1;
			return triggerRupIndexes.get(fm);
		}

		public Location getTriggerLoc() {
			return triggerLoc;
		}

		public double getTriggerMag() {
			return triggerMag;
		}
		
		public long getOT() {
			return Math.round((2012.0-1970.0)*ProbabilityModelsCalc.MILLISEC_PER_YEAR);
		}
		
		public ETAS_EqkRupture getRupture(long ot, FaultSystemRupSet rupSet, FaultModels fm) {
			if (!isETAS())
				return null;
			if (triggerRupIndexes != null) {
				ETAS_EqkRupture mainshockRup = new ETAS_EqkRupture();
				mainshockRup.setOriginTime(ot);
				
				int fssScenarioRupID = triggerRupIndexes.get(fm);
				
				mainshockRup.setAveRake(rupSet.getAveRakeForRup(fssScenarioRupID));
				mainshockRup.setMag(rupSet.getMagForRup(fssScenarioRupID));
				mainshockRup.setRuptureSurface(rupSet.getSurfaceForRupupture(fssScenarioRupID, 1d, false));
				mainshockRup.setID(0);
//				debug("test Mainshock: "+erf.getSource(srcID).getName());
				
				if (!Double.isNaN(triggerMag))
					mainshockRup.setMag(triggerMag);
				
				return mainshockRup;
			} else {
				ETAS_EqkRupture mainshockRup = new ETAS_EqkRupture();
				mainshockRup.setOriginTime(ot);	
				
				mainshockRup.setAveRake(0.0);
				mainshockRup.setMag(triggerMag);
				mainshockRup.setPointSurface(triggerLoc);
				mainshockRup.setID(0);
				
				return mainshockRup;
			}
		}
	}
	
	public enum ETAS_Cybershake_TimeSpans {
		ONE_YEAR("One Year", 1, 1d),
		ONE_WEEK("One Week", 4, 7d/365.25),
		ONE_DAY("One Day", 2, 1d/365.25);
		
		int timeSpanID;
		String name;
		private double years;
		private ETAS_Cybershake_TimeSpans(String name, int timeSpanID, double years) {
			this.timeSpanID = timeSpanID;
			this.name = name;
			this.years = years;
		}
		
		@Override
		public String toString() {
			return name;
		}
		
		public int getTimeSpanID() {
			return timeSpanID;
		}
		
		public double getTimeYears() {
			return years;
		}
		
		public double getTimeDays() {
			return years * 365.25;
		}
	}
	
	private static final boolean calc_by_add_spontaneous = true;
	private static final boolean calc_by_treat_as_new_rupture = true;
	// if >0, all hypos within this distance will be promoted, not just closest
	private static final double hypocenter_buffer_km = 10d;
	
	private ETAS_CyberShake_Scenarios scenario;
	private ETAS_Cybershake_TimeSpans timeSpan;
	
	private File catalogsFile;
	
	private long ot;
	private long endTime;
	
	private FaultSystemSolution sol;
	private List<List<ETAS_EqkRupture>> catalogs;
	
	// mapping from UCERF2 <sourceID, rupID> to UCERF3 FSS Index
//	private Table<Integer, Integer, Integer> rupMappingTable;
	// mapping from FSS Index to CyberShake UCERF2 source/rup
	private Map<Integer, IDPairing> rupMappingTable;
	private Map<IDPairing, Integer> rupMappingReverseTable;
	
	private ERF ucerf2;
	private ERF modifiedUCERF2;
//	private FaultSystemSolutionERF timeDepNoETAS_ERF;
	/** map from original modified source to original source */
	private Map<Integer, Integer> u2SourceMappings;
	/** map from original source to modified source */
	private Map<Integer, Integer> u2ModSourceMappings;
	/** map from original source ID, to modRupID, origRupID */
	private Map<Integer, Map<Integer, Integer>> rupIndexMappings;
	/** map from original source ID, to origRupID, modRupID */
	private Map<Integer, Map<Integer, Integer>> rupIndexMappingsReversed;
	
	private Map<Integer, Map<Location, List<Integer>>> rvHypoLocations;
	private Map<IDPairing, Map<Integer, Location>> hypoLocationsByRV;
	private Map<IDPairing, List<Location>> gmpeHypoLocations;
	
	private Map<IDPairing, List<Double>> rvProbs;
	private List<RVProbSortable> rvProbsSortable;
	
	private double[][] fractOccurances;
	
	private double normalizedTriggerRate = 0d;
	
	// if set to true, will bump up probability for all RVs equally instead of the closest hypocenters to
	// the etas rupture hypos
	private boolean triggerAllHyposEqually = false;
	
	private int erfID;
	private int rupVarScenID;

	public ETASModProbConfig(ETAS_CyberShake_Scenarios scenario, ETAS_Cybershake_TimeSpans timeSpan,
			FaultSystemSolution sol, File catalogsFile, File mappingsCSVFile, int erfID, int rupVarScenID)
			throws IOException {
		super(scenario+" ("+timeSpan+")", scenario.probModelID, timeSpan.timeSpanID);
		
		this.erfID = erfID;
		this.rupVarScenID = rupVarScenID;
		
		this.catalogsFile = catalogsFile;
		
		this.sol = sol;
//		ot = Math.round((2014.0-1970.0)*ProbabilityModelsCalc.MILLISEC_PER_YEAR); // occurs at 2014
		ot = scenario.getOT();
		endTime = ot + Math.round(timeSpan.years*ProbabilityModelsCalc.MILLISEC_PER_YEAR);
		
		System.out.println("Start time: "+ot);
		System.out.println("End time: "+endTime);
		
		this.scenario = scenario;
		this.timeSpan = timeSpan;
		
		ucerf2 = MeanUCERF2_ToDB.createUCERF2ERF();
		if (scenario == ETAS_CyberShake_Scenarios.MAPPED_UCERF2_TIMEDEP)
			loadMappings(mappingsCSVFile, MeanUCERF2.PROB_MODEL_WGCEP_PREF_BLEND);
		else
			loadMappings(mappingsCSVFile);
		
		// TODO use time dep ERF probs?
//		timeDepNoETAS_ERF = new FaultSystemSolutionERF(sol);
//		timeDepNoETAS_ERF.getParameter(IncludeBackgroundParam.NAME).setValue(IncludeBackgroundOption.INCLUDE);
//		timeDepNoETAS_ERF.setParameter(BackgroundRupParam.NAME, BackgroundRupType.POINT);
//		timeDepNoETAS_ERF.setParameter(ApplyGardnerKnopoffAftershockFilterParam.NAME, false);
//		timeDepNoETAS_ERF.getParameter(ProbabilityModelParam.NAME).setValue(ProbabilityModelOptions.U3_BPT);
//		timeDepNoETAS_ERF.getParameter(MagDependentAperiodicityParam.NAME).setValue(MagDependentAperiodicityOptions.MID_VALUES);
//		BPTAveragingTypeOptions aveType = BPTAveragingTypeOptions.AVE_RI_AVE_NORM_TIME_SINCE;
//		timeDepNoETAS_ERF.setParameter(BPTAveragingTypeParam.NAME, aveType);
//		timeDepNoETAS_ERF.setParameter(AleatoryMagAreaStdDevParam.NAME, 0.0);
//		timeDepNoETAS_ERF.getParameter(HistoricOpenIntervalParam.NAME).setValue(2014d-1875d);	
//		timeDepNoETAS_ERF.getTimeSpan().setStartTimeInMillis(ot+1);
//		timeDepNoETAS_ERF.getTimeSpan().setDuration(duration);
	}
	
	private void loadCatalogs(File catalogsDirs) throws IOException {
		catalogs = ETAS_CatalogIO.loadCatalogsBinary(catalogsFile);
		int numWithRups = 0;
		int numFaultRups = 0;
		for (int i=0; i<catalogs.size(); i++) {
			List<ETAS_EqkRupture> catalog = filterCatalog(catalogs.get(i));
			catalogs.set(i, catalog);
			if (!catalog.isEmpty())
				numWithRups++;
			numFaultRups += catalog.size();
		}
		
		System.out.println("Loaded "+catalogs.size()+" catalogs ("+numWithRups+" with "+numFaultRups+" fault rups)");
		Preconditions.checkState(!catalogs.isEmpty(), "Must load at least one catalog!");
		Preconditions.checkState(numWithRups > 0, "Must have at least one catalog with a fault based rupture!");
		
		if (scenario == ETAS_CyberShake_Scenarios.TEST_BOMBAY_M6_SUBSET_FIRST)
			catalogs = catalogs.subList(0, catalogs.size()/2);
		else if (scenario == ETAS_CyberShake_Scenarios.TEST_BOMBAY_M6_SUBSET_SECOND)
			catalogs = catalogs.subList(catalogs.size()/2, catalogs.size());
	}
	
	private List<ETAS_EqkRupture> filterCatalog(List<ETAS_EqkRupture> catalog) {
		// cull ruptures after end time
		for (int i=catalog.size(); --i >= 0;) {
			long rupTime = catalog.get(i).getOriginTime();
			if (rupTime > endTime)
				catalog.remove(i);
			else
				break;
		}
		
		// now already filtered in binary files
//		if (calc_by_add_spontaneous)
//			// only spontaneous, we're adding to the long term rates
//			catalog = ETAS_SimAnalysisTools.getChildrenFromCatalog(catalog, 0);
		
		// now only FSS ruptures
		for (int i=catalog.size(); --i >= 0;)
			if (catalog.get(i).getFSSIndex() < 0)
				catalog.remove(i);
		
		return catalog;
	}
	
	private void loadMappings(File mappingsCSVFile) throws IOException {
		loadMappings(mappingsCSVFile, UCERF2.PROB_MODEL_POISSON);
	}
	
	private void loadMappings(File mappingsCSVFile, String u2ProbModel) throws IOException {
		modifiedUCERF2 = new ModMeanUCERF2_FM2pt1();

		modifiedUCERF2.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_EXCLUDE);
		modifiedUCERF2.setParameter(UCERF2.PROB_MODEL_PARAM_NAME, u2ProbModel);
		modifiedUCERF2.getTimeSpan().setDuration(1d);
		modifiedUCERF2.setParameter(UCERF2.FLOATER_TYPE_PARAM_NAME, UCERF2.FULL_DDW_FLOATER);
		modifiedUCERF2.updateForecast();
		
		// now we need to map from modified UCERF2 to regular UCERF2

		Map<String, Integer> origU2SourceNames = Maps.newHashMap();
		for (int sourceID=0; sourceID<ucerf2.getNumSources(); sourceID++)
			origU2SourceNames.put(ucerf2.getSource(sourceID).getName(), sourceID);
		Map<String, Integer> origU2ModSourceNames = Maps.newHashMap();
		for (int sourceID=0; sourceID<modifiedUCERF2.getNumSources(); sourceID++)
			origU2ModSourceNames.put(modifiedUCERF2.getSource(sourceID).getName(), sourceID);
		
		// map from modified source to original source
		u2SourceMappings = Maps.newHashMap();
		for (int sourceID=0; sourceID<modifiedUCERF2.getNumSources(); sourceID++)
			u2SourceMappings.put(sourceID, origU2SourceNames.get(modifiedUCERF2.getSource(sourceID).getName()));

		u2ModSourceMappings = Maps.newHashMap();
		for (int sourceID=0; sourceID<ucerf2.getNumSources(); sourceID++)
			u2ModSourceMappings.put(sourceID, origU2ModSourceNames.get(ucerf2.getSource(sourceID).getName()));
		
		// map rupture IDs now
		// map from original source ID, to <modRupID, origRupID>
		rupIndexMappings = Maps.newHashMap();
		for (int modSourceID=0; modSourceID<modifiedUCERF2.getNumSources(); modSourceID++) {
			int origSourceID = u2SourceMappings.get(modSourceID);
			
			Map<Integer, Integer> rupIndexMapping = rupIndexMappings.get(origSourceID);
			if (rupIndexMapping == null) {
				rupIndexMapping = Maps.newHashMap();
				rupIndexMappings.put(origSourceID, rupIndexMapping);
			}
			
			ProbEqkSource modSource = modifiedUCERF2.getSource(modSourceID);
			ProbEqkSource origSource = ucerf2.getSource(origSourceID);
			Preconditions.checkState(modSource.getName().equals(origSource.getName()));
			for (int modRupID=0; modRupID<modSource.getNumRuptures(); modRupID++) {
				ProbEqkRupture modRup = modSource.getRupture(modRupID);
				double closestDist = Double.POSITIVE_INFINITY;
				int closestOrigID = -1;
				
				for (int origRupID=0; origRupID<origSource.getNumRuptures(); origRupID++) {
					ProbEqkRupture origRup = origSource.getRupture(origRupID);
//					System.out.println("Testing mags "+modRup.getMag()+" "+origRup.getMag());
					if ((float)modRup.getMag() != (float)origRup.getMag())
						// mag must match
						continue;
					// distance between top left points
					double dist1 = LocationUtils.linearDistanceFast(
							origRup.getRuptureSurface().getFirstLocOnUpperEdge(),
							modRup.getRuptureSurface().getFirstLocOnUpperEdge());
					double dist2 = LocationUtils.linearDistanceFast(
							origRup.getRuptureSurface().getLastLocOnUpperEdge(),
							modRup.getRuptureSurface().getFirstLocOnUpperEdge());
					double dist3 = LocationUtils.linearDistanceFast(
							origRup.getRuptureSurface().getFirstLocOnUpperEdge(),
							modRup.getRuptureSurface().getLastLocOnUpperEdge());
					double dist = Math.min(dist1, Math.min(dist2, dist3));
//					System.out.println("dist="+dist);
					if (dist < closestDist) {
						closestDist = dist;
						closestOrigID = origRupID;
					}
				}
				if (closestDist > 10d)
					continue;
//				Preconditions.checkState(closestDist < 30d, "No mapping for mod source "+modSourceID+" rup "+modRupID
//						+". Closest with mag match was "+closestDist+" km away");
				rupIndexMapping.put(modRupID, closestOrigID);
			}
		}
		// now do mapping reversed
		rupIndexMappingsReversed = Maps.newHashMap();
		for (Integer sourceID : rupIndexMappings.keySet()) {
			Map<Integer, Integer> rupMappings = rupIndexMappings.get(sourceID);
			Map<Integer, Integer> rupMappingsReversed = Maps.newHashMap();
			for (Integer modRupID : rupMappings.keySet())
				rupMappingsReversed.put(rupMappings.get(modRupID), modRupID);
			rupIndexMappingsReversed.put(sourceID, rupMappingsReversed);
		}
		
		CSVFile<String> csv = CSVFile.readFile(mappingsCSVFile, true);
		
		Map<Integer, List<IDPairing>> rupCandidates = Maps.newHashMap();
		
		HashSet<Integer> fssIndexes = new HashSet<Integer>();
		
		for (int r=1; r<csv.getNumRows(); r++) {
			List<String> row = csv.getLine(r);
			int fssIndex = Integer.parseInt(row.get(4));
			fssIndexes.add(fssIndex);
			int modSourceID = Integer.parseInt(row.get(0));
			int sourceID = u2SourceMappings.get(modSourceID);
			int modRupID = Integer.parseInt(row.get(1));
			if (!rupIndexMappings.get(sourceID).containsKey(modRupID)) {
				System.out.println("WARNING: skipping (no mapping) for mod "+modSourceID+", "+modRupID);
				continue;
			}
			int rupID = rupIndexMappings.get(sourceID).get(modRupID);
			String sourceName = row.get(2);
			double mag = Double.parseDouble(row.get(3));
			
			String erfSourceName = ucerf2.getSource(sourceID).getName();
			Preconditions.checkState(erfSourceName.equals(sourceName),
					"Source name mismatch for source "+sourceID+":\n\tFile: "+sourceName+"\n\tERF: "+erfSourceName);
			double erfMag;
			if (rupID >= ucerf2.getSource(sourceID).getNumRuptures())
				erfMag = 0;
			else
				erfMag = ucerf2.getSource(sourceID).getRupture(rupID).getMag();
			List<Double> erfMags = Lists.newArrayList();
			for (ProbEqkRupture rup : ucerf2.getSource(sourceID))
				erfMags.add(rup.getMag());
//			if ((float)mag != (float)erfMag || rupID >= erfMags.size()) {
//				System.out.println("Mag mismatch for source "+sourceID+" rup "+rupID+":\n\tFile: "
//						+mag+"\n\tERF: "+erfMag+"\n\tERF Mags: "+Joiner.on(",").join(erfMags));
//				// remap
////				double magTolerance = 0.05;
//				double smallestDelta = Double.POSITIVE_INFINITY;
//				double matchProb = 0;
//				int matchIndex = -1;
//				ProbEqkSource source = ucerf2.getSource(sourceID);
//				for (int erfRupID=0; erfRupID<source.getNumRuptures(); erfRupID++) {
//					ProbEqkRupture rup = source.getRupture(erfRupID);
//					double delta = Math.abs(mag - rup.getMag());
//					if (delta < smallestDelta || (delta == smallestDelta && rup.getProbability() > matchProb)) {
//						matchProb = rup.getProbability();
//						smallestDelta = delta;
//						matchIndex = erfRupID;
//					}
//				}
//				Preconditions.checkState(smallestDelta < 0.05, "Couldn't find a match within 0.05 mag units: "+smallestDelta);
//				rupID = matchIndex;
//			}
			Preconditions.checkState((float)mag == (float)erfMag, "Mag mismatch for source "+sourceID+" rup "+rupID+":\n\tFile: "
					+mag+"\n\tERF: "+erfMag+"\n\tERF Mags: "+Joiner.on(",").join(erfMags));
			
			List<IDPairing> candidates = rupCandidates.get(fssIndex);
			if (candidates == null) {
				candidates = Lists.newArrayList();
				rupCandidates.put(fssIndex, candidates);
			}
			candidates.add(new IDPairing(sourceID, rupID));
//			rupMappingTable.put(sourceID, rupID, fssIndex);
		}
		System.out.println("Loaded candidates for "+rupCandidates.size()+"/"+fssIndexes.size()+" FSS ruptures");
		
		rupMappingTable = Maps.newHashMap();
		rupMappingReverseTable = Maps.newHashMap();
		for (Integer fssIndex : rupCandidates.keySet()) {
			List<IDPairing> candidates = rupCandidates.get(fssIndex);
			// rate by mag, dist, prob
			double closestMag = Double.POSITIVE_INFINITY;
			double prevDist = Double.POSITIVE_INFINITY;
			double prevProb = 0d;
			IDPairing match = null;
			
			double origMag = sol.getRupSet().getMagForRup(fssIndex);
			CompoundSurface surf = (CompoundSurface) sol.getRupSet().getSurfaceForRupupture(fssIndex, 1d, false);
			List<? extends RuptureSurface> surfs = surf.getSurfaceList();
			Location fssFirstLoc = surfs.get(0).getFirstLocOnUpperEdge();
			Location fssLastLoc = surfs.get(surfs.size()-1).getFirstLocOnUpperEdge();
			
			for (IDPairing pair : candidates) {
				int sourceID = pair.getID1();
				int rupID = pair.getID2();
				
				ProbEqkRupture rup = ucerf2.getRupture(sourceID, rupID);
				double magDelta = Math.abs(rup.getMag() - origMag);
				double dist1 = LocationUtils.linearDistanceFast(
						fssFirstLoc,
						rup.getRuptureSurface().getFirstLocOnUpperEdge());
				double dist2 = LocationUtils.linearDistanceFast(
						fssLastLoc,
						rup.getRuptureSurface().getFirstLocOnUpperEdge());
				double dist3 = LocationUtils.linearDistanceFast(
						fssFirstLoc,
						rup.getRuptureSurface().getLastLocOnUpperEdge());
				double dist = Math.min(dist1, Math.min(dist2, dist3));
				double prob = rup.getProbability();
				if (magDelta < closestMag || (magDelta == closestMag &&
						(dist < prevDist || (dist == prevDist && prob > prevProb)))) {
					closestMag = magDelta;
					prevDist = dist;
					prevProb = prob;
					match = pair;
				}
			}
			Preconditions.checkNotNull(match);
			rupMappingTable.put(fssIndex, match);
//			Preconditions.checkState(!rupMappingReverseTable.containsKey(match));
			rupMappingReverseTable.put(match, fssIndex);
		}
	}
	
	private void loadHyposForETASRups() {
		rvHypoLocations = Maps.newHashMap();
		hypoLocationsByRV = Maps.newHashMap();
		gmpeHypoLocations = Maps.newHashMap();
		
		System.out.println("Loading hypos");
		
		for (List<ETAS_EqkRupture> catalog : catalogs) {
			for (ETAS_EqkRupture rup : catalog) {
				int fssIndex = rup.getFSSIndex();
				
				IDPairing pair = rupMappingTable.get(fssIndex);
				Preconditions.checkNotNull(pair, "No mapping for rupture that occurred: "+fssIndex);
				
				// original hypocenter for GMPE calcs
				Location origHypo = rup.getHypocenterLocation();
				List<Location> gmpeHypos = gmpeHypoLocations.get(pair);
				if (gmpeHypos == null) {
					gmpeHypos = Lists.newArrayList();
					gmpeHypoLocations.put(pair, gmpeHypos);
				}
				gmpeHypos.add(origHypo);
				
				if (rvHypoLocations.containsKey(fssIndex))
					// we've already loaded hypos
					continue;
				
				String sql = "SELECT Rup_Var_ID,Hypocenter_Lat,Hypocenter_Lon,Hypocenter_Depth FROM Rupture_Variations " +
						"WHERE ERF_ID=" + erfID + " AND Rup_Var_Scenario_ID=" + rupVarScenID + " " +
						"AND Source_ID=" + pair.getID1() + " AND Rupture_ID=" + pair.getID2();
				
				Map<Location, List<Integer>> locsMap = Maps.newHashMap();
				rvHypoLocations.put(fssIndex, locsMap);
				Map<Integer, Location> hyposByRV = Maps.newHashMap();
				hypoLocationsByRV.put(pair, hyposByRV);
				
				try {
					ResultSet rs = db.selectData(sql);
					boolean success = rs.first();
					while (success) {
						int rvID = rs.getInt("Rup_Var_ID");
						double lat = rs.getDouble("Hypocenter_Lat");
						double lon = rs.getDouble("Hypocenter_Lon");
						double depth = rs.getDouble("Hypocenter_Depth");
						Location loc = new Location(lat, lon, depth);
						
						List<Integer> ids = locsMap.get(loc);
						if (ids == null) {
							ids = Lists.newArrayList();
							locsMap.put(loc, ids);
						}
						ids.add(rvID);
						hyposByRV.put(rvID, loc);

						success = rs.next();
					}
				} catch (SQLException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
				Preconditions.checkState(!locsMap.isEmpty());
			}
		}
		
		System.out.println("Done loading hypos");
	}
	
	public void setNormTriggerRate(double normalizedTriggerRate) {
		this.normalizedTriggerRate = normalizedTriggerRate;
		// clear all caches
		rvProbs = null;
		rvProbsSortable = null;
		mod = null;
	}
	
	private List<Double> rvCountTrack = Lists.newArrayList();
	
	private void loadRVProbs() {
		// loads in probabilities for rupture variations from the ETAS catalogs
		RuptureProbabilityModifier probMod = getRupProbModifier();
		
		double prob = 1d/catalogs.size();
		if (scenario == ETAS_CyberShake_Scenarios.TEST_NEGLIGABLE)
			// make the probability gain super small which should result if almost zero gain if implemented correctly
			prob = 1e-20;
		// TODO correctly deal with exceedence probs, as a rup can happen more than once in a catalog 
		
		double occurMult = 1d;
		if (normalizedTriggerRate > 0d) {
			// we're normalizing the rate of triggered events
			double actualTriggerCount = 0d;
			for (List<ETAS_EqkRupture> catalog : catalogs)
				actualTriggerCount += catalog.size();
			// this is the rate of triggered ruptures
			actualTriggerCount /= catalogs.size();
			
			occurMult = normalizedTriggerRate/actualTriggerCount;
		}
		
		// map from ID pairing to <rv ID, fractional num etas occurrences>
		Map<IDPairing, Map<Integer, Double>> rvOccurCountsMap = Maps.newHashMap();
		Map<IDPairing, List<Integer>> allRVsMap = Maps.newHashMap();
		
		fractOccurances = new double[ucerf2.getNumSources()][];
		
		double singleFractRate = 1d/(double)catalogs.size();
		
		for (List<ETAS_EqkRupture> catalog : catalogs) {
			for (ETAS_EqkRupture rup : catalog) {
				Location hypo = rup.getHypocenterLocation();
				Preconditions.checkNotNull(hypo);
				IDPairing pair = rupMappingTable.get(rup.getFSSIndex());
				Preconditions.checkNotNull(pair);
				double[] sourceOccurances = fractOccurances[pair.getID1()];
				if (sourceOccurances == null) {
					sourceOccurances = new double[ucerf2.getNumRuptures(pair.getID1())];
					fractOccurances[pair.getID1()] = sourceOccurances;
				}
				sourceOccurances[pair.getID2()] += singleFractRate;
				Map<Location, List<Integer>> rvHypoLocs = rvHypoLocations.get(rup.getFSSIndex());
				
				List<Integer> allRVsList = Lists.newArrayList();
				for (List<Integer> ids : rvHypoLocs.values())
					allRVsList.addAll(ids);
				allRVsMap.put(pair, allRVsList);
				
				double minDist = Double.POSITIVE_INFINITY;
				Location closestLoc = null;
				List<Location> locsWithinBuffer = Lists.newArrayList();
				for (Location loc : rvHypoLocs.keySet()) {
					double dist = LocationUtils.linearDistanceFast(loc, hypo);
					Preconditions.checkState(!loc.equals(closestLoc), "Duplicate locations!");
					if (dist < minDist) {
						minDist = dist;
						closestLoc = loc;
					}
					if (dist <= hypocenter_buffer_km)
						locsWithinBuffer.add(loc);
				}
				Preconditions.checkNotNull(closestLoc);
				Preconditions.checkState(minDist < 1000d, "No hypo match with 1000 km (closest="+minDist+")");
				
//				double myProb = prob;
				List<Integer> toBePromoted;
				if (locsWithinBuffer.size() < 2) {
					// just do the closest
					toBePromoted = Lists.newArrayList(rvHypoLocs.get(closestLoc));
				} else {
					// include all hypocenters within the buffer
					toBePromoted = Lists.newArrayList();
					for (Location hypoLoc : locsWithinBuffer) {
						toBePromoted.addAll(rvHypoLocs.get(hypoLoc));
					}
				}
				rvCountTrack.add((double)toBePromoted.size());
				Preconditions.checkState(toBePromoted.size() >= 1,
						"Should be more than one ID for each hypo (size="+toBePromoted.size()+")");
				
				Map<Integer, Double> rvCounts = rvOccurCountsMap.get(pair);
				if (rvCounts == null) {
					rvCounts = Maps.newHashMap();
					rvOccurCountsMap.put(pair, rvCounts);
				}
				
				// each mapped rv gets a fractional occurance, adding up to one
				double fractionalOccur = occurMult/(double)toBePromoted.size();
				for (int rvIndex : toBePromoted) {
					Double count = rvCounts.get(rvIndex);
					if (count == null)
						count = 0d;
					count += fractionalOccur;
					rvCounts.put(rvIndex, count);
				}
				
//				Map<Double, List<Integer>> rupRVProbs = rvProbs.get(pair);
//				if (rupRVProbs == null) {
//					rupRVProbs = Maps.newHashMap();
//					rvProbs.put(pair, rupRVProbs);
//					
//					if (calc_by_add_spontaneous) {
//						// now add in the regular probability
//						double initialProb = probMod.getModifiedProb(pair.getID1(), pair.getID2(), 0d);
//						List<Integer> allRVIndexes = Lists.newArrayList();
//						for (List<Integer> indexes : rvHypoLocs.values())
//							allRVIndexes.addAll(indexes);
//						// TODO???
////						rupRVProbs.put(initialProb/(double)allRVIndexes.size(), allRVIndexes);
//						rupRVProbs.put(initialProb, allRVIndexes);
//					}
//				}
//				
//				while (!toBePromoted.isEmpty()) {
//					List<Integer> newToBePromoted = Lists.newArrayList();
//					List<Integer> prevIDs = rupRVProbs.get(myProb);
//					if (prevIDs == null) {
//						prevIDs = Lists.newArrayList();
//						rupRVProbs.put(myProb, prevIDs);
//					}
//					for (int newID : toBePromoted) {
//						int index = prevIDs.indexOf(newID);
//						if (index >= 0) {
//							// this hypo now has additional probability, remove from this level and add it next time in the loop
//							prevIDs.remove(index);
//							newToBePromoted.add(newID);
//						} else {
//							prevIDs.add(newID);
//						}
//					}
//					
//					if (prevIDs.isEmpty())
//						rupRVProbs.remove(myProb);
//					
//					myProb += prob;
//					toBePromoted = newToBePromoted;
//				}
			}
		}
		
		// now build rv probs
		rvProbs = Maps.newHashMap();
		rvProbsSortable = Lists.newArrayList();
		
		for (IDPairing pair : rvOccurCountsMap.keySet()) {
			Map<Integer, Double> rvCounts = rvOccurCountsMap.get(pair);
			Preconditions.checkNotNull(rvCounts);
			Preconditions.checkState(!rvCounts.isEmpty());
			
			List<Integer> allRVsList = allRVsMap.get(pair);
			int totNumRVs = allRVsList.size();
			Preconditions.checkState(totNumRVs > 0);
			
			Map<Integer, Double> rvProbMap = Maps.newHashMap();
			
			if (calc_by_add_spontaneous) {
				// add in all RVs at original probability
				double startingProbPer;
				if (calc_by_treat_as_new_rupture)
					startingProbPer = 0d;
				else
					startingProbPer = rupProbMod.getModifiedProb(
							pair.getID1(), pair.getID2(), 0d)/(double)totNumRVs;
				for (Integer rvID : allRVsList)
					rvProbMap.put(rvID, startingProbPer);
			}
			
			// now add probability for each occurrence
			for (Integer rvID : rvCounts.keySet()) {
				double occur = rvCounts.get(rvID);
				double rvProb = occur * prob;
				if (rvProbMap.containsKey(rvID))
					rvProb += rvProbMap.get(rvID);
				rvProbMap.put(rvID, rvProb);
				Location hypocenter = hypoLocationsByRV.get(pair).get(rvID);
				double mag = ucerf2.getRupture(pair.getID1(), pair.getID2()).getMag();
				rvProbsSortable.add(new RVProbSortable(pair.getID1(), pair.getID2(), rvID, mag, hypocenter, occur, rvProb));
			}
			
			List<Double> rvProbsList = Lists.newArrayList();
			for (int rvID=0; rvID<totNumRVs; rvID++) {
				Double rvProb = rvProbMap.get(rvID);
				Preconditions.checkNotNull(rvProb);
				rvProbsList.add(rvProb);
			}
			
			rvProbs.put(pair, rvProbsList);
		}
	}
	
	public void printRVCountStats() {
		if (rvCountTrack.isEmpty())
			return;
		double[] vals = Doubles.toArray(rvCountTrack);
		System.out.println(scenario+" RV Counts:");
		System.out.println("\tMin:"+StatUtils.min(vals));
		System.out.println("\tMax:"+StatUtils.max(vals));
		System.out.println("\tMean:"+StatUtils.mean(vals));
		System.out.println("\tMedian:"+DataUtils.median(vals));
	}
	
	public void setTriggerAllHyposEqually(boolean triggerAllHyposEqually) {
		this.triggerAllHyposEqually = triggerAllHyposEqually;
	}
	
	private RuptureProbabilityModifier rupProbMod;

	@Override
	public synchronized RuptureProbabilityModifier getRupProbModifier() {
		if (rupProbMod != null)
			return rupProbMod;
		if (calc_by_add_spontaneous || scenario == ETAS_CyberShake_Scenarios.MAPPED_UCERF2) {
			final double aftRateCorr = 1d; // include aftershocks
			final double duration = timeSpan.getTimeYears();
			rupProbMod = new RuptureProbabilityModifier() {
				
				@Override
				public double getModifiedProb(int sourceID, int rupID, double origProb) {
					Integer fssIndex = rupMappingReverseTable.get(new IDPairing(sourceID, rupID));
					if (fssIndex == null)
						return 0d;
					double rupRate = sol.getRateForRup(fssIndex);
					double durationAdjustedRate = rupRate * duration;
					if (triggerAllHyposEqually && fractOccurances != null && fractOccurances[sourceID] != null)
						// this means we're applying rate increases to the whole rupture not just specifi RVs
						durationAdjustedRate += fractOccurances[sourceID][rupID];
					double prob = 1-Math.exp(-aftRateCorr*durationAdjustedRate);
					return prob;
				}
			};
		} else {
			rupProbMod = new ZeroProbMod();
		}
		return rupProbMod;
	}
	
	public synchronized void setCatalogs(List<List<ETAS_EqkRupture>> catalogs) {
		this.catalogs = catalogs;
		this.rvHypoLocations = null;
		this.rvProbs = null;
	}
	
	public List<List<ETAS_EqkRupture>> getCatalogs() {
		return catalogs;
	}

	@Override
	public synchronized RuptureVariationProbabilityModifier getRupVarProbModifier() {
		if (scenario == ETAS_CyberShake_Scenarios.MAPPED_UCERF2
				|| scenario == ETAS_CyberShake_Scenarios.MAPPED_UCERF2_TIMEDEP || triggerAllHyposEqually)
			return null;
		try {
			if (catalogs == null) {
				System.out.println("Loading catalogs for "+scenario.name);
				loadCatalogs(catalogsFile);
			}
			
			if (rvHypoLocations == null) {
				System.out.println("Loading Hypos for ETAS rups for "+scenario.name);
				loadHyposForETASRups();
			}

			if (rvProbs == null) {
				System.out.println("Loading RV probs for "+scenario.name);
				loadRVProbs();
				System.out.println("DONE loading RV probs for "+scenario.name);
			}
		} catch (IOException e) {
			ExceptionUtils.throwAsRuntimeException(e);
		}
		if (mod == null)
			mod = new RupProbMod();
		return mod;
	}
	
	public boolean isRupVarProbModifierByAddition() {
		return calc_by_treat_as_new_rupture;
	}
	
	private RupProbMod mod = null;
	private class RupProbMod implements RuptureVariationProbabilityModifier {
		
		@Override
		public List<Double> getVariationProbs(int sourceID,
				int rupID, double originalProb, CybershakeRun run, CybershakeIM im) {
			if (triggerAllHyposEqually)
				return null;
			return rvProbs.get(new IDPairing(sourceID, rupID));
		}
	}
	
	public Map<IDPairing, List<Double>> getRVProbs() {
		return rvProbs;
	}
	
	public Map<Integer, Map<Location, List<Integer>>> getHypoLocs() {
		return rvHypoLocations;
	}
	
	public Map<Integer, IDPairing> getRupMappingTable() {
		return rupMappingTable;
	}
	
	public ETAS_CyberShake_Scenarios getScenario() {
		return scenario;
	}
	
	public ETAS_Cybershake_TimeSpans getTimeSpan() {
		return timeSpan;
	}
	
	public Date getTimeSpanStart() {
		return new GregorianCalendar(2014, 0, 1).getTime();
	}
	
	IncrementalMagFreqDist writeTriggerMFD(File outputDir, String prefix) throws IOException {
		return writeTriggerMFD(outputDir, prefix, null);
	}
	
	IncrementalMagFreqDist writeTriggerMFD(File outputDir, String prefix, IncrementalMagFreqDist primaryMFD)
			throws IOException {
		getRupVarProbModifier(); // make sure everything has been loaded
		
		return writeTriggerMFD(outputDir, prefix, catalogs, scenario, timeSpan, null, getLongTermMFD(), primaryMFD, -1);
	}
	
	void writeTriggerMFDAnim(File outputDir, String prefix, int numPer) throws IOException {
		getRupVarProbModifier(); // make sure everything has been loaded
		
		int index = 0;
		
		int numDigits = ((catalogs.size()-1)+"").length();
		
		while (index < catalogs.size()) {
			index += numPer;
			if (index >= catalogs.size())
				index = catalogs.size();
			
			List<List<ETAS_EqkRupture>> subCat = catalogs.subList(0, index);
			
			String numStr = index+"";
			while (numStr.length() < numDigits)
				numStr = "0"+numStr;
			writeTriggerMFD(outputDir, prefix+"_"+numStr, subCat, scenario, timeSpan, index+" catalogs", null, null, numPer);
		}
	}
	
	static IncrementalMagFreqDist writeTriggerMFD(File outputDir, String prefix, List<List<ETAS_EqkRupture>> catalogs,
			ETAS_CyberShake_Scenarios scenario, ETAS_Cybershake_TimeSpans timeSpan, String annotation,
			IncrementalMagFreqDist longTermMFD, IncrementalMagFreqDist primaryMFD, int subIncr) throws IOException {
		IncrementalMagFreqDist incrMFD = new IncrementalMagFreqDist(6.05, 31, 0.1d);
		incrMFD.setName("Incremental MFD");
		
		// this will keep track of the MFD as catalog size increases
		List<IncrementalMagFreqDist> subMFDs = null;
		List<Integer> subMFDIndexes = null;
		if (subIncr > 0) {
			subMFDs = Lists.newArrayList();
			subMFDIndexes = Lists.newArrayList();
			for (int index=subIncr; index<catalogs.size()-1; index+=subIncr) {
				IncrementalMagFreqDist mfd = new IncrementalMagFreqDist(6.05, 31, 0.1d);
				mfd.setName(index+"");
				subMFDs.add(mfd);
				subMFDIndexes.add(index);
			}
		}
		
		double catRate = 1d/catalogs.size();
		
		int index = 0;
		for (List<ETAS_EqkRupture> catalog : catalogs) {
			for (ETAS_EqkRupture rup : catalog) {
//				System.out.println("Mag: "+rup.getMag()+", "+incrMFD.getMinX()+" => "+incrMFD.getMaxX());
				int mfdInd = incrMFD.getClosestXIndex(rup.getMag());
				incrMFD.add(mfdInd, catRate);
				if (subMFDs != null) {
					for (int i=0; i<subMFDs.size(); i++) {
						int testIndex = subMFDIndexes.get(i);
						if (index < subMFDIndexes.get(i))
							subMFDs.get(i).add(mfdInd, 1d/((double)testIndex));
					}
				}
			}
			index++;
		}
		
		List<DiscretizedFunc> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		EvenlyDiscretizedFunc cmlMFD = incrMFD.getCumRateDistWithOffset();
		cmlMFD.setName("Cumulative MFD");
		
		funcs.add(incrMFD);
//		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLUE));
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 2f, Color.BLUE));
		
		if (longTermMFD != null) {
			EvenlyDiscretizedFunc cmlLongTermMFD = longTermMFD.getCumRateDistWithOffset();
			cmlLongTermMFD.setName("Time Indep");
			funcs.add(cmlLongTermMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, new Color(130, 86, 5))); // BROWN
		}
		
		if (subMFDs != null) {
			double minVal = 0d;
			double maxVal = catalogs.size();
			CPT cpt = new CPT(minVal, maxVal, Color.GREEN.darker(), Color.BLACK);
			for (int i=0; i<subMFDs.size(); i++) {
				Color c = cpt.getColor((float)subMFDIndexes.get(i));
				funcs.add(subMFDs.get(i).getCumRateDistWithOffset());
				chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, c));
			}
		}
		
		if (primaryMFD != null) {
			primaryMFD.setName("Primary MFD");
			EvenlyDiscretizedFunc cmlPrimaryMFD = primaryMFD.getCumRateDistWithOffset();
			funcs.add(cmlPrimaryMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GREEN.darker()));
		}
		
		funcs.add(cmlMFD);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLACK));
		
		PlotSpec spec = new PlotSpec(funcs, chars, scenario.name+" Scenario Supra Seis MFD",
				"Magnitude", timeSpan.name+" Rate");
		spec.setLegendVisible(true);
		
		if (annotation != null) {
			XYTextAnnotation ann = new XYTextAnnotation(annotation, 8.25, 4e-1);
			ann.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 24));
			ann.setTextAnchor(TextAnchor.BASELINE_RIGHT);
			spec.setPlotAnnotations(Lists.newArrayList(ann));
		}
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		gp.setBackgroundColor(Color.WHITE);
		
		gp.setXLog(false);
		gp.setYLog(true);
		gp.setUserBounds(6d, 8.5d, 1e-6, 1e-1);
		gp.drawGraphPanel(spec);
		gp.getChartPanel().setSize(1000, 800);
		gp.saveAsPDF(new File(outputDir, prefix+".pdf").getAbsolutePath());
		gp.saveAsPNG(new File(outputDir, prefix+".png").getAbsolutePath());
		gp.saveAsTXT(new File(outputDir, prefix+".txt").getAbsolutePath());
		
		incrMFD.setName(scenario.toString());
		
		return incrMFD;
	}
	
	void writeTriggerVsIndepMFD(File outputDir, String prefix, IncrementalMagFreqDist incrMFD,
			IncrementalMagFreqDist indepMFD, Color color) throws IOException {
		List<DiscretizedFunc> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		EvenlyDiscretizedFunc cmlMFD = incrMFD.getCumRateDistWithOffset();
		cmlMFD.setName("BPT Time Dependent");
		funcs.add(cmlMFD);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, color));
		
		EvenlyDiscretizedFunc indepCmlMFD = indepMFD.getCumRateDistWithOffset();
		indepCmlMFD.setName("Poisson");
		funcs.add(indepCmlMFD);
		chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 3f, color));
		
		PlotSpec spec = new PlotSpec(funcs, chars, scenario.name+" Scenario Supra Seis MFD",
				"Magnitude", timeSpan.name+" Rate");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		gp.setBackgroundColor(Color.WHITE);
		
		gp.setXLog(false);
		gp.setYLog(true);
		gp.setUserBounds(6d, 8.5d, 1e-6, 1e-1);
		gp.drawGraphPanel(spec);
		gp.getChartPanel().setSize(1000, 800);
		gp.saveAsPDF(new File(outputDir, prefix+".pdf").getAbsolutePath());
		gp.saveAsPNG(new File(outputDir, prefix+".png").getAbsolutePath());
		gp.saveAsTXT(new File(outputDir, prefix+".txt").getAbsolutePath());
	}
	
	private IncrementalMagFreqDist longTermMFD = null;
	
	synchronized IncrementalMagFreqDist getLongTermMFD() {
		if (longTermMFD != null)
			return longTermMFD;
		longTermMFD = new IncrementalMagFreqDist(6.05, 31, 0.1d);
		double minMag = 6d;
		
		double rateMult = timeSpan.getTimeYears();
		
		for (int r=0; r<sol.getRupSet().getNumRuptures(); r++) {
			double rate = sol.getRateForRup(r);
			if (rate == 0d)
				continue;
			rate *= rateMult;
			double mag = sol.getRupSet().getMagForRup(r);
			if (mag < minMag)
				continue;
			longTermMFD.add(longTermMFD.getClosestXIndex(mag), rate);
		}
		
		return longTermMFD;
	}
	
	static void writeCombinedMFDs(File outputDir, List<? extends IncrementalMagFreqDist> mfds,
			List<Color> colors, IncrementalMagFreqDist longTermIndepMFD, IncrementalMagFreqDist longTermDepMFD,
			ETAS_Cybershake_TimeSpans timeSpan) throws IOException {
		List<DiscretizedFunc> funcs = Lists.newArrayList();
		List<PlotCurveCharacterstics> chars = Lists.newArrayList();
		
		if (longTermIndepMFD != null) {
			EvenlyDiscretizedFunc cmlLongTermMFD = longTermIndepMFD.getCumRateDistWithOffset();
			cmlLongTermMFD.setName("Time Indep");
			funcs.add(cmlLongTermMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.BLUE));
		}
		
		if (longTermDepMFD != null) {
			EvenlyDiscretizedFunc cmlLongTermMFD = longTermDepMFD.getCumRateDistWithOffset();
			cmlLongTermMFD.setName("BPT Time Dep");
			funcs.add(cmlLongTermMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 2f, Color.GREEN));
		}
		
		for (int i=0; i<mfds.size(); i++) {
			IncrementalMagFreqDist mfd = mfds.get(i);
			EvenlyDiscretizedFunc cmlMFD = mfd.getCumRateDistWithOffset();
			mfd.setName(mfd.getName());
			funcs.add(cmlMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, colors.get(i)));
		}
		
		System.out.println("Plotting combined MFD with "+funcs.size()+" Functions ("+mfds.size()+" input MFDs)");
		
		PlotSpec spec = new PlotSpec(funcs, chars, "Combined Scenario Supra Seis MFDs",
				"Magnitude", timeSpan.name+" Rate");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(20);
		gp.setPlotLabelFontSize(21);
		gp.setBackgroundColor(Color.WHITE);
		
		gp.setXLog(false);
		gp.setYLog(true);
		gp.setUserBounds(6d, 8.5d, 1e-6, 1e-1);
		gp.drawGraphPanel(spec);
		gp.getChartPanel().setSize(1000, 800);
		String prefix = "combined_mfds";
		gp.saveAsPDF(new File(outputDir, prefix+".pdf").getAbsolutePath());
		gp.saveAsPNG(new File(outputDir, prefix+".png").getAbsolutePath());
		gp.saveAsTXT(new File(outputDir, prefix+".txt").getAbsolutePath());
	}
	
	IncrementalMagFreqDist getPrimaryMFD(File cacheDir, FaultModels fm) throws IOException {
		if (!scenario.isETAS())
			return null;
		// the following makes me feel dirty and sad
		GriddedRegion griddedRegion = RELM_RegionUtils.getGriddedRegionInstance();
		double duration = timeSpan.getTimeYears();
		FaultSystemSolutionERF_ETAS erf = MPJ_ETAS_Simulator.buildERF(sol, false, duration);
		erf.updateForecast();
		double sourceRates[] = new double[erf.getNumSources()];
		for(int s=0;s<erf.getNumSources();s++) {
			ProbEqkSource src = erf.getSource(s);
			sourceRates[s] = src.computeTotalEquivMeanAnnualRate(duration);
		}
		double gridSeisDiscr = 0.1;
		ETAS_ParameterList etasParams = new ETAS_ParameterList();
		etasParams.setU3ETAS_ProbModel(U3ETAS_ProbabilityModelOptions.FULL_TD);
		ETAS_Utils etas_utils = new ETAS_Utils();
		File fractionSrcAtPointListFile = new File(cacheDir, "fractSectInCubeCache");
		File srcAtPointListFile = new File(cacheDir, "sectInCubeCache");
		File isCubeInsideFaultPolygonFile = new File(cacheDir, "cubeInsidePolyCache");
		Preconditions.checkState(fractionSrcAtPointListFile.exists(),
				"cache file not found: "+fractionSrcAtPointListFile.getAbsolutePath());
		Preconditions.checkState(srcAtPointListFile.exists(),
				"cache file not found: "+srcAtPointListFile.getAbsolutePath());
		Preconditions.checkState(isCubeInsideFaultPolygonFile.exists(),
				"cache file not found: "+isCubeInsideFaultPolygonFile.getAbsolutePath());
		List<float[]> fractionSrcAtPointList = MatrixIO.floatArraysListFromFile(fractionSrcAtPointListFile);
		List<int[]> srcAtPointList = MatrixIO.intArraysListFromFile(srcAtPointListFile);
		int[] isCubeInsideFaultPolygon = MatrixIO.intArrayFromFile(isCubeInsideFaultPolygonFile);
		ETAS_PrimaryEventSampler sampler = new ETAS_PrimaryEventSampler(griddedRegion, erf, sourceRates,
				gridSeisDiscr, null, etasParams, etas_utils, fractionSrcAtPointList, srcAtPointList, isCubeInsideFaultPolygon);
		
		ETAS_EqkRupture rupture = scenario.getRupture(ot, sol.getRupSet(), fm);
		long ot = Math.round((2014.0-1970.0)*ProbabilityModelsCalc.MILLISEC_PER_YEAR);
		IntegerPDF_FunctionSampler aveAveCubeSamplerForRup =
				sampler.getAveSamplerForRupture(rupture);

		double[] relSrcProbs = sampler.getRelativeTriggerProbOfEachSource(aveAveCubeSamplerForRup, 1.0, rupture);
		
		// list contains total, and supra seismogenic
		List<SummedMagFreqDist> expectedPrimaryMFD_PDF = sampler.getExpectedPrimaryMFD_PDF(relSrcProbs);
		IncrementalMagFreqDist supraMFD = expectedPrimaryMFD_PDF.get(1);
		
		// this is a PDF, need to scale by expected number of events. the total one (not just supra seis)
		// sums to one, the supra seis one here is different
		
		// start/end days are relative ot occurance time
		double startDay = 0;
		double endDay = timeSpan.getTimeDays();
		
		double expNum = ETAS_Utils.getExpectedNumEvents(etasParams.get_k(), etasParams.get_p(),
				rupture.getMag(), ETAS_Utils.magMin_DEFAULT, etasParams.get_c(), startDay, endDay);
		// scale to get actual expected number of EQs in each mag bin
		supraMFD.scale(expNum);
		
		return supraMFD;
	}
	
	List<RVProbSortable> getRVProbsSortable() {
		return rvProbsSortable;
	}
	
	public static class RVProbSortable implements Comparable<RVProbSortable> {
		
		private int sourceID, rupID;
		private List<Integer> rvIDs;
		private List<Location> hypocenters;
		private double mag;
		private double occurances;
		private double triggerRate;

		public RVProbSortable(int sourceID, int rupID, int rvID, double mag,
				Location hypocenter, double occurances, double triggerRate) {
			this.sourceID = sourceID;
			this.rupID = rupID;
			this.mag = mag;
			this.rvIDs = Lists.newArrayList(rvID);
			this.hypocenters = Lists.newArrayList(hypocenter);
			this.occurances = occurances;
			this.triggerRate = triggerRate;
		}

		public RVProbSortable(int sourceID, int rupID, double mag) {
			this.sourceID = sourceID;
			this.rupID = rupID;
			this.mag = mag;
			
			this.occurances = 0d;
			this.triggerRate = 0d;
			this.hypocenters = Lists.newArrayList();
			this.rvIDs = Lists.newArrayList();
		}

		@Override
		public int compareTo(RVProbSortable o) {
//			return Double.compare(o.triggerRate, triggerRate);
			return Double.compare(o.getTriggerMoRate(), getTriggerMoRate());
		}
		
		public void addRV(double occurances, double triggerRate, int rvID, Location hypocenter) {
			this.occurances += occurances;
			this.triggerRate += triggerRate;
			rvIDs.add(rvID);
			hypocenters.add(hypocenter);
		}
		
		public double getTriggerMoRate() {
			double moment = Math.pow(10, 1.5*(mag + 6d));
			return moment * triggerRate;
		}
		
		@Override
		public String toString() {
			return "Source: "+sourceID+", Rup: "+rupID+", RV: "+Joiner.on(",").join(rvIDs)+", Mag: "+mag
					+"\nHypocenter: "+Joiner.on(",").join(hypocenters)+"\noccur: "+occurances+", triggerRate: "+triggerRate;
		}

		public int getSourceID() {
			return sourceID;
		}

		public int getRupID() {
			return rupID;
		}

		public List<Integer> getRvIDs() {
			return rvIDs;
		}

		public List<Location> getHypocenters() {
			return hypocenters;
		}

		public double getOccurances() {
			return occurances;
		}

		public double getTriggerRate() {
			return triggerRate;
		}

		public double getMag() {
			return mag;
		}
		
	}
	
	ERF getCS_UCERF2_ERF() {
		return ucerf2;
	}
	
	public ERF getModERFforGMPE(final boolean gmpeDirectivity) {
		final double timeRateMultiplier = timeSpan.getTimeYears();
		return new AbstractERF() {
			
			@Override
			public String getName() {
				return scenario.name()+" ETAS MODIFIED UCERF2";
			}
			
			@Override
			public void updateForecast() {
				
			}
			
			@Override
			public ProbEqkSource getSource(int sourceID) {
				double probPerOccur;
				if (catalogs == null) {
					Preconditions.checkState(!scenario.isETAS());
					probPerOccur = 0;
				} else {
//					probPerOccur = 1d-Math.exp(-(1d/(double)catalogs.size()));
					probPerOccur = 1d/(double)catalogs.size();
				}
				final ProbEqkSource orig = ucerf2.getSource(sourceID);
				final List<ProbEqkRupture> modRups = Lists.newArrayList();
				for (int rupID=0; rupID<orig.getNumRuptures(); rupID++) {
					double fractOccur;
					if (fractOccurances != null && fractOccurances[sourceID] != null)
						fractOccur = fractOccurances[sourceID][rupID];
					else
						fractOccur = 0d;
					ProbEqkRupture origRup = orig.getRupture(rupID);
					double origProb = origRup.getProbability();
					double origRate = -Math.log(1d-origProb); // 1 year, don't need to divide by years
					double scaledOrigRate = origRate*timeRateMultiplier;
					double scaledOrigProb = 1-Math.exp(-scaledOrigRate);
					if (gmpeDirectivity && !triggerAllHyposEqually && fractOccur > 0d) {
						// first add normal rupture at original rate
						modRups.add(new ProbEqkRupture(origRup.getMag(), origRup.getAveRake(),
								scaledOrigProb, origRup.getRuptureSurface(), null));
						// now one for each hypocenter
						List<Location> hypos = gmpeHypoLocations.get(new IDPairing(sourceID, rupID));
						Preconditions.checkState(!hypos.isEmpty(), "Shouldn't be empty if fractOccur > 0");
						Preconditions.checkState(probPerOccur > 0);
						for (Location hypo : hypos)
							modRups.add(new ProbEqkRupture(origRup.getMag(), origRup.getAveRake(),
									probPerOccur, origRup.getRuptureSurface(), hypo));
					} else {
						// no directivity or never triggered
						double modProb;
						if (fractOccur > 0) {
							// convert to the correct time span and modify for trigger rate
							double modRate = scaledOrigRate + fractOccur;
							modProb = 1-Math.exp(-modRate);
						} else {
							modProb = scaledOrigProb;
						}
						modRups.add(new ProbEqkRupture(origRup.getMag(), origRup.getAveRake(),
								modProb, origRup.getRuptureSurface(), null));
					}
				}
				ProbEqkSource mod = new ProbEqkSource() {
					
					@Override
					public RuptureSurface getSourceSurface() {
						return orig.getSourceSurface();
					}
					
					@Override
					public LocationList getAllSourceLocs() {
						return orig.getAllSourceLocs();
					}
					
					@Override
					public ProbEqkRupture getRupture(int nRupture) {
						return modRups.get(nRupture);
					}
					
					@Override
					public int getNumRuptures() {
						return modRups.size();
					}
					
					@Override
					public double getMinDistance(Site site) {
						return orig.getMinDistance(site);
					}
				};
				return mod;
			}
			
			@Override
			public int getNumSources() {
				return ucerf2.getNumSources();
			}
		};
	}
	
	public static void main(String[] args) throws IOException, DocumentException {
		ETAS_Cybershake_TimeSpans timeSpan = ETAS_Cybershake_TimeSpans.ONE_WEEK;
		FaultSystemSolution sol = FaultSystemIO.loadSol(new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/ucerf2_mapped_sol.zip"));
		int erfID = 35;
		int rupVarScenID = 4;
		ETASModProbConfig conf = new ETASModProbConfig(ETAS_CyberShake_Scenarios.PARKFIELD, timeSpan, sol,
				new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/sims/2014_09_02-parkfield-nospont/results.zip"),
				new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/mappings.csv"), erfID, rupVarScenID);
//		ETASModProbConfig conf = new ETASModProbConfig(ETAS_CyberShake_Scenarios.BOMBAY_M6, timeSpan, sol,
//				new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/sims/2014_09_02-bombay_beach_m6-nospont/results.zip"),
//				new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/mappings.csv"));
//		ETASModProbConfig conf = new ETASModProbConfig(ETAS_CyberShake_Scenarios.MAPPED_UCERF2, timeSpan, sol,
//				new File[0],
//				new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/mappings.csv"));
		
		conf.writeTriggerMFD(new File("/home/kevin/OpenSHA/UCERF3/cybershake_etas/mfds"),
				conf.scenario.name().toLowerCase()+"_trigger_mfd");
		ETAS_MultiSimAnalysisTools.calcNumWithMagAbove(conf.catalogs, 0d);
		
		System.exit(0);
	}

}
