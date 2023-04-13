package org.opensha.sha.cybershake.constants;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jfree.data.Range;
import org.opensha.commons.calc.FaultMomentCalc;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.eq.MagUtils;
import org.opensha.commons.geo.Region;
import org.opensha.commons.geo.json.Feature;
import org.opensha.commons.geo.json.FeatureCollection;
import org.opensha.commons.geo.json.FeatureProperties;
import org.opensha.commons.geo.json.Geometry;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotPreferences;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.util.ComparablePairing;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.FileNameComparator;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.calc.UCERF2_AleatoryMagVarRemovalMod;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeRun.Status;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.RunIDFetcher;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.gui.util.ERFSaver;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.faultSysSolution.ruptures.util.RupSetMapMaker;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.magdist.GutenbergRichterMagFreqDist;
import org.opensha.sha.magdist.IncrementalMagFreqDist;
import org.opensha.sha.simulators.SimulatorElement;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.primitives.Doubles;

import scratch.UCERF3.enumTreeBranches.DeformationModels;
import scratch.UCERF3.enumTreeBranches.FaultModels;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.plots.AbstractPlot;
import scratch.kevin.simulators.plots.MagAreaScalingPlot;
import scratch.kevin.simulators.ruptures.BBP_PartBValidationConfig.Scenario;
import scratch.kevin.simulators.ruptures.rotation.RSQSimRotatedRupVariabilityConfig;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig;

public enum CyberShakeStudy {
	
	STUDY_14_2_1D(cal(2014, 2), 38, "Study 14.2 BBP 1D", "study_14_2_bbp_1d",
			"Los Angeles region with BBP 1-D Velocity Model, 0.5hz",
			35, 8,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(5).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_14_2_CVM_S426(cal(2014, 2), 35, "Study 14.2 CVM-S4.26", "study_14_2_cvms426",
			"Los Angeles region with CVM-S4.26 Velocity Model, 0.5hz",
			35, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(5).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_14_2_CVMH(cal(2014, 2), 34, "Study 14.2 CVM-H", "study_14_2_cvmh",
			"Los Angeles region with CVM-S4.26 Velocity Model, 0.5hz",
			35, 7,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(5).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_15_4(cal(2015, 4), 57, "Study 15.4", "study_15_4",
			"Los Angeles region with CVM-S4.26 Velocity Model, 1hz",
			36, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(6).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_15_12(cal(2015, 12), 61, "Study 15.12", "study_15_12",
			"Los Angeles region with CVM-S4.26 Velocity Model, 1hz deterministic with stochastic high frequencies",
			36, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(7).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_17_3_1D(cal(2017, 3), 80, "Study 17.3 1-D",
			"study_17_3_1d", "Central California with CCA-1D Velocity Model, 1hz",
			36, 9,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(8).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_17_3_3D(cal(2017, 3), 81, "Study 17.3 3-D",
			"study_17_3_3d", "Central California with CCA-06 Velocity Model, 1hz",
			36, 10,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forStudyID(8).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
	},
	STUDY_18_4_RSQSIM_PROTOTYPE_2457(cal(2018, 4), 82, "RSQSim 2457",
			"study_18_4_rsqsim_prototype_2457", "RSQSim prototype with catalog 2457",
			36, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir2457");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).hasHazardCurves(this.getDatasetIDs()).unique(true);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_2457.instance();
			return catalog;
		}
	},
	STUDY_18_4_RSQSIM_2585(cal(2018, 4), 83, "RSQSim 2585",
			"study_18_4_rsqsim_2585", "RSQSim prototype with catalog 2585 (1myr)",
			36, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir2585_1myr");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).hasHazardCurves(this.getDatasetIDs()).unique(true);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_2585_1MYR.instance();
			return catalog;
		}
	},
	STUDY_18_8(cal(2018, 8), 87, "Study 18.8", "study_18_8",
			"Northern California with Bay Area, CCA, and CVM-S4.26 Velocity Models, 1hz",
			36, 12,
			new CaliforniaRegions.CYBERSHAKE_BAY_AREA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).noTestSites().unique(true)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED).hasMeshVsitop();
		}
	},
	STUDY_18_9_RSQSIM_2740(cal(2018, 9), new int[] { 85, 86}, "RSQSim 2740",
			"study_18_9_rsqsim_2740", "RSQSim prototype with catalog 2740 (259kyr)",
			49, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir2740");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).hasHazardCurves(this.getDatasetIDs()).unique(true);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_2740.instance();
			return catalog;
		}
	},
	STUDY_19_2_RSQSIM_ROT_2740(cal(2019, 2), 88, "RSQSim RotRup 2740",
			"study_19_2_rsqsim_rot_2740", "RSQSim rotated-rupture variability study with catalog 2740",
			50, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimRotRupERF("rundir2740");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(true);
		}
		@Override
		public List<String> writeStandardDiagnosticPlots(File outputDir, int skipYears, double minMag, boolean replot,
				String topLink) throws IOException {
			// standard plots are not relevant for this variability study
			return new ArrayList<>();
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_2740.instance();
			return catalog;
		}
	},
	STUDY_19_3_RSQSIM_ROT_2585(cal(2019, 3), 89, "RSQSim RotRup 2585",
			"study_19_3_rsqsim_rot_2585", "RSQSim rotated-rupture variability study with catalog 2585",
			51, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimRotRupERF("rundir2585_1myr");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(true);
		}
		@Override
		public List<String> writeStandardDiagnosticPlots(File outputDir, int skipYears, double minMag, boolean replot,
				String topLink) throws IOException {
			// standard plots are not relevant for this variability study
			return new ArrayList<>();
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_2585_1MYR.instance();
			return catalog;
		}
	},
	STUDY_20_2_RSQSIM_4841(cal(2020, 2), new int[] { 92 }, "RSQSim 4841",
			"study_20_2_rsqsim_4841", "RSQSim prototype with catalog 4841 (134kyr)",
			53, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4841");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4841.instance();
			return catalog;
		}
	},
	STUDY_20_2_RSQSIM_4860(cal(2020, 2), new int[] { 92 }, "RSQSim 4860",
			"study_20_2_rsqsim_4860", "RSQSim prototype with catalog 4860 (35kyr)",
			54, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4860");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4860.instance();
			return catalog;
		}
	},
	STUDY_20_2_RSQSIM_4860_10X(cal(2020, 2), new int[] { 93 }, "RSQSim 4860 10x",
			"study_20_2_rsqsim_4860_10x", "RSQSim prototype with catalog 4860 (10x, 270kyr)",
			55, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4860_multi_combine");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4860_10X.instance();
			return catalog;
		}
	},
	STUDY_20_2_RSQSIM_ROT_4860_10X(cal(2020, 3), 94, "RSQSim RotRup 4860 10X",
			"study_20_2_rsqsim_rot_4860_10x", "RSQSim rotated-rupture variability study with catalog 4860 10X",
			56, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimRotRupERF("rundir4860_multi_combine");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(true);
		}
		@Override
		public List<String> writeStandardDiagnosticPlots(File outputDir, int skipYears, double minMag, boolean replot,
				String topLink) throws IOException {
			// standard plots are not relevant for this variability study
			return new ArrayList<>();
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4860_10X.instance();
			return catalog;
		}
	},
	STUDY_20_5_RSQSIM_4983(cal(2020, 5), 98, "RSQSim 4983",
			"study_20_5_rsqsim_4983", "RSQSim prototype with catalog 4983 (stitched, 4x, 775yr)",
			58, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4983_stitched");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4983_STITCHED.instance();
			return catalog;
		}
	},
	STUDY_20_5_RSQSIM_ROT_4983(cal(2020, 5), 100, "RSQSim RotRup 4983 10X",
			"study_20_5_rsqsim_rot_4983", "RSQSim rotated-rupture variability study with "
					+ "catalog 4983 (stitched, 4x, 775yr)",
			59, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimRotRupERF("rundir4983_stitched");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(true);
		}
		@Override
		public List<String> writeStandardDiagnosticPlots(File outputDir, int skipYears, double minMag, boolean replot,
				String topLink) throws IOException {
			// standard plots are not relevant for this variability study
			return new ArrayList<>();
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4983.instance();
			return catalog;
		}
	},
	STUDY_20_5_RSQSIM_4983_SKIP65k(cal(2020, 5), 98, "RSQSim 4983",
			"study_20_5_rsqsim_4983_skip65k", "RSQSim prototype with catalog 4983 (stitched, 4x, 715yr)",
			58, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4983_stitched");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4983_STITCHED.instance();
			return catalog;
		}
	},
	STUDY_21_12_RSQSIM_4983_SKIP65k_0p5hz(cal(2021, 12), 109, "RSQSim 4983 Production 0.5 Hz",
			"study_21_12_rsqsim_4983_skip65k", "RSQSim production with catalog 4983 (stitched, 4x, 715yr)",
			61, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4983_stitched");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4983_STITCHED.instance();
			return catalog;
		}
	},
	STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz(cal(2021, 12), 110, "RSQSim 4983 Production 1 Hz",
			"study_21_12_rsqsim_4983_skip65k_1hz", "RSQSim production with catalog 4983 (stitched, 4x, 715yr), 1hz",
			62, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4983_stitched");
		}
		@Override
		public RunIDFetcher runFetcher() {
//			return new RunIDFetcher(this.getDB()).forERF(62).hasAmplitudes().unique(false);
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4983_STITCHED.instance();
			return catalog;
		}
	},
	STUDY_22_1_RSQSIM_4983_SKIP65k_1Hz_CVMH(cal(2022, 1), 111, "RSQSim 4983 Production 1 Hz CVMH",
			"study_22_01_rsqsim_4983_skip65k_1hz_cvmh", "RSQSim production with catalog 4983 (stitched, 4x, 715yr), 1hz, CVMH",
			62, 4,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir4983_stitched");
		}
		@Override
		public RunIDFetcher runFetcher() {
//			return new RunIDFetcher(this.getDB()).forERF(62).hasAmplitudes().unique(false);
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_4983_STITCHED.instance();
			return catalog;
		}
	},
	STUDY_22_3_RSQSIM_5413(cal(2022, 3), 112, "RSQSim 5413",
			"study_21_12_rsqsim_5413", "RSQSim catalog 5413 (240kyr), 1hz",
			63, 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return getRSQSimERF("rundir5413");
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).forERF(getERF_ID()).hasAmplitudes().unique(false)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
//					.hasHazardCurves(this.getDatasetIDs());
		}
		private RSQSimCatalog catalog = null;
		@Override
		public synchronized RSQSimCatalog getRSQSimCatalog() {
			if (catalog == null)
				catalog = RSQSimCatalog.Catalogs.BRUCE_5413.instance();
			return catalog;
		}
	},
	STUDY_22_12_LF(cal(2022, 12), 115, "Study 22.12 LF", "study_22_12_lf",
			"Southern California with the Graves and Pitarka (2022) rupture generator (0-1 Hz).",
			36, 13,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			return new RunIDFetcher(this.getDB()).noTestSites().unique(false)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED).hasMeshVsitop();
//					.hasHazardCurves(this.getDatasetIDs()).hasAmplitudes(null).hasMeshVsitop();
		}
	},
	STUDY_22_12_HF(cal(2022, 12), 116, "Study 22.12 HF", "study_22_12_hf",
			"Southern California with stochastic high frequency simulations added (1-50 Hz) using SCEC Broadband "
			+ "Platform v22.4",
			36, 13,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME) {
		@Override
		public AbstractERF buildNewERF() {
			return MeanUCERF2_ToDB.createUCERF2ERF();
		}
		@Override
		public RunIDFetcher runFetcher() {
			// TODO require Vsitop?
			return new RunIDFetcher(this.getDB()).noTestSites().unique(false)
					.hasHazardCurves(this.getDatasetIDs()).forStatus(Status.VERIFIED);
//					.hasHazardCurves(this.getDatasetIDs()).hasAmplitudes(null).hasHazardCurves();
		}
	};
	
	public RSQSimCatalog getRSQSimCatalog() {
		return null;
	}
	
	private static AbstractERF getRSQSimERF(String catalogDirName) {
		File catDir = RSQSimCatalog.locateCatalog(catalogDirName, "erf_params.xml");
		Preconditions.checkState(catDir.exists(), "Could not find catalog dir for "+catalogDirName);
		File xmlFile = new File(catDir, "erf_params.xml");
		AbstractERF erf;
		try {
			erf = ERFSaver.LOAD_ERF_FROM_FILE(xmlFile.getAbsolutePath());
		} catch (Exception e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		erf.updateForecast();
		return erf;
	}
	
	private static AbstractERF getRSQSimRotRupERF(String catalogDirName) {
		File catDir = RSQSimCatalog.locateCatalog(catalogDirName, "cybershake_rotation_inputs");
		Preconditions.checkState(catDir.exists(), "Could not find catalog dir for "+catalogDirName);
		RSQSimCatalog catalog = new RSQSimCatalog(catDir, catalogDirName, FaultModels.FM3_1, DeformationModels.GEOLOGIC);
		File csRotDir = new File(catalog.getCatalogDir(), "cybershake_rotation_inputs");
		Map<Scenario, RSQSimRotatedRupVariabilityConfig> rotConfigs;
		try {
			rotConfigs = RSQSimRotatedRuptureFakeERF.loadRotationConfigs(catalog, csRotDir, false);
		} catch (IOException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		RSQSimRotatedRuptureFakeERF erf = new RSQSimRotatedRuptureFakeERF(catalog, rotConfigs);
		erf.updateForecast();
		return erf;
	}
	
	private int[] datasetIDs;
	private String name;
	private String dirName;
	private String description;
	private int erfID;
	private int velocityModelID;
	private Region region;
	private String dbHost;
	private GregorianCalendar date;

	private DBAccess db;
	private DBAccess writeDB;
	private Runs2DB runs2db;
	
	private CyberShakeStudy(GregorianCalendar date, int datasetID, String name, String dirName, String description,
			int erfID, int velocityModelID, Region region, String dbHost) {
		this(date, new int[] {datasetID}, name, dirName, description, erfID, velocityModelID, region, dbHost);
	}
	
	private CyberShakeStudy(GregorianCalendar date, int[] datasetIDs, String name, String dirName, String description,
			int erfID, int velocityModelID, Region region, String dbHost) {
		this.date = date;
		Preconditions.checkState(datasetIDs.length > 0);
		this.datasetIDs = datasetIDs;
		this.name = name;
		this.dirName = dirName;
		this.description = description;
		this.erfID = erfID;
		this.velocityModelID = velocityModelID;
		this.region = region;
		this.dbHost = dbHost;
	}
	
	private static GregorianCalendar cal(int year, int month) {
		return new GregorianCalendar(year, month-1, 1);
	}
	
	private static DateFormat dateFormat = new SimpleDateFormat("MMM yyyy");

	public int[] getDatasetIDs() {
		return datasetIDs;
	}

	public String getName() {
		return name;
	}

	public String getDirName() {
		return dirName;
	}

	public String getDescription() {
		return description;
	}

	public int getVelocityModelID() {
		return velocityModelID;
	}

	public int getERF_ID() {
		return erfID;
	}

	public Region getRegion() {
		return region;
	}

	public String getDBHost() {
		return dbHost;
	}
	
	public GregorianCalendar getDate() {
		return date;
	}
	
	public synchronized DBAccess getDB() {
		if (db == null)
			db = Cybershake_OpenSHA_DBApplication.getDB(dbHost);
		return db;
	}
	
	public synchronized DBAccess getWriteDB(boolean allowReadOnly, boolean exitOnCancel) {
		if (writeDB == null) {
			try {
				writeDB = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(
						exitOnCancel, allowReadOnly, dbHost);
			} catch (IOException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
		return writeDB;
	}
	
	private synchronized Runs2DB getRunsDB() {
		if (runs2db == null)
			runs2db = new Runs2DB(getDB());
		return runs2db;
	}
	
	private int getERF_ID(DBAccess db) {
		String sql = "SELECT ERF_ID FROM Hazard_Datasets WHERE Hazard_Dataset_ID="+getDatasetIDs()[0];
		
		try {
			ResultSet rs = db.selectData(sql);
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
	
	private int getRupVarScenarioID(DBAccess db) {
		String sql = "SELECT Rup_Var_Scenario_ID FROM Hazard_Datasets WHERE Hazard_Dataset_ID="+getDatasetIDs()[0];
		
		try {
			ResultSet rs = db.selectData(sql);
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public CybershakeVelocityModel getVelocityModel() {
		return getRunsDB().getVelocityModel(velocityModelID);
	}
	
	public List<String> getMarkdownMetadataTable() {
		TableBuilder builder = MarkdownUtils.tableBuilder();
		builder.addLine("**Name**", getName());
		builder.addLine("**Date**", dateFormat.format(date.getTime()));
		String regStr;
		if (getRegion() instanceof CaliforniaRegions.CYBERSHAKE_MAP_REGION)
			regStr = "Los Angeles Box";
		else if (getRegion() instanceof CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION)
			regStr = "Central California Box";
		else
			regStr = getRegion().getName();
		builder.addLine("**Region**", regStr);
		builder.addLine("**Description**", getDescription());
		CybershakeVelocityModel vm = getVelocityModel();
		builder.addLine("**Velocity Model**", vm.getName()+", "+vm.getVersion());
		return builder.build();
	}
	
	public void writeMarkdownSummary(File dir) throws IOException {
		writeMarkdownSummary(dir, false);
	}
	
	public void writeMarkdownSummary(File dir, boolean replot) throws IOException {
		List<String> lines = new LinkedList<>();
		String topLink = "*[(top)](#"+MarkdownUtils.getAnchorName(getName())+")*";
		lines.add("# "+getName());
		lines.add("## Metadata");
		lines.addAll(getMarkdownMetadataTable());
		lines.add("");
		int tocIndex = lines.size();
		
		Map<Vs30_Source, List<String>> gmpeLinksMap = new HashMap<>();
		Map<Vs30_Source, List<String>> gmpeNamesMap = new HashMap<>();
		
		Map<Vs30_Source, List<String>> nonErgodicLinksMap = new HashMap<>();
		Map<Vs30_Source, List<String>> nonErgodicNamesMap = new HashMap<>();
		
		Table<String, Vs30_Source, List<String>> siteHazardLinksTable = HashBasedTable.create();
		Table<String, Vs30_Source, List<String>> siteHazardNamesTable = HashBasedTable.create();
		
		Map<Vs30_Source, String> sourceSiteLinksMap = new HashMap<>();
		
		List<String> sourceDetailLinks = new ArrayList<>();
		List<String> sourceDetailNames = new ArrayList<>();
		
		String rotDDLink = null;
		
		String study_3d_vs_1d_link = null;
		
		String hazardMapLink = null;
		
		String bgHazardMapLink = null;
		
		List<String> rotatedRupLinks = new ArrayList<>();
		List<String> rotatedRupNames = new ArrayList<>();
		
		File[] dirList = dir.listFiles();
		Arrays.sort(dirList, new FileNameComparator());
		for (File subDir : dirList) {
			if (!subDir.isDirectory())
				continue;
			File mdFile = new File(subDir, "README.md");
			if (!mdFile.exists())
				continue;
			String name = subDir.getName();
			if (name.endsWith(".bak"))
				continue;
			if (name.startsWith("gmpe_comparisons_") && name.contains("_Vs30")) {
				String gmpeName = name.substring("gmpe_comparisons_".length());
				gmpeName = gmpeName.substring(0, gmpeName.indexOf("_Vs30"));
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				if (!gmpeLinksMap.containsKey(vs30)) {
					gmpeLinksMap.put(vs30, new ArrayList<>());
					gmpeNamesMap.put(vs30, new ArrayList<>());
				}
				
				gmpeLinksMap.get(vs30).add(name);
				gmpeNamesMap.get(vs30).add(gmpeName);
			} else if (name.startsWith("gmpe_non_ergodic_maps_") && name.contains("_Vs30")) {
				String gmpeName = name.substring("gmpe_non_ergodic_maps_".length());
				gmpeName = gmpeName.substring(0, gmpeName.indexOf("_Vs30"));
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				if (!nonErgodicLinksMap.containsKey(vs30)) {
					nonErgodicLinksMap.put(vs30, new ArrayList<>());
					nonErgodicNamesMap.put(vs30, new ArrayList<>());
				}
				
				nonErgodicLinksMap.get(vs30).add(name);
				nonErgodicNamesMap.get(vs30).add(gmpeName);
			}else if (name.equals("rotd_ratio_comparisons")) {
				Preconditions.checkState(rotDDLink == null, "Duplicate RotDD dirs! %s and %s", name, rotDDLink);
				rotDDLink = name;
			} else if (name.startsWith("site_hazard_")) {
				String siteName = name.substring("site_hazard_".length());
				siteName = siteName.substring(0, siteName.indexOf("_"));
				String gmpeName = name.substring("site_hazard_".length()+siteName.length()+1);
				gmpeName = gmpeName.substring(0, gmpeName.indexOf("_Vs30"));
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				if (!siteHazardNamesTable.contains(gmpeName, vs30)) {
					siteHazardLinksTable.put(gmpeName, vs30, new ArrayList<>());
					siteHazardNamesTable.put(gmpeName, vs30, new ArrayList<>());
				}
				
				siteHazardLinksTable.get(gmpeName, vs30).add(name);
				siteHazardNamesTable.get(gmpeName, vs30).add(siteName);
			} else if (name.startsWith("source_site_comparisons_Vs30")) {
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				sourceSiteLinksMap.put(vs30, name);
			} else if (name.startsWith("source_site_detail")) {
				sourceDetailLinks.add(name);
				sourceDetailNames.add(MarkdownUtils.getTitle(mdFile));
			} else if (name.equals("3d_1d_comparison")) {
				study_3d_vs_1d_link = name;
			} else if (name.equals("hazard_maps")) {
				hazardMapLink = name;
			} else if (name.equals("hazard_maps_back_seis")) {
				bgHazardMapLink = name;
			} else if (name.startsWith("rotated_ruptures_")) {
				for (Scenario scenario : Scenario.values()) {
					if (name.contains(scenario.getPrefix())) {
						rotatedRupLinks.add(name);
						rotatedRupNames.add(scenario.getName());
					}
				}
			}
		}
		
		if (hazardMapLink != null) {
			lines.add("");
			lines.add("## Hazard Maps");
			lines.add(topLink);
			lines.add("");
			lines.add("[Hazard Maps Plotted Here]("+hazardMapLink+"/)");
		}
		
		if (bgHazardMapLink != null) {
			lines.add("");
			lines.add("## Hazard Maps (with background seismicity)");
			lines.add(topLink);
			lines.add("");
			lines.add("[Background Seismicity Hazard Maps Plotted Here]("+bgHazardMapLink+"/)");
		}

		if (!gmpeLinksMap.isEmpty()) {
			lines.add("");
			lines.add("## GMPE Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Vs30_Source vs30 : gmpeLinksMap.keySet()) {
				if (gmpeLinksMap.keySet().size() > 1) {
					lines.add("### Vs30 model for GMPE comparisons: "+vs30);
					lines.add("");
				}
				List<String> gmpeNames = gmpeNamesMap.get(vs30);
				List<String> gmpeLinks = gmpeLinksMap.get(vs30);
				
				for (int i=0; i<gmpeNames.size(); i++)
					lines.add("* ["+gmpeNames.get(i)+"]("+gmpeLinks.get(i)+"/)");
			}
		}
		if (!nonErgodicLinksMap.isEmpty()) {
			lines.add("");
			lines.add("## GMPE Non-Ergodic Map Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Vs30_Source vs30 : nonErgodicLinksMap.keySet()) {
				if (gmpeLinksMap.keySet().size() > 1) {
					lines.add("### Vs30 model for GMPE comparisons: "+vs30);
					lines.add("");
				}
				List<String> gmpeNames = nonErgodicNamesMap.get(vs30);
				List<String> gmpeLinks = nonErgodicLinksMap.get(vs30);
				
				for (int i=0; i<gmpeNames.size(); i++)
					lines.add("* ["+gmpeNames.get(i)+"]("+gmpeLinks.get(i)+"/)");
			}
		}
		if (!siteHazardLinksTable.isEmpty()) {
			lines.add("");
			lines.add("## Site Hazard Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Cell<String, Vs30_Source, List<String>> cell : siteHazardLinksTable.cellSet()) {
				String gmpe = cell.getRowKey();
				Vs30_Source vs30 = cell.getColumnKey();
				lines.add("### GMPE: "+cell.getRowKey()+", Vs30 model: "+cell.getColumnKey());
				lines.add("");
				List<String> siteNames = siteHazardNamesTable.get(gmpe, vs30);
				List<String> siteLinks = siteHazardLinksTable.get(gmpe, vs30);
				
				for (int i=0; i<siteNames.size(); i++)
					lines.add("* ["+siteNames.get(i)+"]("+siteLinks.get(i)+"/)");
			}
		}
		if (!sourceSiteLinksMap.isEmpty()) {
			lines.add("");
			lines.add("## Source/Site Ground Motion Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Vs30_Source vs30 : sourceSiteLinksMap.keySet()) {
				String link = sourceSiteLinksMap.get(vs30);
				if (sourceSiteLinksMap.size() > 1) {
					lines.add("### Vs30 model: "+vs30);
					lines.add("");
					lines.add("* [Source/Site Ground Motion Comparisons with Vs30 from "+vs30+"]("+link+"/)");
				} else {
					lines.add("[Source/Site Ground Motion Comparisons Here]("+link+"/)");
				}
			}
		}
		if (!sourceDetailLinks.isEmpty()) {
			lines.add("");
			lines.add("## Source/Site Ground Motion Details");
			lines.add(topLink);
			lines.add("");
			for (int i=0; i<sourceDetailLinks.size(); i++)
				lines.add("* ["+sourceDetailNames.get(i)+"]("+sourceDetailLinks.get(i)+"/)");
		}
		if (rotDDLink != null) {
			lines.add("");
			lines.add("## RotD100/RotD50 Ratios");
			lines.add(topLink);
			lines.add("");
			lines.add("[RotD100/RotD50 Ratios Plotted Here]("+rotDDLink+"/)");
		}
		if (study_3d_vs_1d_link != null) {
			lines.add("");
			lines.add("## 3-D vs 1-D Comparisons");
			lines.add(topLink);
			lines.add("");
			lines.add("[3-D vs 1-D Comparisons Plotted Here]("+study_3d_vs_1d_link+"/)");
		}
		if (!rotatedRupLinks.isEmpty()) {
			lines.add("");
			lines.add("### Rotated Rupture Variability Comparisons");
			lines.add(topLink);
			lines.add("");
			for (int i=0; i<rotatedRupLinks.size(); i++)
				lines.add("* ["+rotatedRupNames.get(i)+"]("+rotatedRupLinks.get(i)+"/)");
		}
		
		File resourcesDir = new File(dir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		lines.add("");
		lines.addAll(writeStandardDiagnosticPlots(resourcesDir, 5000, 6d, replot, topLink));
		
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2));
		
		MarkdownUtils.writeReadmeAndHTML(lines, dir);
	}
	
	public List<String> writeStandardDiagnosticPlots(File outputDir, int skipYears, double minMag, boolean replot, String topLink)
			throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("## Plots");
		
		TableBuilder table;
		
		File siteMapGeoJSON = new File(outputDir, "sites.geojson");
		if (replot || !siteMapGeoJSON.exists()) {
			writeSiteGeoJSON(siteMapGeoJSON);
			
			if (siteMapGeoJSON.exists()) {
				lines.add("### Site Map");
				lines.add(topLink);
				
				lines.add("");
				String relLink = outputDir.getName()+"/"+siteMapGeoJSON.getName();
				lines.add("[Download Sites GeoJSON]("+relLink+") "
						+RupSetMapMaker.getGeoJSONViewerRelativeLink("View Sites GeoJSON", relLink));
				lines.add("");
			}
		}
		
		if (replot || !new File(outputDir, "mfd.png").exists()) {
			writeMFD(outputDir, "mfd", null);
		}
		lines.add("### Magnitude-Frequency Plot");
		lines.add(topLink);
		lines.add("");
		lines.add("![MFD]("+outputDir.getName()+"/mfd.png)");
		
		if (replot || !new File(outputDir, "rv_count.png").exists()) {
			writeRVCount(outputDir, "rv_count", null);
		}
		lines.add("### Rupture Variation Count Plot");
		lines.add(topLink);
		lines.add("");
		lines.add("![RV Count]("+outputDir.getName()+"/rv_count.png)");
		
		if (replot || !new File(outputDir, "mag_area_hist2D.png").exists()
				|| !new File(outputDir, "slip_area_hist2D.png").exists()) {
			writeMagSlipAreaPlots(outputDir, "", null);
		}
		lines.add("### Magnitude-Area Plots");
		lines.add(topLink);
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.addLine("Scatter", "2-D Hist");
		table.initNewLine();
		table.addColumn("![MFD Scatter]("+outputDir.getName()+"/mag_area.png)");
		table.addColumn("![MFD Hist]("+outputDir.getName()+"/mag_area_hist2D.png)");
		table.finalizeLine();
		lines.addAll(table.build());

		lines.add("### Slip-Area Plots");
		lines.add(topLink);
		lines.add("");
		table = MarkdownUtils.tableBuilder();
		table.addLine("Scatter", "2-D Hist");
		table.initNewLine();
		table.addColumn("![Slip Scatter]("+outputDir.getName()+"/slip_area.png)");
		table.addColumn("![Slip Hist]("+outputDir.getName()+"/slip_area_hist2D.png)");
		table.finalizeLine();
		lines.addAll(table.build());
		
		AbstractERF erf = getERF();
		if (new File(outputDir, "mfd_no_aleatory.png").exists()
				|| erf != null && (erf instanceof MeanUCERF2 || erf.getName().startsWith(MeanUCERF2.NAME))) {
			UCERF2_AleatoryMagVarRemovalMod probMod = new UCERF2_AleatoryMagVarRemovalMod(erf);
			
			lines.add("### Plots Without Aleatory Magnitude Variability");
			
			if (replot || !new File(outputDir, "mfd_no_aleatory.png").exists()) {
				writeMFD(outputDir, "mfd_no_aleatory", probMod);
			}
			lines.add("#### No-Aleatory Magnitude-Frequency Plot");
			lines.add(topLink);
			lines.add("");
			lines.add("![MFD]("+outputDir.getName()+"/mfd_no_aleatory.png)");
			
			if (replot || !new File(outputDir, "rv_count_no_aleatory.png").exists()) {
				writeRVCount(outputDir, "rv_count_no_aleatory", probMod);
			}
			lines.add("#### No-Aleatory Rupture Variation Count Plot");
			lines.add(topLink);
			lines.add("");
			lines.add("![RV Count]("+outputDir.getName()+"/rv_count_no_aleatory.png)");
			
			if (replot || !new File(outputDir, "mag_area_no_aleatory_hist2D.png").exists()
					|| !new File(outputDir, "slip_area_no_aleatory_hist2D.png").exists()) {
				writeMagSlipAreaPlots(outputDir, "_no_aleatory", probMod);
			}
			lines.add("#### No-Aleatory Magnitude-Area Plots");
			lines.add(topLink);
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Scatter", "2-D Hist");
			table.initNewLine();
			table.addColumn("![MFD Scatter]("+outputDir.getName()+"/mag_area_no_aleatory.png)");
			table.addColumn("![MFD Hist]("+outputDir.getName()+"/mag_area_no_aleatory_hist2D.png)");
			table.finalizeLine();
			lines.addAll(table.build());
			
			lines.add("#### No-Aleatory Slip-Area Plots");
			lines.add(topLink);
			lines.add("");
			table = MarkdownUtils.tableBuilder();
			table.addLine("Scatter", "2-D Hist");
			table.initNewLine();
			table.addColumn("![Slip Scatter]("+outputDir.getName()+"/slip_area_no_aleatory.png)");
			table.addColumn("![Slip Hist]("+outputDir.getName()+"/slip_area_no_aleatory_hist2D.png)");
			table.finalizeLine();
			lines.addAll(table.build());
		}
		
		return lines;
	}
	
	private void writeSiteGeoJSON(File outputFile) throws IOException {
		List<Feature> features = new ArrayList<>();
		
		List<CybershakeRun> runs = runFetcher().fetch();
		List<Site> sites;
		try {
			sites = CyberShakeSiteBuilder.buildSites(this, Vs30_Source.Simulation, runs);
		} catch (Exception e) {
			System.err.println("Warning: skipping site map for "+this.getName()+": "+e.getMessage());
			return;
		}
		for (Site site : sites) {
			CyberShakeSiteRun siteRun = (CyberShakeSiteRun)site;
			FeatureProperties props = new FeatureProperties();
			
			props.set("id", siteRun.getCS_Site().id);
			props.set("name", siteRun.getCS_Site().name);
			props.set("short_name", siteRun.getCS_Site().short_name);
			props.set("type_id", siteRun.getCS_Site().type_id);
			props.set("lat", siteRun.getCS_Site().lat);
			props.set("lon", siteRun.getCS_Site().lon);
			props.set("run_id", siteRun.getCS_Run().getRunID());
			props.set("model_vs30", siteRun.getCS_Run().getModelVs30());
			props.set("mesh_vsitop", siteRun.getCS_Run().getMeshVsitop());
			props.set("z1.0", siteRun.getCS_Run().getZ10());
			props.set("z2.5", siteRun.getCS_Run().getZ25());
			
			features.add(new Feature(site.getName(), new Geometry.Point(site.getLocation()), props));
		}
		FeatureCollection collection = new FeatureCollection(features);
		
		FeatureCollection.write(collection, outputFile);
	}
	
	private void writeMagSlipAreaPlots(File outputDir, String prefixAdd, RuptureProbabilityModifier probMod) throws IOException {
		DefaultXY_DataSet maScatter = new DefaultXY_DataSet();
		DefaultXY_DataSet slipScatter = new DefaultXY_DataSet();
		AbstractERF erf = getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				if (rup.getProbability() == 0d
						|| probMod != null && probMod.getModifiedProb(sourceID, rupID, rup.getProbability()) == 0d)
					continue;
				double area;
				if (rup instanceof RSQSimProbEqkRup) {
					area = 0;
					for (SimulatorElement elem : ((RSQSimProbEqkRup)rup).getElements())
						area += elem.getArea();
					area /= 1e6; // m^2 to km^2
				} else {
					area = rup.getRuptureSurface().getArea();
				}
				maScatter.set(area, rup.getMag());
				double moment = MagUtils.magToMoment(rup.getMag());
				double meanSlip = FaultMomentCalc.getSlip(area*1e6, moment);
				slipScatter.set(area, meanSlip);
			}
		}
		MagAreaScalingPlot.writeScatterPlots(maScatter, false, getName(), outputDir, "mag_area"+prefixAdd, 650, 600);
		MagAreaScalingPlot.writeScatterPlots(slipScatter, true, getName(), outputDir, "slip_area"+prefixAdd, 650, 600);
	}
	
	private void writeMFD(File outputDir, String prefix, RuptureProbabilityModifier probMod) throws IOException {
		IncrementalMagFreqDist mfd = buildMagFunc();
		Range xRange = new Range(mfd.getMinX() - 0.5*mfd.getDelta(), mfd.getMaxX()+0.5*mfd.getDelta());
		AbstractERF erf = getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double mag = rup.getMag();
				if (xRange.contains(mag)) {
					double rate;
					if (probMod != null) {
						double origProb = rup.getProbability();
						double newProb = probMod.getModifiedProb(sourceID, rupID, origProb);
						if (newProb == 0d)
							continue;
						rate = -Math.log(1d - newProb);
					} else {
						rate = rup.getMeanAnnualRate(1d);
					}
					mfd.add(mfd.getClosestXIndex(mag), rate);
				}
			}
		}
		EvenlyDiscretizedFunc cumulative = mfd.getCumRateDistWithOffset();
		
		writeMagPlot(outputDir, prefix, "Annual Rate", "Magnitude-Frequency Distribution", mfd, cumulative, true, xRange);
	}
	
	private void writeRVCount(File outputDir, String prefix, RuptureProbabilityModifier probMod) throws IOException {
		IncrementalMagFreqDist mnd = buildMagFunc();
		Range xRange = new Range(mnd.getMinX() - 0.5*mnd.getDelta(), mnd.getMaxX()+0.5*mnd.getDelta());
		DBAccess db = getDB();
		int erfID = getERF_ID(db);
		int rvScenID = getRupVarScenarioID(db);
		AbstractERF erf = getERF();
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double mag = rup.getMag();
				if (rup.getProbability() == 0d
						|| probMod != null && probMod.getModifiedProb(sourceID, rupID, rup.getProbability()) == 0d)
					continue;
				if (xRange.contains(mag)) {
					String sql = "SELECT count(*) FROM Rupture_Variations WHERE ERF_ID="+erfID+" AND Rup_Var_Scenario_ID="+rvScenID
							+" AND Source_ID="+sourceID+" AND Rupture_ID="+rupID;
					try {
						ResultSet rs = db.selectData(sql);
						rs.next();
						int count = rs.getInt(1);
						if (count > 0)
							mnd.add(mnd.getClosestXIndex(mag), (double)count);
					} catch (SQLException e) {
						throw new IllegalStateException(e);
					}
					
				}
			}
		}
		EvenlyDiscretizedFunc cumulative = mnd.getCumRateDistWithOffset();
		
		writeMagPlot(outputDir, prefix, "Number", "Rupture Variation Count", mnd, cumulative, false, xRange);
	}
	
	private void writeMagPlot(File outputDir, String prefix, String yAxisLabel, String title, EvenlyDiscretizedFunc incremental,
			EvenlyDiscretizedFunc cumulative, boolean doGR, Range xRange) throws IOException {
		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		funcs.add(incremental);
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.GRAY));
		incremental.setName(getName()+" Incremental");

		funcs.add(cumulative);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLACK));
		cumulative.setName("Cumulative");
		
		if (doGR) {
			double totCmlRate = cumulative.getY(0);
			GutenbergRichterMagFreqDist grMFD = new GutenbergRichterMagFreqDist(1d, 1d,
					xRange.getLowerBound(), xRange.getUpperBound(), 100);
			grMFD.scaleToIncrRate(0, totCmlRate);
			grMFD.setName("G-R B=1");
			funcs.add(grMFD);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 1f, Color.GRAY));
		}
		
		String xAxisLabel = "Magnitude";
		
		PlotSpec plot = new PlotSpec(funcs, chars, title, xAxisLabel, yAxisLabel);
		plot.setLegendVisible(true);
		
		double minY = Double.POSITIVE_INFINITY;
		double maxY = 0d;
		for (DiscretizedFunc func : funcs) {
			minY = Math.min(minY, AbstractPlot.minNonZero(func));
			maxY = Math.max(maxY, func.getMaxY());
//			System.out.println(func);
		}
		Range yRange;
		if (!Doubles.isFinite(minY))
			yRange = new Range(1d, 10d);
		else
			yRange = AbstractPlot.calcEncompassingLog10Range(minY, maxY);
		
		PlotPreferences plotPrefs = PlotPreferences.getDefault();
		plotPrefs.setTickLabelFontSize(20);
		plotPrefs.setAxisLabelFontSize(22);
		plotPrefs.setPlotLabelFontSize(24);
		plotPrefs.setLegendFontSize(22);
		plotPrefs.setBackgroundColor(Color.WHITE);
		HeadlessGraphPanel gp = new HeadlessGraphPanel(plotPrefs);
		
		gp.drawGraphPanel(plot, false, true, xRange, yRange);
		gp.getChartPanel().setSize(650, 600);
		gp.saveAsPNG(new File(outputDir, prefix+".png").getAbsolutePath());
		gp.saveAsPDF(new File(outputDir, prefix+".pdf").getAbsolutePath());
		gp.saveAsTXT(new File(outputDir, prefix+".txt").getAbsolutePath());
	}
	
	private static IncrementalMagFreqDist buildMagFunc() {
		double minMag = 6;
		double delta = 0.1;
		int num = (int)((9d - minMag)/delta);
		
		IncrementalMagFreqDist mfd = new IncrementalMagFreqDist(minMag+0.5*delta, num, delta);
		
		return mfd;
	}
	
	public static void writeStudiesIndex(File dir) throws IOException {
		// sort by date, newest first
		List<Long> times = new ArrayList<>();
		List<CyberShakeStudy> studies = new ArrayList<>();
		for (File subDir : dir.listFiles()) {
			if (!subDir.isDirectory())
				continue;
			CyberShakeStudy study = null;
			for (CyberShakeStudy s : values()) {
				if (subDir.getName().equals(s.getDirName())) {
					study = s;
					break;
				}
			}
			if (study == null)
				continue;
			studies.add(study);
			times.add(study.date.getTimeInMillis());
		}
		studies = ComparablePairing.getSortedData(times, studies);
		Collections.reverse(studies);
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Date", "Name", "Description");
		for (CyberShakeStudy study : studies) {
			table.initNewLine();
			
			table.addColumn(dateFormat.format(study.date.getTime()));
			table.addColumn("["+study.getName()+"]("+study.getDirName()+"#"+MarkdownUtils.getAnchorName(study.getName())+")");
			table.addColumn(study.getDescription());
			
			table.finalizeLine();
		}
		
		List<String> lines = new LinkedList<>();
		lines.add("# CyberShake Study Analysis");
		lines.add("");
		lines.addAll(table.build());
		
		MarkdownUtils.writeReadmeAndHTML(lines, dir);
	}
	
	public abstract AbstractERF buildNewERF();
	
	private AbstractERF erf;
	public synchronized AbstractERF getERF() {
		if (erf == null)
			erf = buildNewERF();
		return erf;
	}
	
	public abstract RunIDFetcher runFetcher();
	
	public static void main(String[] args) throws IOException {
		File gitDir = new File("/home/kevin/markdown/cybershake-analysis");
		
		boolean replot = false;
		
		for (CyberShakeStudy study : CyberShakeStudy.values()) {
			System.out.println("Processing "+study.getName());
			File studyDir = new File(gitDir, study.getDirName());
			Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
			study.writeMarkdownSummary(studyDir, replot);
		}
		
		System.out.println("Writing index");
		
		writeStudiesIndex(gitDir);
		
		System.out.println("DONE");
		
		for (CyberShakeStudy study : CyberShakeStudy.values())
			study.getDB().destroy();
	}

}
