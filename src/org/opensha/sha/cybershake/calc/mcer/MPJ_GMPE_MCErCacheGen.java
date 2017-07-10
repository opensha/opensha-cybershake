package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import mpi.MPI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.hpc.mpj.taskDispatch.MPJTaskCalculator;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.XMLUtils;
import org.opensha.commons.util.binFile.BinaryDoubleScalarRandomAccessFile;
import org.opensha.commons.util.binFile.BinaryGeoDatasetRandomAccessFile;
import org.opensha.commons.util.threads.Task;
import org.opensha.commons.util.threads.ThreadedTaskComputer;
import org.opensha.sha.calc.hazardMap.components.BinaryCurveArchiver;
import org.opensha.sha.calc.hazardMap.components.CurveMetadata;
import org.opensha.sha.calc.mcer.ASCEDetLowerLimitCalc;
import org.opensha.sha.calc.mcer.AbstractMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.AbstractMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedCurveBasedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedMCErDeterministicCalc;
import org.opensha.sha.calc.mcer.CachedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.GMPE_MCErDeterministicCalc;
import org.opensha.sha.calc.mcer.GMPE_MCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.cybershake.calc.RuptureProbabilityModifier;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.gui.util.AttenRelSaver;
import org.opensha.sha.cybershake.gui.util.ERFSaver;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.gui.infoTools.IMT_Info;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MPJ_GMPE_MCErCacheGen extends MPJTaskCalculator {
	
	private List<Double> periods;
	private DiscretizedFunc periodsFunc;
	private List<CalcTask> calcTasks;
	
//	private AbstractMCErDeterministicCalc[] detCalcs;
//	private AbstractMCErProbabilisticCalc[] probCalcs;
	private CalcRunnable[] calcs;
	private String[] cachePrefixes;
	
	private static final boolean duplicateERF = false;
	
	private BinaryCurveArchiver archiver;
	private Map<String, BinaryGeoDatasetRandomAccessFile> pgaFiles;
	
	public MPJ_GMPE_MCErCacheGen(CommandLine cmd) throws InvocationTargetException, MalformedURLException, DocumentException {
		super(cmd);
		
		int numThreads = getNumThreads();
		Preconditions.checkState(numThreads >= 1);
		
		List<List<AttenuationRelationship>> attenRelsList = Lists.newArrayList();
		for (int t=0; t<numThreads; t++) {
			List<AttenuationRelationship> attenRels = Lists.newArrayList();
			
			for (String attenRelFile : DataUtils.commaSplit(cmd.getOptionValue("atten-rel-file"))) {
				AttenuationRelationship attenRel = AttenRelSaver.LOAD_ATTEN_REL_FROM_FILE(attenRelFile);
				attenRels.add(attenRel);
			}
			
			attenRelsList.add(attenRels);
		}
		
		ERF[] erfs = new ERF[numThreads];
		ERF[] detERFs = new ERF[numThreads];
		
		if (duplicateERF) {
			// build ERFs in parallel
			List<Thread> erfInitThreads = Lists.newArrayList();
			for (int i=0; i<numThreads; i++) {
				Thread t = new Thread(new ERFInitRunnable(i, erfs, detERFs, cmd.getOptionValue("erf-file")));
				t.start();
				erfInitThreads.add(t);
			}
			for (int i=0; i<erfInitThreads.size(); i++) {
				Thread t = erfInitThreads.get(i);
				try {
					t.join();
				} catch (InterruptedException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
				Preconditions.checkNotNull(erfs[i]);
			}
		} else {
			// shared ERF
			new ERFInitRunnable(0, erfs, detERFs, cmd.getOptionValue("erf-file")).run();
			for (int i=1; i<numThreads; i++) {
				erfs[i] = erfs[0];
				detERFs[i] = detERFs[0];
			}
		}
		
		String periodStrs;
		if (cmd.hasOption("period"))
			periodStrs = cmd.getOptionValue("period");
		else
			periodStrs = MCERDataProductsCalc.default_periods;
		periods = DataUtils.commaDoubleSplit(periodStrs);
		
		File outputDir = new File(cmd.getOptionValue("output-dir"));
		if (rank == 0)
			Preconditions.checkArgument((outputDir.exists() && outputDir.isDirectory()) || outputDir.mkdir(),
				"Output dir does not exist and could not be created");
		
		CyberShakeComponent comp = CybershakeIM.fromShortName(cmd.getOptionValue("component"), CyberShakeComponent.class);
		
		File sitesFile = new File(cmd.getOptionValue("sites"));
		Preconditions.checkState(sitesFile.exists(), "Sites file doesn't exist: %s", sitesFile);
		
		// load sites
		Document siteDoc = XMLUtils.loadDocument(sitesFile);
		List<Site> sites = Lists.newArrayList();
		
		ParameterList siteParams = MCERDataProductsCalc.getSiteParams(attenRelsList.get(0));
		
		for (Element elem : XMLUtils.getSubElementsList(siteDoc.getRootElement(), "Site")) {
			// will clone site params list
			sites.add(Site.fromXMLMetadata(elem, siteParams));
		}
		
		Collections.sort(sites, new SiteComparator());
		
		// create calcs
		calcs = new CalcRunnable[numThreads];
		cachePrefixes = new String[attenRelsList.get(0).size()];
		
		Component gmpeComp = MCErCalcUtils.getSupportedTranslationComponent(
				attenRelsList.get(0).get(0), comp.getGMPESupportedComponents());
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		
		periodsFunc = new ArbitrarilyDiscretizedFunc();
		for (double period : periods)
			periodsFunc.set(period, 0d);
		
		Map<String, DiscretizedFunc> xValsMap = Maps.newHashMap();
		pgaFiles = Maps.newHashMap();
		
		for (int t=0; t<numThreads; t++) {
			List<AttenuationRelationship> attenRels = attenRelsList.get(t);
			GMPE_MCErDeterministicCalc[] detCalcs = new GMPE_MCErDeterministicCalc[attenRels.size()];
			GMPE_MCErProbabilisticCalc[] probCalcs = new GMPE_MCErProbabilisticCalc[attenRels.size()];
			for (int i=0; i<attenRels.size(); i++) {
				cachePrefixes[i] = CyberShakeMCErMapGenerator.getCachePrefix(
						-1, erfs[t], gmpeComp, Lists.newArrayList(attenRels.get(i)));
				detCalcs[i] = new GMPE_MCErDeterministicCalc(detERFs[t], attenRels.get(i), gmpeComp);
				probCalcs[i] = new GMPE_MCErProbabilisticCalc(erfs[t], attenRels.get(i), gmpeComp, xVals);
				
				if (t == 0) {
					xValsMap.put(cachePrefixes[i]+"_det", periodsFunc);
					xValsMap.put(cachePrefixes[i]+"_prob", periodsFunc);
					xValsMap.put(cachePrefixes[i]+"_mcer", periodsFunc);
					
					pgaFiles.put(cachePrefixes[i]+"_pga_det",
							new BinaryGeoDatasetRandomAccessFile(new File(outputDir, cachePrefixes[i]+"_pga_det.bin"),
									BinaryCurveArchiver.byteOrder, sites.size()));
					pgaFiles.put(cachePrefixes[i]+"_pga_prob",
							new BinaryGeoDatasetRandomAccessFile(new File(outputDir, cachePrefixes[i]+"_pga_prob.bin"),
									BinaryCurveArchiver.byteOrder, sites.size()));
					pgaFiles.put(cachePrefixes[i]+"_pga",
							new BinaryGeoDatasetRandomAccessFile(new File(outputDir, cachePrefixes[i]+"_pga.bin"),
									BinaryCurveArchiver.byteOrder, sites.size()));
				}
			}
			calcs[t] = new CalcRunnable(detCalcs, probCalcs);
		}
		
		archiver = new BinaryCurveArchiver(outputDir, sites.size(), xValsMap);
		if (rank == 0) {
			archiver.initialize();
			try {
				for (BinaryGeoDatasetRandomAccessFile pgaFile : pgaFiles.values())
					pgaFile.initialize();
			} catch (IOException e) {
				ExceptionUtils.throwAsRuntimeException(e);
			}
		}
		
		calcTasks = Lists.newArrayList();
		for (int i=0; i<sites.size(); i++) {
			Site site = sites.get(i);
			for (int j=0; j<cachePrefixes.length; j++)
				calcTasks.add(new CalcTask(cachePrefixes[j], i, j, site));
		}
	}
	
	private class ERFInitRunnable implements Runnable {
		private int index;
		private ERF[] erfs;
		private ERF[] detERFs;
		private String erfPath;

		public ERFInitRunnable(int index, ERF[] erfs, ERF[] detERFs, String erfPath) {
			this.index = index;
			this.erfs = erfs;
			this.detERFs = detERFs;
			this.erfPath = erfPath;
		}

		@Override
		public void run() {
			try {
				erfs[index] = ERFSaver.LOAD_ERF_FROM_FILE(erfPath);
				
				RuptureProbabilityModifier detProbMod = CyberShakeMCErDeterministicCalc.getProbMod(erfs[index]);
				detERFs[index] = MCERDataProductsCalc.getGMPEDetERF(erfs[index], detProbMod);
				
				erfs[index].updateForecast();
			} catch (Exception e) {
				erfs[index] = null;
				ExceptionUtils.throwAsRuntimeException(e);
			}
		}
	}
	
	private class CalcRunnable implements Runnable {
		
		private GMPE_MCErDeterministicCalc[] detCalcs;
		private GMPE_MCErProbabilisticCalc[] probCalcs;
		
		private Deque<CalcTask> tasks;
		
		public CalcRunnable(GMPE_MCErDeterministicCalc[] detCalcs, GMPE_MCErProbabilisticCalc[] probCalcs) {
			this.detCalcs = detCalcs;
			this.probCalcs = probCalcs;
		}
		
		private CalcTask popTask() {
			Preconditions.checkNotNull(tasks);
			synchronized (tasks) {
				try {
					return tasks.pop();
				} catch (NoSuchElementException e) {
					return null;
				}
			}
		}

		@Override
		public void run() {
			while (true) {
				CalcTask task = popTask();
				if (task == null)
					break;
				task.compute(detCalcs, probCalcs);
			}
		}
		
	}
	
	/**
	 * Comparator to ensure consistent ordering of site list
	 * @author kevin
	 *
	 */
	private static class SiteComparator implements Comparator<Site> {

		@Override
		public int compare(Site o1, Site o2) {
			int ret = o1.getLocation().compareTo(o2.getLocation());
			if (ret == 0 && o1.getName() != null && o2.getName() != null)
				ret = o1.getName().compareTo(o2.getName());
			return ret;
		}
		
	}
	
	private class CalcTask {
		private String cachePrefix;
		private int gmpeIndex;
		private int siteIndex;
		private Site site;
		
		public CalcTask(String cachePrefix, int siteIndex, int gmpeIndex, Site site) {
			this.cachePrefix = cachePrefix;
			this.siteIndex = siteIndex;
			this.gmpeIndex = gmpeIndex;
			this.site = site;
		}
		
		public void compute(GMPE_MCErDeterministicCalc[] detCalcs, GMPE_MCErProbabilisticCalc[] probCalcs) {
			String name = siteIndex+", prefix '"+cachePrefix+"'";
			
			// MCER
			CurveMetadata mcerMetadata = new CurveMetadata(site, siteIndex, null, cachePrefix+"_mcer");
			if (archiver.isCurveCalculated(mcerMetadata, periodsFunc)) {
				debug("Site "+name+" MCEr dalready done!");
			} else {
				debug("Calculating MCEr deterministic, site "+name);
				DiscretizedFunc det = AbstractMCErDeterministicCalc.toSpectrumFunc(detCalcs[gmpeIndex].calc(site, periods));
				debug("Calculating MCEr probabilistic, site "+name);
				DiscretizedFunc prob = probCalcs[gmpeIndex].calc(site, periods);
				DiscretizedFunc asceDeterm = null;
				try {
					asceDeterm = ASCEDetLowerLimitCalc.calc(
							periodsFunc, site.getParameter(Double.class, Vs30_Param.NAME).getValue(), site.getLocation());
				} catch (Exception e1) {
					System.out.println("WARNING: couldn't fetch Deterministic Lower Limit for site at "+site.getLocation());
				}
				DiscretizedFunc mcer = MCERDataProductsCalc.calcMCER(det, prob, asceDeterm);
				
				debug("Archiving MCEr site "+name);
				try {
					archiver.archiveCurve(det, new CurveMetadata(site, siteIndex, null, cachePrefix+"_det"));
					archiver.archiveCurve(prob, new CurveMetadata(site, siteIndex, null, cachePrefix+"_prob"));
					archiver.archiveCurve(mcer, mcerMetadata);
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
				debug("DONE MCEr site "+name);
			}
			
			// PGA
			try {
				if (pgaFiles.get(cachePrefix+"_pga").isCalculated(siteIndex)) {
					debug("Site "+name+" PGA G dalready done!");
				} else {
					debug("Calculating PGA deterministic, site "+name);
					double det = detCalcs[gmpeIndex].calcPGA_G(site).getVal();
					debug("Calculating PGA probabilistic, site "+name);
					double prob = probCalcs[gmpeIndex].calcPGA_G(site);
					double vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
					double detLower = ASCEDetLowerLimitCalc.calcPGA_G(vs30);
					double pga = MCErCalcUtils.calcMCER(det, prob, detLower);
					
					debug("Archiving PGA site "+name);
					Location loc = site.getLocation();
					pgaFiles.get(cachePrefix+"_pga_det").write(siteIndex, loc, det);
					pgaFiles.get(cachePrefix+"_pga_prob").write(siteIndex, loc, prob);
					pgaFiles.get(cachePrefix+"_pga").write(siteIndex, loc, pga);
					debug("DONE PGA site "+name);
				}
			} catch (IOException e) {
				ExceptionUtils.throwAsRuntimeException(e);
			}
		}
	}

	@Override
	protected int getNumTasks() {
		return calcTasks.size();
	}

	@Override
	protected void calculateBatch(int[] batch) throws Exception {
		Deque<CalcTask> deque = new ArrayDeque<MPJ_GMPE_MCErCacheGen.CalcTask>();
		for (int index : batch)
			deque.push(calcTasks.get(index));
		
		List<Thread> threads = Lists.newArrayList();
		
		for (int i=0; i<calcs.length; i++) {
			calcs[i].tasks = deque;
			threads.add(new Thread(calcs[i]));
		}
		
		// start the threads
		for (Thread t : threads) {
			t.start();
		}
		
		// join the threads
		for (Thread t : threads) {
			t.join();
		}
		
//		for (int index : batch)
//			calcTasks.get(index).compute();
	}

	@Override
	protected void doFinalAssembly() throws Exception {
		// do nothing
		archiver.close();
		for (BinaryGeoDatasetRandomAccessFile pgaFile : pgaFiles.values())
			pgaFile.close();
	}
	
	public static Options createOptions() {
		Options ops = MPJTaskCalculator.createOptions();
		
		Option sites = new Option("s", "sites", true, "Sites XML file");
		sites.setRequired(true);
		ops.addOption(sites);
		
		Option output = new Option("o", "output-dir", true, "Output directory");
		output.setRequired(true);
		ops.addOption(output);
		
		Option period = new Option("p", "period", true, "SA periods. Default: "+MCERDataProductsCalc.default_periods);
		period.setRequired(false);
		ops.addOption(period);
		
		Option erfFile = new Option("ef", "erf-file", true, "XML ERF description file for comparison");
		erfFile.setRequired(true);
		ops.addOption(erfFile);
		
		Option attenRelFiles = new Option("af", "atten-rel-file", true,
				"XML Attenuation Relationship description file(s) for " + 
				"comparison. Multiple files should be comma separated");
		attenRelFiles.setRequired(true);
		ops.addOption(attenRelFiles);
		
		Option component = new Option("cmp", "component", true, "Intensity measure component. "
				+ "Options: "+Joiner.on(",").join(CybershakeIM.getShortNames(CyberShakeComponent.class)));
		component.setRequired(true);
		ops.addOption(component);
		
		return ops;
	}
	
	public static void main(String[] args) {
		args = MPJTaskCalculator.initMPJ(args);
		
		try {
			Options options = createOptions();
			
			CommandLine cmd = parse(options, args, MPJ_GMPE_MCErCacheGen.class);
			
			MPJ_GMPE_MCErCacheGen driver = new MPJ_GMPE_MCErCacheGen(cmd);
			
			driver.run();
			
			finalizeMPJ();
			
			System.exit(0);
		} catch (Throwable t) {
			abortAndExit(t);
		}
	}

}
