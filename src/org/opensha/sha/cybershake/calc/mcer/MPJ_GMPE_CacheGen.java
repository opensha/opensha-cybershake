package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import mpi.MPI;
import edu.usc.kmilner.mpj.taskDispatch.MPJTaskCalculator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.param.ParameterList;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.XMLUtils;
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class MPJ_GMPE_CacheGen extends MPJTaskCalculator {
	
	private List<Double> periods;
	private List<CalcTask> calcTasks;
	
	CachedMCErDeterministicCalc[] detCalcs;
	CachedCurveBasedMCErProbabilisticCalc[] probCalcs;
	
	public MPJ_GMPE_CacheGen(CommandLine cmd) throws InvocationTargetException, MalformedURLException, DocumentException {
		super(cmd);
		
		ERF erf = ERFSaver.LOAD_ERF_FROM_FILE(cmd.getOptionValue("erf-file"));
		
		RuptureProbabilityModifier detProbMod = CyberShakeMCErDeterministicCalc.getProbMod(erf);
		ERF detERF = MCERDataProductsCalc.getGMPEDetERF(erf, detProbMod);
		List<AttenuationRelationship> attenRels = Lists.newArrayList();
		
		for (String attenRelFile : DataUtils.commaSplit(cmd.getOptionValue("atten-rel-file"))) {
			AttenuationRelationship attenRel = AttenRelSaver.LOAD_ATTEN_REL_FROM_FILE(attenRelFile);
			attenRels.add(attenRel);
		}
		
		erf.updateForecast();
		
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
		if (rank > 0) {
			// store results in sub directory
			outputDir = new File(outputDir, "running_results");
			if (rank == 1)
				Preconditions.checkArgument((outputDir.exists() && outputDir.isDirectory()) || outputDir.mkdir(),
						"Output dir does not exist and could not be created");
		}
		
		CyberShakeComponent comp = CybershakeIM.fromShortName(cmd.getOptionValue("component"), CyberShakeComponent.class);
		
		File sitesFile = new File(cmd.getOptionValue("sites"));
		Preconditions.checkState(sitesFile.exists(), "Sites file doesn't exist: %s", sitesFile);
		
		// load sites
		Document siteDoc = XMLUtils.loadDocument(sitesFile);
		List<Site> sites = Lists.newArrayList();
		
		ParameterList siteParams = MCERDataProductsCalc.getSiteParams(attenRels);
		
		for (Element elem : XMLUtils.getSubElementsList(siteDoc.getRootElement(), "Site")) {
			// will clone site params list
			sites.add(Site.fromXMLMetadata(elem, siteParams));
		}
		
		Collections.sort(sites, new SiteComparator());
		
		// create calcs
		detCalcs = new CachedMCErDeterministicCalc[attenRels.size()];
		probCalcs = new CachedCurveBasedMCErProbabilisticCalc[attenRels.size()];
		
		Component gmpeComp = MCErCalcUtils.getSupportedTranslationComponent(
				attenRels.get(0), comp.getGMPESupportedComponents());
		
		DiscretizedFunc xVals = new IMT_Info().getDefaultHazardCurve(SA_Param.NAME);
		
		for (int i=0; i<attenRels.size(); i++) {
			String cachePrefix = CyberShakeMCErMapGenerator.getCachePrefix(
					-1, erf, gmpeComp, Lists.newArrayList(attenRels.get(i)));
			GMPE_MCErDeterministicCalc detCalc =
					new GMPE_MCErDeterministicCalc(detERF, attenRels.get(i), gmpeComp);
			GMPE_MCErProbabilisticCalc probCalc =
					new GMPE_MCErProbabilisticCalc(erf, attenRels.get(i), gmpeComp, xVals);
			
			if (rank > 0)
				cachePrefix = "results_process_"+rank+"_"+cachePrefix;
			
			detCalcs[i] = new CachedMCErDeterministicCalc(detCalc,
					new File(outputDir, cachePrefix+"_deterministic.xml"));
			probCalcs[i] = new CachedCurveBasedMCErProbabilisticCalc(probCalc,
					new File(outputDir, cachePrefix+"_probabilistic_curve.xml"));
		}
		
		calcTasks = Lists.newArrayList();
		for (Site site : sites)
			for (int i=0; i<attenRels.size(); i++)
				calcTasks.add(new CalcTask(detCalcs[i], probCalcs[i], site));
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
		private CachedMCErDeterministicCalc detCalc;
		private CachedCurveBasedMCErProbabilisticCalc probCalc;
		private Site site;
		
		public CalcTask(CachedMCErDeterministicCalc detCalc,
				CachedCurveBasedMCErProbabilisticCalc probCalc, Site site) {
			this.detCalc = detCalc;
			this.probCalc = probCalc;
			this.site = site;
		}
		
		public void calc() {
			debug("Calculating deterministic, "+site.getName());
			detCalc.calc(site, periods);
			try {
				detCalc.flushCache();
			} catch (IOException e) {
				e.printStackTrace();
			}
			debug("Calculating probabilistic, "+site.getName());
			probCalc.calc(site, periods);
			try {
				probCalc.flushCache();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	protected int getNumTasks() {
		return calcTasks.size();
	}

	@Override
	protected void calculateBatch(int[] batch) throws Exception {
		for (int index : batch)
			calcTasks.get(index).calc();
	}

	@Override
	protected void doFinalAssembly() throws Exception {
		for (int i=0; i<detCalcs.length; i++) {
			if (rank == 0)
				debug("Gathering deterministic, calc "+i);
			CachedMCErDeterministicCalc[] detSendBuf = {detCalcs[i]};
			CachedMCErDeterministicCalc[] detRecvBuf = null;
			if (rank == 0)
				detRecvBuf = new CachedMCErDeterministicCalc[size];
			MPI.COMM_WORLD.Gather(detSendBuf, 0, 1, MPI.OBJECT, detRecvBuf, 0, 1, MPI.OBJECT, 0);
			
			if (rank == 0) {
				for (int n=1; n<size; n++)
					detCalcs[i].addToCache(detRecvBuf[n]);
				detCalcs[i].flushCache();
				detRecvBuf = null;
			}
			
			if (rank == 0)
				debug("Gathering probabilistic, calc "+i);
			CachedCurveBasedMCErProbabilisticCalc[] probSendBuf = {probCalcs[i]};
			CachedCurveBasedMCErProbabilisticCalc[] probRecvBuf = null;
			if (rank == 0)
				probRecvBuf = new CachedCurveBasedMCErProbabilisticCalc[size];
			MPI.COMM_WORLD.Gather(probSendBuf, 0, 1, MPI.OBJECT, probRecvBuf, 0, 1, MPI.OBJECT, 0);
			
			if (rank == 0) {
				for (int n=1; n<size; n++)
					probCalcs[i].addToCache(probRecvBuf[n]);
				probCalcs[i].flushCache();
				probRecvBuf = null;
			}
		}
		if (rank == 0)
			debug("Done gathering");
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
			
			CommandLine cmd = parse(options, args, MPJ_GMPE_CacheGen.class);
			
			MPJ_GMPE_CacheGen driver = new MPJ_GMPE_CacheGen(cmd);
			
			driver.run();
			
			finalizeMPJ();
			
			System.exit(0);
		} catch (Throwable t) {
			abortAndExit(t);
		}
	}

}
