package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.data.siteData.SiteDataValueList;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.hpc.JavaShellScriptWriter;
import org.opensha.commons.hpc.mpj.FastMPJShellScriptWriter;
import org.opensha.commons.hpc.mpj.MPJExpressShellScriptWriter;
import org.opensha.commons.hpc.pbs.BatchScriptWriter;
import org.opensha.commons.hpc.pbs.USC_HPCC_ScriptWriter;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.param.ParameterList;
import org.opensha.sha.calc.mcer.TLDataLoader;
import org.opensha.sha.cybershake.plot.HazardCurvePlotter;
import org.opensha.sha.imr.attenRelImpl.ngaw2.NGAW2_Wrappers.BSSA_2014_Wrapper;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;
import org.opensha.sha.util.SiteTranslator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class MPJ_GMPE_MCErCacheGenScriptWriter {
	
//	private static final String args_continue_newline = " \\\n\t";
	private static final String args_continue_newline = " "; // for MPJ Express

	public static void main(String[] args) throws IOException {
		int mins = 60*48;
		int nodes = 16;
		int memGigs = 50;
		String queue = "scec";
		int ppn = 20;
		
//		int mins = 60*24;
//		int nodes = 24;
//		int memGigs = 40;
//		String queue = null;
//		int ppn = 20;
		
		boolean fmpj = false;
		
		Map<String, Double> vs30Map = UGMS_WebToolCalc.vs30Map;
		
		double spacing = 0.02;
		
		GriddedRegion reg = new CaliforniaRegions.CYBERSHAKE_MAP_GRIDDED(spacing);
		
		int velModelID = 5;
		BSSA_2014_Wrapper bssa = new BSSA_2014_Wrapper();
		bssa.setParamDefaults();
		ParameterList siteParams = bssa.getSiteParams();
		
		List<Site> sites = getSites(reg, velModelID, siteParams);
		System.out.println(sites.size()+" sites before TL filter");
		TLDataLoader tlData = new TLDataLoader(
				CSVFile.readStream(TLDataLoader.class.getResourceAsStream(
						"/resources/data/site/USGS_TL/tl-nodes.csv"), true),
				CSVFile.readStream(TLDataLoader.class.getResourceAsStream(
						"/resources/data/site/USGS_TL/tl-attributes.csv"), true));
		for (int i=sites.size(); --i>=0;) {
			if (Double.isNaN(tlData.getValue(sites.get(i).getLocation())))
				sites.remove(i);
		}
		System.out.println(sites.size()+" sites after TL filter");
		
		for (String vsModel : vs30Map.keySet()) {
			String jobName = "ucerf3_downsampled_ngaw2_binary_"+(float)spacing;
			
//			String erfFileName = "MeanUCERF3_full.xml";
			String erfFileName = "MeanUCERF3_downsampled2.xml";
//			String[] gmpeNames = { "ask2014_no_trunc.xml", "bssa2014_no_trunc.xml", "cb2014_no_trunc.xml", "cy2014_no_trunc.xml" };
			String[] gmpeNames = { "nga2014_no_idriss_no_trunc.xml" };
			
			String periods = MCERDataProductsCalc.all_periods;
			
			// now set Vs30
			List<Site> mySites;
			Double vs30 = vs30Map.get(vsModel);
			if (vs30 == null) {
				mySites = sites;
				jobName += "_"+vsModel;
			} else {
				mySites = Lists.newArrayList();
				for (Site site : sites) {
					site = (Site) site.clone(); // also clones parameters
					site.setValue(Vs30_Param.NAME, vs30);
					mySites.add(site);
				}
				jobName += "_class"+vsModel;
			}
			
			String sitesFileName = "sites.xml";
			
			Component comp = Component.RotD100;
			
			jobName = new SimpleDateFormat("yyyy_MM_dd").format(new Date())+"-"+jobName;
			
			BatchScriptWriter pbsWrite;
			
//			int memGigs = 26;
//			int ppn = 16;
//			File remoteDir = new File("/work/00950/kevinm/cybershake/mcer_cache_gen");
//			pbsWrite = new StampedeScriptWriter();
//			File javaBin = StampedeScriptWriter.JAVA_BIN;
//			File fmpjHome = StampedeScriptWriter.FMPJ_HOME;
			
			File remoteDir = new File("/home/scec-02/kmilner/cybershake/mcer_cache_gen");
			pbsWrite = new USC_HPCC_ScriptWriter();
			File javaBin = USC_HPCC_ScriptWriter.JAVA_BIN;
			
			File remoteJobDir = new File(remoteDir, jobName);
			File localDir = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen");
			File localJobDir = new File(localDir, jobName);
			Preconditions.checkState(localJobDir.exists() || localJobDir.mkdir());
			
			File confDir = new File(remoteDir, "conf");
			
			List<File> classpath = new ArrayList<File>();
			classpath.add(new File(remoteDir, "commons-cli-1.2.jar"));
			classpath.add(new File(remoteDir, "OpenSHA_complete.jar"));
			
			JavaShellScriptWriter mpjWrite;
			if (fmpj) {
				mpjWrite = new FastMPJShellScriptWriter(
					javaBin, memGigs*1024, classpath, USC_HPCC_ScriptWriter.FMPJ_HOME);
				((FastMPJShellScriptWriter)mpjWrite).setUseLaunchWrapper(true);
			} else {
				mpjWrite = new MPJExpressShellScriptWriter(
						javaBin, memGigs*1024, classpath, USC_HPCC_ScriptWriter.MPJ_HOME);
			}
			
			CyberShakeMCErMapGenerator.writeSitesFile(new File(localJobDir, sitesFileName), mySites);
			
			String argz = args_continue_newline+"--output-dir "+remoteJobDir.getAbsolutePath();
			argz += args_continue_newline+"--erf-file "+confDir.getAbsolutePath()+"/"+erfFileName;
			argz += args_continue_newline+"--atten-rel-file ";
			for (int i=0; i<gmpeNames.length; i++) {
				if (i>0)
					argz += ",";
				argz += confDir.getAbsolutePath()+"/"+gmpeNames[i];
			}
			argz += args_continue_newline+"--sites "+remoteJobDir.getAbsolutePath()+"/"+sitesFileName;
			argz += args_continue_newline+"--component "+comp.name();
			argz += args_continue_newline+"--period "+periods;
			
			File pbsFile = new File(localJobDir, jobName+".pbs");
			
			List<String> script = mpjWrite.buildScript(MPJ_GMPE_MCErCacheGen.class.getName(), argz);
			
			script = pbsWrite.buildScript(script, mins, nodes, ppn, queue);
			pbsWrite.writeScript(pbsFile, script);
		}
	}
	
	private static List<Site> getSites(GriddedRegion reg, int velModelID, ParameterList siteParams) throws IOException {
		// ParameterList siteParams = MCERDataProductsCalc.getSiteParams(gmpes);
		OrderedSiteDataProviderList provs = HazardCurvePlotter.createProviders(velModelID);
		
		List<Site> sites = Lists.newArrayList();
		
		SiteTranslator siteTrans = new SiteTranslator();
		
		LocationList nodes = reg.getNodeList();
		
		ArrayList<SiteDataValueList<?>> allDatas = provs.getAllAvailableData(nodes);
		
		for (int i=0; i<nodes.size(); i++) {
			Site site = new Site(nodes.get(i));
			
			ArrayList<SiteDataValue<?>> datas = Lists.newArrayList();
			for (SiteDataValueList<?> provData : allDatas)
				datas.add(provData.getValue(i));
			for (Parameter<?> param : siteParams) {
				param = (Parameter<?>)param.clone();
				siteTrans.setParameterValue(param, datas);
				site.addParameter(param);
			}
			sites.add(site);
		}
		
//		for (Location loc : reg.getNodeList()) {
//			Site site = new Site(loc);
//			ArrayList<SiteDataValue<?>> datas = provs.getBestAvailableData(site.getLocation());
//			for (Parameter<?> param : siteParams) {
//				param = (Parameter<?>)param.clone();
//				siteTrans.setParameterValue(param, datas);
//				site.addParameter(param);
//			}
//			sites.add(site);
//		}
		
		return sites;
	}

}
