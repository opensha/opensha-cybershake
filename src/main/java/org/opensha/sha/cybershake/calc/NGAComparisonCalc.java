package org.opensha.sha.cybershake.calc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.data.siteData.SiteDataValueList;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.util.ClassUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.attenRelImpl.AS_2008_AttenRel;
import org.opensha.sha.imr.attenRelImpl.BA_2008_AttenRel;
import org.opensha.sha.imr.attenRelImpl.CB_2008_AttenRel;
import org.opensha.sha.imr.attenRelImpl.CY_2008_AttenRel;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.util.SiteTranslator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class NGAComparisonCalc {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) {
		// first look for a --site argument
		ArrayList<String> newArgs = new ArrayList<String>();
		String siteName = null;
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("--site")) {
				Preconditions.checkArgument(siteName == null,
						"multiple --site arguments are not allowed!");
				Preconditions.checkArgument(i+1<args.length,
						"--site specified but no argument following!");
				// increment i and set the siteName
				siteName = args[++i];
			} else {
				newArgs.add(args[i]);
			}
		}
		
		args = newArgs.toArray(new String[newArgs.size()]);
		
		// prepare user inputs (either from command line or a file
		ArrayList<String[]> inputs = new ArrayList<String[]>();
		if (args.length == 1) {
			// file based input
			String fileName = args[0];
			try {
				for (String line : FileUtils.loadFile(fileName)) {
					String[] input = new String[3];
					
					StringTokenizer tok = new StringTokenizer(line);
					Preconditions.checkState(tok.countTokens() == 3, "Incorrectly formatted line: "+line);
					
					input[0] = tok.nextToken();
					input[1] = tok.nextToken();
					input[2] = tok.nextToken();
					
					inputs.add(input);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("Error loading file: "+fileName);
				System.exit(1);
			}
		} else  if (args.length > 0 && args.length % 3 == 0) {
			// CLI input
			String[] input = null;
			for (int i=0; i<args.length; i++) {
				int mod = i % 3;
				
				if (mod == 0) {
					if (input != null)
						inputs.add(input);
					input = new String[3];
				}
				
				input[mod] = args[i];
			}
			if (input != null)
				inputs.add(input);
		} else {
			String cName = ClassUtils.getClassNameWithoutPackage(NGAComparisonCalc.class);
			String commonOps = cName+" [--site <site short name>]";
			System.err.println("USAGE: ");
			System.err.println("\t"+commonOps+" <input file>");
			System.err.println("\t--- OR ---");
			System.err.println("\t"+commonOps+" <source id> <rup id> <SA period>");
			System.exit(2);
		}
		
		// create the CyberShake modified UCERF2 ERF (ERF 35 in the database)
		System.out.println("Instantiating UCERF2");
		int erfID = 35;
		ERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		erf.updateForecast();
		
		// create the attenuation relationship
		ArrayList<ScalarIMR> imrs = new ArrayList<ScalarIMR>();
		imrs.add(new CB_2008_AttenRel(null));
		imrs.add(new BA_2008_AttenRel(null));
		imrs.add(new CY_2008_AttenRel(null));
		imrs.add(new AS_2008_AttenRel(null));
		for (ScalarIMR imr : imrs) {
			imr.setParamDefaults();
			
			// set the intensity measure...in this case, spectral acceleration at the given period
			imr.setIntensityMeasure(SA_Param.NAME);
		}
		
		// this is needed to get the CyberShake site list from the database
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		CybershakeSiteInfo2DB sites2db = new CybershakeSiteInfo2DB(db);
		PeakAmplitudesFromDB amps2db = new PeakAmplitudesFromDB(db);
		
		List<CybershakeSite> allCSSites;
		List<CybershakeRun> ampRuns;
		
		if (siteName == null) {
			// we're doing it for all sites
			allCSSites = sites2db.getAllSitesFromDB();
			ampRuns = amps2db.getPeakAmpRuns();
		} else {
			// just for the given site
			CybershakeSite site = sites2db.getSiteFromDB(siteName);
			allCSSites = Lists.newArrayList(site);
			ampRuns = amps2db.getPeakAmpRuns(site.id, erfID, -1, -1, -1);
		}
		
		ArrayList<CybershakeSite> csSites = new ArrayList<CybershakeSite>();
		ArrayList<ArrayList<Integer>> csSiteRunIDs = new ArrayList<ArrayList<Integer>>();
		
		for (CybershakeSite site : allCSSites) {
			ArrayList<Integer> runIDs = new ArrayList<Integer>();
			for (CybershakeRun run : ampRuns) {
				if (run.getSiteID() == site.id)
					runIDs.add(run.getRunID());
			}
			if (!runIDs.isEmpty()) {
				csSites.add(site);
				csSiteRunIDs.add(runIDs);
			}
		}
		
		ArrayList<Site> sites = new ArrayList<Site>();
		
		db.destroy();
		
		for (CybershakeSite csSite : csSites) {
			Site site = new Site(csSite.createLocation());
			site.setName(csSite.short_name);
			
			// add site parameters from the IMR to the site. we will set them in the next step
			for (ScalarIMR imr : imrs) {
				Iterator<Parameter<?>> it = imr.getSiteParamsIterator();
				while (it.hasNext()) {
					Parameter<?> param = it.next();
					if (!site.containsParameter(param))
						site.addParameter((Parameter)param.clone());
				}
			}
			sites.add(site);
		}
		
		// get site data and set the parameters
		System.out.println("Fetching site data");
		OrderedSiteDataProviderList providers = OrderedSiteDataProviderList.createSiteDataProviderDefaults();
		ArrayList<SiteDataValueList<?>> siteData = null;
		try {
			siteData = providers.getAllAvailableData(sites);
		} catch (IOException e) {
			System.out.println("Error fetching site data (server down?)");
			e.printStackTrace();
			System.exit(1);
		}
		
		// make sure that we have a value for each site
		for (SiteDataValueList<?> datas : siteData)
			Preconditions.checkState(datas.size() == sites.size());
		
		// actually set each site value
		System.out.println("setting site param values from data");
		SiteTranslator trans = new SiteTranslator();
		for (int i=0; i<sites.size(); i++) {
			Site site = sites.get(i);
			
			ArrayList<SiteDataValue<?>> data = new ArrayList<SiteDataValue<?>>();
			for (SiteDataValueList<?> datas : siteData)
				data.add(datas.getValue(i));
			
			Iterator<Parameter<?>> it = site.getParametersIterator();
			while (it.hasNext()) {
				Parameter<?> param = it.next();
				trans.setParameterValue(param, data);
			}
		}
		
		ArrayList<String> siteSpecificHeader = Lists.newArrayList("ID", "Short Name", "Run IDs");
		
		Preconditions.checkState(!csSites.isEmpty(), "no sites found!");
		
		for (Iterator<Parameter<?>> it = sites.get(0).getParametersIterator(); it.hasNext(); )
			siteSpecificHeader.add(it.next().getName());
		
		ArrayList<String> rupSpecificHeader = Lists.newArrayList("Source ID", "Rup ID",
				"DistanceJB", "DistanceRup", "DistanceSeis", "DistanceX");
		for (ScalarIMR imr : imrs) {
			rupSpecificHeader.add("mean ("+imr.getShortName()+")");
			rupSpecificHeader.add("std. dev. ("+imr.getShortName()+")");
		}
		
		boolean siteSpecific = siteName != null;
		
		CSVFile<String> csv = null;
		File outputFile = null;
		if (siteSpecific) {
			csv = new CSVFile<String>(false);
			outputFile = new File("ERF"+erfID+"_"+siteName+".csv");
			
			csv.addLine(siteSpecificHeader);
			csv.addLine(getSiteSpecificVals(csSites.get(0), csSiteRunIDs.get(0), sites.get(0)));
			csv.addLine(rupSpecificHeader);
		}
		
		for (String[] input : inputs) {
			int sourceID = Integer.parseInt(input[0]);
			int rupID = Integer.parseInt(input[1]);
			double period = Double.parseDouble(input[2]);
			
			// set the period
			for (ScalarIMR imr : imrs)
				SA_Param.setPeriodInSA_Param(imr.getIntensityMeasure(), period);
			
			if (!siteSpecific) {
				outputFile = new File("ERF"+erfID+"_src"+sourceID+"_rup"+rupID+"_SA"+(float)period+".csv");
				csv = new CSVFile<String>(true);
				ArrayList<String> header = new ArrayList<String>();
				
				header.addAll(siteSpecificHeader);
				header.addAll(rupSpecificHeader);
				
				csv.addLine(header);
			}
			
			// get the rupture for the specified source/rup ID
			ProbEqkRupture rup = erf.getRupture(sourceID, rupID);
			
			// get mean/std dev of ground motion
			System.out.println("Calculating Ground Motions");
			for (int i=0; i<sites.size(); i++) {
				Site site = sites.get(i);
				ArrayList<Integer> runIDs = csSiteRunIDs.get(i);
				CybershakeSite csSite = csSites.get(i);
				
				ArrayList<String> line = new ArrayList<String>();
				
				if (!siteSpecific)
					line.addAll(getSiteSpecificVals(csSite, runIDs, site));
				
				line.add(sourceID+"");
				line.add(rupID+"");
				line.addAll(getNGAData(site, rup, imrs));
				
				csv.addLine(line);
				
//				System.out.println("site: "+site.getName()+"\tmean: "+mean+"\t std dev: "+stdDev);
			}
			
			if (!siteSpecific) {
				System.out.println("Writing: "+outputFile.getName()+"...");
				try {
					csv.writeToFile(outputFile);
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("Error writing output file!");
					System.exit(1);
				}
				System.out.print("Done!");
			}
		}
		
		if (siteSpecific) {
			System.out.println("Writing: "+outputFile.getName()+"...");
			try {
				csv.writeToFile(outputFile);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error writing output file!");
				System.exit(1);
			}
			System.out.print("Done!");
		}
		
		System.exit(0);
	}
	
	private static List<String> getNGAData(Site site, ProbEqkRupture rup, List<ScalarIMR> imrs) {
		ArrayList<String> line = new ArrayList<String>();
		
		line.add(rup.getRuptureSurface().getDistanceJB(site.getLocation())+"");
		line.add(rup.getRuptureSurface().getDistanceRup(site.getLocation())+"");
		line.add(rup.getRuptureSurface().getDistanceSeis(site.getLocation())+"");
		line.add(rup.getRuptureSurface().getDistanceX(site.getLocation())+"");
		
		for (ScalarIMR imr : imrs) {
			imr.setEqkRupture(rup);
			imr.setSite(site);
			
			// natural log of SA at given period, in G's
			double mean = imr.getMean();
			double stdDev = imr.getStdDev();
			
			line.add(mean+"");
			line.add(stdDev+"");
		}
		
		return line;
	}
	
	private static String getRunIDString(List<Integer> runIDs) {
		String ids = null;
		for (int runID : runIDs) {
			if (ids == null)
				ids = "";
			else
				ids += ",";
			ids += runID;
		}
		return ids;
	}
	
	private static List<String> getSiteSpecificVals(CybershakeSite csSite, List<Integer> runIDs, Site site) {
		ArrayList<String> line = new ArrayList<String>();
		line.add(csSite.id+"");
		line.add(csSite.short_name);
		
		String ids = getRunIDString(runIDs);
		
		line.add(ids);
		
		for (Iterator<Parameter<?>> it = site.getParametersIterator(); it.hasNext(); )
			line.add(it.next().getValue()+"");
		return line;
	}

}
