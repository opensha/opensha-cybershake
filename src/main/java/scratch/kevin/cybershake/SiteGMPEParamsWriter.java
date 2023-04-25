package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.faultSurface.utils.GriddedSurfaceUtils;
import org.opensha.sha.imr.param.SiteParams.DepthTo1pt0kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.DepthTo2pt5kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SiteGMPEParamsWriter {

	public static void main(String[] args) throws IOException, SQLException {
		File baseDir = new File("/home/kevin/CyberShake/site_flat_files");

		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
		
		File outputDir = new File(baseDir, study.getDirName());
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		List<CybershakeRun> runs = null;
		
//		String[] siteNames = {
//				"LADT", "PAS", "CCP", "STNI", "STG", "SBSM", "WNGC"
//		};
//		runs = study.runFetcher().forSiteNames(siteNames).fetch();
		
		runs = study.runFetcher().fetch();
		
		WillsMap2015 wills = new WillsMap2015();
		
		AbstractERF erf = study.buildNewERF();
		
		List<CyberShakeSiteRun> sites = CyberShakeSiteBuilder.buildSites(
				study, Vs30_Source.Simulation, runs);
		
		List<String> header = Lists.newArrayList("Site Name", "Site Lat", "Site Lon", "Wills (2015) Vs30 (m/s)",
				"CyberShake Vs30 (m/s)", "Site Z1.0 (km)", "Site Z2.5 (km)", "Source ID", "Rupture ID", "Rate (1/yr)",
				"Magnitude", "Dip", "Rake", "ZTOR (km)", "DDW (km)", "Rrup (km)", "Rjb (km)", "Rx (km)", "Ry0 (km)");
//				"Closest Point Lat", "Closest Point Lon", "Closest Point Depth (km)");
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(study.getDB(), null, erf);
		CybershakeIM refIM = CybershakeIM.getSA(CyberShakeComponent.RotD50, 3);
		
		for (Site site : sites) {
			CSVFile<String> csv = new CSVFile<>(true);
			csv.addLine(header);
			
			Location siteLoc = site.getLocation();
			String siteName = site.getName();
			System.out.println("Doing site: "+siteName);
			double csVs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
			double willsVs30 = wills.getValue(siteLoc);
			double z10 = site.getParameter(Double.class, DepthTo1pt0kmPerSecParam.NAME).getValue()/1000d;;
			double z25 = site.getParameter(Double.class, DepthTo2pt5kmPerSecParam.NAME).getValue();
			
			int runID = ((CyberShakeSiteRun)site).getCS_Run().getRunID();
			double[][][] amps = amps2db.getAllIM_Values(runID, refIM);
			
			for (int sourceID=0; sourceID<amps.length; sourceID++) {
				if (amps[sourceID] == null)
					continue;
				ProbEqkSource source = erf.getSource(sourceID);
				Preconditions.checkState(amps[sourceID].length == source.getNumRuptures());
				for (int rupID=0; rupID<amps[sourceID].length; rupID++) {
					if (amps[sourceID][rupID] == null)
						continue;
					ProbEqkRupture rup = source.getRupture(rupID);
					RuptureSurface surf = rup.getRuptureSurface();
					
					double rate = rup.getMeanAnnualRate(1d);
					double rJB = surf.getDistanceJB(siteLoc);
					double rRup = surf.getDistanceRup(siteLoc);
					double rX = surf.getDistanceX(siteLoc);
					double rY0 = GriddedSurfaceUtils.getDistanceY0(surf.getEvenlyDiscritizedUpperEdge(), siteLoc);
					double ddw = surf.getAveWidth();
					double ztor = surf.getAveRupTopDepth();
					
//					Location closestPt = null;
//					double minDist = Double.POSITIVE_INFINITY;
//					for (Location pt : surf.getEvenlyDiscritizedListOfLocsOnSurface()) {
//						double dist = LocationUtils.linearDistanceFast(siteLoc, pt);
//						if (dist < minDist) {
//							closestPt = pt;
//							minDist = dist;
//						}
//					}
//					Preconditions.checkState((float)minDist == (float)rRup,
//							"Dist mismatch. %s != %s", (float)minDist, (float)rRup);
					
					List<String> line = new ArrayList<>();
					line.add(siteName);
					line.add((float)siteLoc.getLatitude()+"");
					line.add((float)siteLoc.getLongitude()+"");
					line.add((float)willsVs30+"");
					line.add((float)csVs30+"");
					line.add((float)z10+"");
					line.add((float)z25+"");
					line.add(sourceID+"");
					line.add(rupID+"");
					line.add((float)rate+"");
					line.add((float)rup.getMag()+"");
					line.add((float)surf.getAveDip()+"");
					line.add((float)rup.getAveRake()+"");
					line.add((float)ztor+"");
					line.add((float)ddw+"");
					line.add((float)rRup+"");
					line.add((float)rJB+"");
					line.add((float)rX+"");
					line.add((float)rY0+"");
//					line.add((float)closestPt.getLatitude()+"");
//					line.add((float)closestPt.getLongitude()+"");
//					line.add((float)closestPt.getDepth()+"");
					csv.addLine(line);
				}
			}
			File outputFile = new File(outputDir, siteName+"_gmpe_params.csv");
			System.out.println("Writing CSV with "+csv.getNumRows()+" rows to: "+outputFile.getAbsolutePath());
			csv.writeToFile(outputFile);
		}
		study.getDB().destroy();
		System.out.println("DONE");
	}

}
