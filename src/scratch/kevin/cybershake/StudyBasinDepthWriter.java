package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.imr.param.SiteParams.DepthTo1pt0kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.DepthTo2pt5kmPerSecParam;

public class StudyBasinDepthWriter {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_17_3_3D;
		
		File outputFile = new File("/tmp/"+study.getDirName()+"_basin_depth.csv");
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		CyberShakeSiteBuilder builder = new CyberShakeSiteBuilder(Vs30_Source.Wills2015, study.getVelocityModelID());
		SiteInfo2DB sites2db = new SiteInfo2DB(study.getDB());
		List<CybershakeSite> allSites = sites2db.getAllSitesFromDB();
		List<CybershakeSite> csSites = new ArrayList<>();
		for (CybershakeRun run : runs) {
			CybershakeSite site = null;
			for (CybershakeSite s2 : allSites)
				if (run.getSiteID() == s2.id)
					site = s2;
			csSites.add(site);
		}
		
		List<Site> sites = builder.buildSites(runs, csSites);
		CSVFile<String> csv = new CSVFile<>(true);
		csv.addLine("Site Short Name", "Latitude", "Longitude", "Z1.0 [m]", "Z2.5 [km]");
		for (Site site : sites) {
			double z10 = site.getParameter(Double.class, DepthTo1pt0kmPerSecParam.NAME).getValue();
			double z25 = site.getParameter(Double.class, DepthTo2pt5kmPerSecParam.NAME).getValue();
			Location loc = site.getLocation();
			csv.addLine(site.getName(), (float)loc.getLatitude()+"", (float)loc.getLongitude()+"", (float)z10+"", (float)z25+"");
		}
		System.out.println("Writing "+outputFile.getAbsolutePath());
		csv.writeToFile(outputFile);
		
		study.getDB().destroy();
	}

}
