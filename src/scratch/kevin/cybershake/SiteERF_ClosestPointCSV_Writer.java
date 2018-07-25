package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkSource;

public class SiteERF_ClosestPointCSV_Writer {

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		List<CybershakeSite> sites = sites2db.getAllSitesFromDB();
		
		File outputDir = new File("/home/kevin/CyberShake/site_erf_closest_points/ucerf2");
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		double cutoff = 200d;
		
		Map<CybershakeSite, Future<CSVFile<String>>> futuresMap = new HashMap<>();
		
		ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		
		for (CybershakeSite site : sites)
			futuresMap.put(site, exec.submit(new CalcTask(erf, site, cutoff)));
		
		for (CybershakeSite site : sites) {
			CSVFile<String> csv = futuresMap.get(site).get();
			System.out.println("Writing for "+site.short_name);
			csv.writeToFile(new File(outputDir, site.short_name+".csv"));
		}
		
		exec.shutdown();
		
		db.destroy();
	}
	
	private static class CalcTask implements Callable<CSVFile<String>> {
		
		private AbstractERF erf;
		private CybershakeSite site;
		private double cutoff;

		public CalcTask(AbstractERF erf, CybershakeSite site, double cutoff) {
			super();
			this.erf = erf;
			this.site = site;
			this.cutoff = cutoff;
		}

		@Override
		public CSVFile<String> call() throws Exception {
			CSVFile<String> csv = new CSVFile<>(true);
			csv.addLine("Source ID", "Rupture ID", "Min Distance (km)", "Closest Point Latitude",
					"Closest Point Longitude", "Closest Point Depth (km)");
			Location siteLoc = site.createLocation();
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
				ProbEqkSource source = erf.getSource(sourceID);
				if (source.getMinDistance(new Site(siteLoc)) > cutoff)
					continue;
				for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
					double minDist = Double.POSITIVE_INFINITY;
					Location closest = null;
					for (Location loc : source.getRupture(rupID).getRuptureSurface().getEvenlyDiscritizedListOfLocsOnSurface()) {
						double dist = LocationUtils.horzDistanceFast(siteLoc, loc);
						if (dist < minDist) {
							minDist = dist;
							closest = loc;
						}
					}
					csv.addLine(sourceID+"", rupID+"", (float)minDist+"", (float)closest.getLatitude()+"",
							(float)closest.getLongitude()+"", (float)closest.getDepth()+"");
				}
			}
			return csv;
		}
		
	}

}
