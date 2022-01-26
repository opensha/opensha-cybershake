package org.opensha.sha.cybershake.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.sha.earthquake.AbstractERF;

public class CybershakeSiteManager {
	
	public static boolean insertCybershakeSite(DBAccess db, CybershakeSite site, AbstractERF erf, int erfID, double cutoffDistance, int typeID) {
		SiteInfo2DB site2db = new SiteInfo2DB(db);
		CybershakeSiteInfo2DB csSite2db = new CybershakeSiteInfo2DB(db);
		System.out.println("Inserting site: " + site.getFormattedName());
		
		// CyberShake_Sites table
		System.out.println("Inserting site record");
		csSite2db.putCybershakeLocationInDB(site);
		int siteID = csSite2db.getCybershakeSiteId(site.short_name);
		if (siteID < 0) {
			System.out.println("Error inserting site record!");
			return false;
		}
		System.out.println("Site inserted with ID=" + siteID);
		
		System.out.println("Setting site type to " + typeID);
		site2db.setSiteType(siteID, typeID);
		
		CybershakeSiteInfo2DB.CUT_OFF_DISTANCE = cutoffDistance;
		
		// CyberShake_Site_Regional_Bounds table
		System.out.println("Inserting regional bounds");
		csSite2db.putCyberShakeLocationRegionalBounds(erf, erfID, siteID, site.lat, site.lon, false);
		
		System.out.println("Inserting Source Rupture info");
		csSite2db.putCyberShakeLocationSrcRupInfo(erf, erfID, siteID, site.lat, site.lon, false, null);
		
		System.out.println("DONE inserting site!");
		
		return true;
	}
	
	public static boolean deleteCybershakeSite(DBAccess db, CybershakeSite site) {
		HazardCurve2DB curve2db = new HazardCurve2DB(db);
		PeakAmplitudesFromDB amps2db = new PeakAmplitudesFromDB(db);
		SiteInfo2DB site2db = new SiteInfo2DB(db);
		
		System.out.println("Deleting site: " + site.getFormattedName());
		
		// first delete hazard curves:
		System.out.println("Deleting all hazard curves...");
		ArrayList<CybershakeHazardCurveRecord> curves = curve2db.getHazardCurveRecordsForSite(site.id);
		if (curves != null) {
			for (CybershakeHazardCurveRecord curve: curves) {
				System.out.println("Deleting curve " + curve.getCurveID());
				curve2db.deleteHazardCurve(curve.getCurveID());
			}
		}
		
		System.out.println("Deleting all peak amplitudes...");
		int rows = amps2db.deleteAllAmpsForSite(site.id);
		System.out.println("Deleted " + rows + " amp rows");
		
		System.out.println("Deleting all site ruptures");
		rows = site2db.deleteRupturesForSite(site.id);
		System.out.println("Deleted " + rows + " site rupture rows");
		
		System.out.println("Deleting all site regional bounds");
		rows = site2db.deleteRegionsForSite(site.id);
		System.out.println("Deleted " + rows + " site regional bounds");
		
		System.out.println("Deleting site record");
		rows = site2db.deleteSiteRecord(site.id);
		System.out.println("Deleted " + rows + " site records");
		
		return true;
	}
	
	public static void main(String[] args) throws IOException {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(true, false);
		CSVFile<String> csv = CSVFile.readFile(new File("/tmp/sites.csv"), true, 5);
		
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		int erfID = 35;
		double cutoffDistance = 200;
		
		for (int i=1; i<csv.getNumRows(); i++) {
			List<String> row = csv.getLine(i);
			String shortName = row.get(0);
			String name = row.get(1);
			double lat = Double.parseDouble(row.get(2));
			double lon = Double.parseDouble(row.get(3));
			int typeID = Integer.parseInt(row.get(4));
			
			CybershakeSite site = new CybershakeSite(lat, lon, name, shortName);
			System.out.println("Inserting: "+site);
			
			insertCybershakeSite(db, site, erf, erfID, cutoffDistance, typeID);
		}
//		int typeID = 1;
		
		db.destroy();
	}
}
