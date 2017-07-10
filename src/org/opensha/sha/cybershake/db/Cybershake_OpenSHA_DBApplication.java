/*******************************************************************************
 * Copyright 2009 OpenSHA.org in partnership with
 * the Southern California Earthquake Center (SCEC, http://www.scec.org)
 * at the University of Southern California and the UnitedStates Geological
 * Survey (USGS; http://www.usgs.gov)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.opensha.sha.cybershake.db;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.gui.UserAuthDialog;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.earthquake.ERF;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Main application to put the Cybershake locations and Earthquake Rupture Forecast information
 * in the database.
 * @author nitingupta
 *
 */
public class Cybershake_OpenSHA_DBApplication {

	public static String PRODUCTION_HOST_NAME = "moment.usc.edu";
	public static String ARCHIVE_HOST_NAME = "focal.usc.edu";
	public static String HOST_NAME = System.getProperty("cybershake.db.host", PRODUCTION_HOST_NAME);
//	public static String HOST_NAME = System.getProperty("cybershake.db.host", "focal.usc.edu");
	public static String DATABASE_NAME = System.getProperty("cybershake.db.name", "CyberShake");
//	private static DBAccess db;
	private static Map<String, DBAccess> dbs = Maps.newHashMap();
	
	public static DBAccess getDB() {
		return getDB(HOST_NAME);
	}
	
	public static DBAccess getDB(String hostName) {
		Preconditions.checkNotNull(hostName);
		synchronized (Cybershake_OpenSHA_DBApplication.class) {
			if (!hostName.equals(HOST_NAME))
				HOST_NAME = hostName;
			if (!dbs.containsKey(hostName))
				dbs.put(hostName, new DBAccess(hostName, DATABASE_NAME));
			return dbs.get(hostName);
		}
	}
	
	public static void destroyAllDBs() {
		synchronized (Cybershake_OpenSHA_DBApplication.class) {
			List<String> hosts = Lists.newArrayList(dbs.keySet());
			for (String hostName : hosts) {
				DBAccess db = dbs.get(hostName);
				db.destroy();
				dbs.remove(hostName);
			}
		}
	}
	
	CybershakeSiteInfo2DB csSiteDB = null;
	
	public static boolean timer = false;
	
	/**
	 * putting the Cybershake location information in the database
	 * @param forecast
	 * @param erfId
	 */
	private void putSiteInfoInDB(ERF forecast,int erfId){
		/**
	     * Site List for Cybershake
	     */
		CybershakeSiteInfo2DB sites = new CybershakeSiteInfo2DB(getDB());
		
		//USC
		System.out.println("Doing Site USC");
		double USC_LAT = 34.019200;
		double USC_LON = -118.28600;
		int siteId = sites.getCybershakeSiteId("USC");
		//siteId= sites.putCybershakeLocationInDB("USC", "USC", USC_LAT, USC_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, USC_LAT, USC_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, USC_LAT, USC_LON);
	    
	    //PAS
	    System.out.println("Doing Site PAS");
	    double PAS_LAT = 34.148427;
	    double PAS_LON = -118.17119;
	    siteId = sites.getCybershakeSiteId("PAS");
	    //siteId = sites.putCybershakeLocationInDB("PAS", "PAS",PAS_LAT ,PAS_LON );
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, PAS_LAT, PAS_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, PAS_LAT, PAS_LON);

	    //LADT
	    System.out.println("Doing Site LADT");
	    double LADT_LAT = 34.052041;
	    double LADT_LON = -118.25713;
	    siteId = sites.getCybershakeSiteId("LADT");
	    //siteId= sites.putCybershakeLocationInDB("LADT", "LADT", LADT_LAT, LADT_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, LADT_LAT, LADT_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, LADT_LAT, LADT_LON);

	    //LBP
	    System.out.println("Doing Site LBP");
	    double LBP_LAT = 33.754944;
	    double LBP_LON = -118.22300;
	    siteId = sites.getCybershakeSiteId("LBP");
	    //siteId = sites.putCybershakeLocationInDB("LBP", "LBP", LBP_LAT,LBP_LON );
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, LBP_LAT, LBP_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, LBP_LAT, LBP_LON);

	    
	    //WNGC
	    System.out.println("Doing Site WNGC");
	    double WNGC_LAT = 34.041823;
	    double WNGC_LON = -118.06530;
	    siteId = sites.getCybershakeSiteId("WNGC");
	    //siteId = sites.putCybershakeLocationInDB("WNGC", "WNGC", WNGC_LAT,WNGC_LON );
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, WNGC_LAT, WNGC_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, WNGC_LAT, WNGC_LON);

	    //SABD
	    System.out.println("Doing Site SABD");
	    double SABD_LAT = 33.754111;
	    double SABD_LON = -117.86778;
	    siteId = sites.getCybershakeSiteId("SABD");
	    //siteId = sites.putCybershakeLocationInDB("SABD", "SABD",SABD_LAT , SABD_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, SABD_LAT, SABD_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, SABD_LAT, SABD_LON);
	    
	    //SBSM
	    System.out.println("Doing Site SBSM");
	    double SBSM_LAT = 34.064986;
	    double SBSM_LON = -117.29201;
	    siteId = sites.getCybershakeSiteId("SBSM");
	    //siteId = sites.putCybershakeLocationInDB("SBSM", "SBSM", SBSM_LAT, SBSM_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, SBSM_LAT, SBSM_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, SBSM_LAT, SBSM_LON);

	    
	    //FFI
	    System.out.println("Doing Site FFI");
	    double FFI_LAT = 34.336030;
	    double FFI_LON = -118.50862;
	    siteId = sites.getCybershakeSiteId("FFI");
	    //siteId = sites.putCybershakeLocationInDB("FFI", "FFI", FFI_LAT, FFI_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, FFI_LAT, FFI_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, FFI_LAT, FFI_LON);

	    //CCP
	    System.out.println("Doing Site CCP");
	    double CCP_LAT = 34.054884;
	    double CCP_LON = -118.41302;
	    siteId = sites.getCybershakeSiteId("CCP");
	    //siteId = sites.putCybershakeLocationInDB("CCP", "CCP", CCP_LAT, CCP_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, CCP_LAT, CCP_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, CCP_LAT, CCP_LON);
	    
	    //SMCA
	    System.out.println("Doing Site SMCA");
	    double SMCA_LAT = 34.009092;
	    double SMCA_LON = -118.48939;
	    siteId = sites.getCybershakeSiteId("SMCA");
	    //siteId = sites.putCybershakeLocationInDB("SMCA", "SMCA", SMCA_LAT, SMCA_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, SMCA_LAT, SMCA_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, SMCA_LAT, SMCA_LON);
		
		//PTWN
	    System.out.println("Doing Site PTWN");
	    double PTWN_LAT = 34.14280;
	    double PTWN_LON = -116.49771;
	    siteId = sites.getCybershakeSiteId("PTWN");
	    //int siteId = sites.putCybershakeLocationInDB("Pioneer Town", "PTWN", PTWN_LAT, PTWN_LON);
	    sites.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, PTWN_LAT, PTWN_LON, false);
	    sites.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, PTWN_LAT, PTWN_LON);

	}
	
	/**
	 * puts a list of Cybershake location information into the database
	 * @param list of locations (SiteInsert's)
	 * @param forecast
	 * @param erfId
	 * @param siteDB object
	 */
	private void putSiteListInfoInDB(List<CybershakeSite> sites, ERF forecast,int erfId, CybershakeSiteInfo2DB siteDB, boolean checkAdd){
		ArrayList<int[]> newRups = new ArrayList<int[]>();
		int i=0;
		int numSites = sites.size();
		for (CybershakeSite newsite : sites) {
			System.out.println("Doing Site " + newsite.name + " (" + newsite.short_name + "), " + ++i + " of " + numSites + " (" + getPercent(i, numSites) + ")");
			System.out.println("Putting location into DB");
			int siteId= siteDB.putCybershakeLocationInDB(newsite);
			Preconditions.checkState(siteId >= 0);
			CybershakeSite inserted = siteDB.getSiteFromDB(newsite.short_name);
			if (inserted.type_id <= 0 && newsite.type_id > 0) {
				siteDB.getSitesDB().setSiteType(siteId, newsite.type_id);
				System.out.println("Setting site type ID for site "+siteId+" to "+newsite.type_id);
			}
//			siteId = siteDB.getCybershakeSiteId(newsite.short_name);
			System.out.println("Putting regional bounds into DB");
			siteDB.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, newsite.lat, newsite.lon, false);
			System.out.println("Putting Source Rupture info into DB");
			newRups.addAll(siteDB.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, newsite.lat, newsite.lon, checkAdd, "newRupsForScott.txt"));
		}
		System.out.println("New ruptures...");
		for (int[] rup : newRups) {
			System.out.println(rup[0] + " " + rup[1]);
		}
	}
	
	public void putSiteListRupsIntoDB(ArrayList<CybershakeSite> sites, ERF forecast, int erfId, CybershakeSiteInfo2DB siteDB) {
		ArrayList<int[]> newRups = new ArrayList<int[]>();
		int i=0;
		int numSites = sites.size();
		for (CybershakeSite newsite : sites) {
			System.out.println("Doing Site " + newsite.name + " (" + newsite.short_name + "), " + ++i + " of " + numSites + " (" + getPercent(i, numSites) + " %)");
			
			int siteId = siteDB.getCybershakeSiteId(newsite.short_name);
			System.out.println("Putting regional bounds into DB");
			siteDB.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, newsite.lat, newsite.lon, false);
			System.out.println("Putting Source Rupture info into DB");
			newRups.addAll(siteDB.putCyberShakeLocationSrcRupInfo(forecast, erfId, siteId, newsite.lat, newsite.lon, true));
		}
		System.out.println("New ruptures...");
		for (int[] rup : newRups) {
			System.out.println(rup[0] + " " + rup[1]);
		}
	}
	
	public void updateSiteRegionalBounds(ArrayList<CybershakeSite> sites, ERF forecast, int erfId, CybershakeSiteInfo2DB siteDB) {
		int i=0;
		int numSites = sites.size();
		for (CybershakeSite newsite : sites) {
			System.out.println("Doing Site " + newsite.name + " (" + newsite.short_name + "), " + ++i + " of " + numSites + " (" + getPercent(i, numSites) + " %)");
			int siteId = siteDB.getCybershakeSiteId(newsite.short_name);
			System.out.println("Putting regional bounds into DB");
			siteDB.putCyberShakeLocationRegionalBounds(forecast, erfId, siteId, newsite.lat, newsite.lon, true);
		}
	}
	
	private String getPercent(int small, int big) {
		double percent = (double)((int)(((double)small / (double)big * 10000) + 0.5)) / 100d;
		return Double.toString(percent);
	}
	
	/**
	 * Gets site info DB object
	 * @return CybershakeSiteInfo2DB
	 */
	private CybershakeSiteInfo2DB getSiteInfoObject() {
		if (csSiteDB == null)
			csSiteDB = new CybershakeSiteInfo2DB(getDB());
		return csSiteDB;
	}
	
	public void setSiteInfoObject(CybershakeSiteInfo2DB csSiteDB) {
		this.csSiteDB = csSiteDB;
	}
	
	public ArrayList<CybershakeSite> getSiteListFromFile(String fileName) throws FileNotFoundException, IOException {
		ArrayList<CybershakeSite> sites = new ArrayList<CybershakeSite>();
		
		System.out.println("Loading sites from " + fileName);
		
		ArrayList<String> lines = FileUtils.loadFile(fileName);
		for (String line : lines) {
			StringTokenizer tok = new StringTokenizer(line);
			
			// get the Longitude from the file
			double lon = Double.parseDouble(tok.nextToken());
			// get the Latitude from the file
			double lat = Double.parseDouble(tok.nextToken());
			// short name
			String shortName = tok.nextToken().trim();
			// long name
			//String longName = tok.nextToken().trim();
			//String longName = shortName;
			String longName = line.substring(line.indexOf(tok.nextToken()));
			
			CybershakeSite site = new CybershakeSite(lat, lon, longName, shortName);
			//if (!(site.short_name.equals("PDU") || site.short_name.equals("TAB"))) {
			if (!site.short_name.equals("PDU")) {
				continue;
			}
			System.out.println(site);
			sites.add(site);
		}
		
		System.out.println("Loaded " + sites.size() + " sites!");
		
		return sites;
	}
	
	public List<CybershakeSite> getAllSites() {
		return this.getAllSites(0);
	}
	
	public List<CybershakeSite> getAllSites(int minIndex) {
		SiteInfo2DB siteInfoDB = new SiteInfo2DB(getDB());
		
		List<CybershakeSite> sites = siteInfoDB.getAllSitesFromDB();
		
		List<String> shortNames = siteInfoDB.getAllSites();
		
		if (minIndex > 0) {
			List<CybershakeSite> clone = Lists.newArrayList(sites);
			
			for (CybershakeSite site : sites) {
				
				if (site.id < minIndex)
					clone.remove(site);
			}
			return clone;
		}
		
		return sites;
	}
	
	public void insertNewERFForSites(List<CybershakeSite> sites, ERF2DB erf2db, String name, String description, boolean forceAdd) {
		// get a new ERF-ID
		int erfID = erf2db.getInserted_ERF_ID(name);
		
		SiteInfo2DB siteInfoDB = null;
		
		int i=0;
		int numSites = sites.size();
		CybershakeSiteInfo2DB cyberSiteDB = this.getSiteInfoObject();
		cyberSiteDB.setForceAddRuptures(forceAdd);
		ERF erf = erf2db.getERF_Instance();
		for (CybershakeSite site : sites) {
			System.out.println("Doing Site " + site.name + " (" + site.short_name + "), " + ++i + " of " + numSites + " (" + getPercent(i, numSites) + " %)");
			if (site.id < 0) {
				if (siteInfoDB == null)
					siteInfoDB = new SiteInfo2DB(getDB());
				site.id = siteInfoDB.getSiteId(site.short_name);
			}
			System.out.println("Putting regional bounds into DB");
			cyberSiteDB.putCyberShakeLocationRegionalBounds(erf, erfID, site.id, site.lat, site.lon, false);
			System.out.println("Putting Source Rupture info into DB");
			cyberSiteDB.putCyberShakeLocationSrcRupInfo(erf, erfID, site.id, site.lat, site.lon);
		}
	}
	
	public void insertNewERFForAllSites(ERF2DB erf2db, String name, String description) {
		List<CybershakeSite> sites = this.getAllSites();
		
		this.insertNewERFForSites(sites, erf2db, name, description, false);
	}
	
	public static DBAccess getAuthenticatedDBAccess(boolean exitOnCancel, boolean allowReadOnly) throws IOException {
		return getAuthenticatedDBAccess(exitOnCancel, allowReadOnly, HOST_NAME);
	}
	
	public static DBAccess getAuthenticatedDBAccess(boolean exitOnCancel, boolean allowReadOnly, String hostName)
			throws IOException {
		UserAuthDialog auth = new UserAuthDialog(null, exitOnCancel, allowReadOnly);
		auth.setVisible(true);
		auth.validate();
		DBAccess db;
		if (auth.isReadOnly()) {
			db = new DBAccess(hostName, DATABASE_NAME);
			db.setReadOnly(true);
		} else {
			db = new DBAccess(hostName, DATABASE_NAME, auth.getUsername(), new String(auth.getPassword()));
			db.setReadOnly(false);
		}
		return db;
	}
	
	public static DBAccess getAuthenticatedDBAccess(boolean exitOnCancel) throws IOException {
		return getAuthenticatedDBAccess(exitOnCancel, false);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		HOST_NAME = "moment.usc.edu";
		Cybershake_OpenSHA_DBApplication app = new Cybershake_OpenSHA_DBApplication();
		//NSHMP2002_ToDB erfDB = new NSHMP2002_ToDB(db);
		// String erfDescription = "NSHMP 2002 (Frankel02) Earthquake Rupture Forecast Model";
		DBAccess db = getAuthenticatedDBAccess(true, true);
		if (db.isReadOnly())
			db.setIgnoreInserts(true);
		
		boolean highRes = true;
		boolean ddwAdjust = false;
		System.out.println("Creating and Updating ERF...");
		MeanUCERF2_ToDB erfDB  = new MeanUCERF2_ToDB(db, highRes, ddwAdjust);
		File erfDir = new File("/home/kevin/CyberShake/UCERF2_200m_noDDW");
		erfDB.setFileBased(erfDir, true);
		String erfName = erfDB.getERF_Instance().getName();
		String erfDescription;
		if (highRes)
			erfDescription = "Mean UCERF 2 - Single Branch Earthquake Rupture Forecast FINAL, 200m";
		else
			erfDescription = "Mean UCERF 2 - Single Branch Earthquake Rupture Forecast FINAL";
		
		if (ddwAdjust) {
			erfDescription += ", No DDW Adjustment";
			if (!highRes)
				// already added if it is high res
				erfName += ", No DDW";
		}
		
		ERF forecast = erfDB.getERF_Instance();
		System.out.println("ERF NAME: " + erfName);
//		int erfId = erfDB.getInserted_ERF_ID(erfName);
//		System.out.println("ERF ID: " + erfId);
		
//		System.exit(0);
		
		CybershakeSiteInfo2DB siteDB = new CybershakeSiteInfo2DB(db);
		app.setSiteInfoObject(siteDB);
		
		///////////////// INSERT ERF //////////////////////
		
		
		GriddedRegion region = null;
		
		// uncomment and modify if you only want to insert for a region
//		LocationList corners = new LocationList();
//		corners.addLocation(new Location(34.19, -116.60));
//		corners.addLocation(new Location(35.33, -118.75));
//		corners.addLocation(new Location(34.13, -119.63));
//		corners.addLocation(new Location(33.00, -117.50));
//		double gridSpacing = 0.2;
//		region = new GriddedRegion(corners, gridSpacing);
		
		// this inserts it
		// TODO deal with rakes along strike before inserting UCERF3
		erfDB.insertForecaseInDB(erfName, erfDescription, region);
		
		// if you have to reinsert a rupture surface for some reason, do this
//		int erfID = 35;
//		int sourceID = 7;
//		int ruptureID = 0;
//		erfDB.insertSrcRupInDB(forecast, erfID, sourceID, ruptureID);
		
		// this inserts the site info
		siteDB.setMatchSourceNames(false);
		app.insertNewERFForAllSites(erfDB, erfName, erfDescription);
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		ArrayList<CybershakeSite> sites = Lists.newArrayList();
//		CybershakeSiteInfo2DB.CUT_OFF_DISTANCE = 300;
//		CybershakeSiteInfo2DB.FORCE_CUTOFF = true;
//		sites.add(sites2db.getSiteFromDB("s040"));
//		sites.add(sites2db.getSiteFromDB("s064"));
//		sites.add(sites2db.getSiteFromDB("s758"));
//		sites.add(sites2db.getSiteFromDB("s778"));
//		sites.add(sites2db.getSiteFromDB("STNI"));
		sites.add(sites2db.getSiteFromDB("SBSM"));
		sites.add(sites2db.getSiteFromDB("USC"));
////		sites.add(sites2db.getSiteFromDB(73));
////		sites.add(sites2db.getSiteFromDB(978));
//		sites.add(sites2db.getSiteFromDB(999));
//		sites.add(sites2db.getSiteFromDB(1000));
//		sites.add(sites2db.getSiteFromDB(1001));
		app.insertNewERFForSites(sites, erfDB, erfName, erfDescription, false);
		
		/////////////// ADD SITES //////////////////////
		/*
		
		boolean checkAdd = false;
		
		List<CybershakeSite> site_list = new ArrayList<CybershakeSite>();
//		site_list.addAll(loadSitesFromCSV(new File("/tmp/all_but_10km_short_names.csv")));
//		site_list.addAll(loadSitesFromCSV(new File("/tmp/20km_10km_5km_sites.csv")));
		site_list.addAll(loadSitesFromCSV(new File("/tmp/new_sites.csv")));
//		site_list = site_list.subList(0, 1);
//		site_list.add(new CybershakeSite(33.88110, -118.17568, "Lighthipe", "LTP"));
//		site_list.add(new CybershakeSite(34.10647, -117.09822, "Seven Oaks Dam", "SVD"));
//		site_list.add(new CybershakeSite(34.557, -118.125, "Lake Palmdale", "LAPD"));
//		site_list.add(new CybershakeSite(34.39865, -118.912, "Filmore Central Park", "FIL"));
//		site_list.add(new CybershakeSite(33.93088, -118.17881, "Seven Ten-Ninety Interchange ", "STNI"));
//		site_list.add(new CybershakeSite(34.019200, -118.28600, "CyberShake Verification Test - USC", "TEST"));
		
		app.putSiteListInfoInDB(site_list, forecast, erfId, siteDB, checkAdd);
		
		*/
		
		db.destroy();
		
		System.out.println("Done!");
		
		System.exit(0);
	}
	
	private static List<CybershakeSite> loadSitesFromCSV(File cvsFile) throws IOException {
		CSVFile<String> csv = CSVFile.readFile(cvsFile, true);
		
		 List<CybershakeSite> sites = Lists.newArrayList();
		
		for (int row=0; row<csv.getNumRows(); row++) {
			int typeID = Integer.parseInt(csv.get(row, 0));
			String name = csv.get(row, 1);
			String shortName = csv.get(row, 2);
			double lat = Double.parseDouble(csv.get(row, 3));
			double lon = Double.parseDouble(csv.get(row, 4));
			
			sites.add(new CybershakeSite(-1, lat, lon, name, shortName, typeID));
		}
		
		return sites;
	}
}
