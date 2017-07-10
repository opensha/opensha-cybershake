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

import java.util.List;

import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;

public interface SiteInfo2DBAPI {
	
	/**
	 * Inserts the new site in the database table CyberShake_Sites
	 * @param site
	 * @return the Site Id for the last inserted Site
	 */
	public int insertSite(CybershakeSite site);
	
	/**
	 * Inserts the rupture ids that correspond to a given site within gven cutoff distance
	 * inside the table Site_Ruptures.
	 * @param siteId
	 * @param erfId
	 * @param sourceId
	 * @param ruptureId
	 * @param cutoffDistance
	 */
	public void insertSite_RuptureInfo(int siteId,int erfId,int sourceId,int ruptureId,double cutoffDistance);
	
	/**
	 * Inserts thelist of rupture ids that correspond to a given site within given cutoff distance
	 * inside the table Site_Ruptures.
	 * @param siteId
	 * @param erfId
	 * @param sourceId
	 * @param ruptureId - list of Rupture IDs
	 * @param rupDistsToAdd 
	 * @param cutoffDistance
	 */
	public void insertSite_RuptureInfoList(int siteId, int erfId, int sourceId, List<Integer> ruptureId, List<Double> rupDistsToAdd, double cutOffDistance);
	
	/**
	 * Inserts the regional bounds (min/max lat/lon) for all cybershake sites in table Site_Region.
	 * @param siteId
	 * @param erfId
	 * @param cutOffDistance
	 * @param maxLat
	 * @param maxLatSrcId
	 * @param maxLatRupId
	 * @param minLat
	 * @param minLatSrcId
	 * @param minLatRupId
	 * @param maxLon
	 * @param maxLonSrcId
	 * @param maxLonRupId
	 * @param minLon
	 * @param minLonSrcId
	 * @param minLonRupId
	 */
	public void insertSiteRegionalBounds(int siteId,int erfId,double cutOffDistance,double maxLat,int maxLatSrcId,
			                             int maxLatRupId,double minLat,int minLatSrcId,int minLatRupId,
			                             double maxLon,int maxLonSrcId,int maxLonRupId,double minLon,
			                             int minLonSrcId,int minLonRupId);
	
	/**
	 * Updates the regional bounds (min/max lat/lon) for all cybershake sites in table Site_Region.
	 * @param siteId
	 * @param erfId
	 * @param cutOffDistance
	 * @param maxLat
	 * @param maxLatSrcId
	 * @param maxLatRupId
	 * @param minLat
	 * @param minLatSrcId
	 * @param minLatRupId
	 * @param maxLon
	 * @param maxLonSrcId
	 * @param maxLonRupId
	 * @param minLon
	 * @param minLonSrcId
	 * @param minLonRupId
	 */
	public void updateSiteRegionalBounds(int siteId,int erfId,double cutOffDistance,double maxLat,int maxLatSrcId,
			                             int maxLatRupId,double minLat,int minLatSrcId,int minLatRupId,
			                             double maxLon,int maxLonSrcId,int maxLonRupId,double minLon,
			                             int minLonSrcId,int minLonRupId);
	
	/**
	 * Returns the site id of the cybershake site for the corresponding cybershake_short_site_name
	 * @param cybershakeShortSiteName
	 * @return
	 */
	public int getSiteId(String cybershakeShortSiteName);
	
	/**
	 * Returns the site id the cybershake site with given lat and lon
	 * @param lat
	 * @param lon
	 * @return
	 */
	public int getSiteId(double lat,double lon);
	
	/**
	 * 
	 * @return the List of short site names for all Cybershake
	 */
	public List<String> getAllSites();
	
	/**
	 * Find out if the given rupture is already in the database
	 * @param erfID
	 * @param sourceID
	 * @param rupID
	 * @return
	 */
	public boolean isRupInDB(int erfID, int sourceID, int rupID);
	
	/**
	 * Find out if the given rupture is already in the database
	 * @param erfID
	 * @param sourceID
	 * @param rupID
	 * @param siteID
	 * @return
	 */
	public boolean isSiteRupInDB(int erfID, int sourceID, int rupID, int siteID);
	
	/**
	 * 
	 * @return the List of all cybershake site locations
	 */
	public LocationList getAllSitesLocation();
	
	
	/**
	 * 
	 * @param siteShortName short site name as in database for Cybershake site
	 * @return the Earthquake rupture forecast source id's for a given cybershake site.
	 */
	public List<Integer> getSrcIdsForSite(String siteShortName, int erf_ID);
	
	/**
	 * 
	 * @param siteShortName short site name as in database for Cybershake site
	 * @return the Earthquake rupture forecast source id's for a given cybershake site.
	 */
	public List<Integer> getSrcIdsForSite(int siteID, int erf_ID);
	
	/**
	 * 
	 * @param siteShortName
	 * @param srcId
	 * @return the list of rupture ids 
	 */
	public List<Integer> getRupIdsForSite(int siteID, int erf_ID, int srcId);
	
	/**
	 * 
	 * @param siteShortName
	 * @param srcId
	 * @return the list of rupture ids 
	 */
	public List<Integer> getRupIdsForSite(String siteShortName, int erf_ID, int srcId);
	
	/**
	 * 
	 * @param site
	 * @return the Location for the given cybershake site location
	 */
	public Location getLocationForSite(String site);
	
	/**
	 * Gets a CybershakeSite from the Database
	 * @param shortName
	 * @return
	 */
	public CybershakeSite getSiteFromDB(String shortName);
	
	/**
	 * Gets a CybershakeSite from the Database
	 * @param shortName
	 * @return
	 */
	public CybershakeSite getSiteFromDB(int siteID);
	
	/**
	 * Gets all CybershakeSite's from the Database
	 * @return
	 */
	public List<CybershakeSite> getAllSitesFromDB();

}
