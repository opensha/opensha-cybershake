package org.opensha.sha.cybershake.db;

import java.util.ArrayList;

import org.opensha.sha.earthquake.ERF;

public interface ERF2DBAPI {
	
	
	/**
	 * Get a list of all ERFs in the database
	 * @return
	 */
	public ArrayList<CybershakeERF> getAllERFs();
	
	/**
	 * Inserts the ERF parameters info in the table "ERF_Metadata"
	 * @param erfName
	 * @param attrName
	 * @param attrVal
	 */
	public void insertERFParams(int erfId,String attrName, String attrVal, String attrType,String attrUnits);
	
	/**
	 * 
	 * Inserts ERF name and description in table ERF_IDs
	 * @param erfName
	 * @param erfDesc
	 * @return
	 */
	public int insertERFId(String erfName, String erfDesc);
	

	/**
	 * Inserts source rupture information for the ERF in table "Ruptures"
	 * @param erfName
	 * @param sourceId
	 * @param ruptureId
	 * @param sourceName
	 * @param sourcetype
	 * @param magnitude
	 * @param probability
	 * @param gridSpacing
	 * @param numRows
	 * @param numCols
	 * @param numPoints
	 */
	public void insertERFRuptureInfo(int erfId ,int sourceId,int ruptureId,
			                        String sourceName,String sourcetype,double magnitude,
			                        double probability,double gridSpacing,
			                        double surfaceStartLat,double surfaceStartLon,double surfaceStartDepth,
			                        double surfaceEndLat,double surfaceEndLon,double surfaceEndDepth,
			                        int numRows,int numCols,int numPoints);
	
	/**
	 * Retrives the id of the ERF from the table ERF_IDs  for the corresponding ERF_Name.
	 * @param erfName
	 * @return
	 */
	public int getInserted_ERF_ID(String erfName);
	
	/**
	 * Retrives the rupture probability
	 * @param erfId
	 * @param sourceId
	 * @param rupId
	 * @return
	 */
	public double getRuptureProb(int erfId,int sourceId,int rupId);
	
	/**
	 * Insert the specified rupture from the given forecast
	 * @param forecast
	 * @param erfID
	 * @param sourceID
	 * @param rupID
	 */
	public void insertSrcRupInDB(ERF forecast, int erfID, int sourceID, int rupID);
	
	/**
	 * Deletes a rupture
	 * @param erfID
	 * @param srcID
	 * @param rupID
	 */
	public void deleteRupture(int erfID, int srcID, int rupID);
	
	/**
	 * Returns the ID for a given rupture name
	 * @param erfID
	 * @param name
	 * @return
	 */
	public int getSourceIDFromName(int erfID, String name);

}
