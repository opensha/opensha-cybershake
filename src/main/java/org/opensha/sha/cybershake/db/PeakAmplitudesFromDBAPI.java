package org.opensha.sha.cybershake.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public interface PeakAmplitudesFromDBAPI {

	/**
	 * @return the supported SA Period as list of strings.
	 */
	public ArrayList<CybershakeIM>  getSupportedIMs();
	
	/**
	 * @return the supported SA Period as list of strings.
	 */
	public ArrayList<CybershakeIM>  getSupportedIMs(int runID);
	
	/**
	 * 
	 * @param erfId
	 * @param srcId
	 * @param rupId
	 * @return the rupture variation ids for the rupture
	 */
	public ArrayList<Integer> getRupVarationsForRupture(int erfId,int srcId, int rupId);
	
	/**
	 * 
	 * @param siteId
	 * @param erfId
	 * @param srcId
	 * @param rupId
	 * @param rupVarId
	 * @return the IM Value for the particular IM type
	 * @throws SQLException 
	 */
	public double getIM_Value(int runID, int srcId,int rupId,int rupVarId, CybershakeIM im) throws SQLException;
	
	/**
	 * 
	 * @param siteId
	 * @param erfId
	 * @param srcId
	 * @param rupId
	 * @throws SQLException 
	 * @return the a list of IM Values for the particular IM type
	 */
	public List<Double> getIM_Values(int runID, int srcId,int rupId, CybershakeIM im) throws SQLException;
	
	/**
	  * @return all possible SGT Variation IDs
	  */
	public ArrayList<Integer> getSGTVarIDs();
	
	/**
	 * @return all possible Rup Var Scenario IDs
	 */
	public ArrayList<Integer> getRupVarScenarioIDs();
	
	/**
	 * delete all peak amplitudes for a given site
	 * @param siteId
	 * @return
	 */
	public int deleteAllAmpsForSite(int siteID);
	
	/**
	 * delete all peak amplitudes for a given run
	 * @param siteId
	 * @return
	 */
	public int deleteAmpsForRun(int runID);
}
