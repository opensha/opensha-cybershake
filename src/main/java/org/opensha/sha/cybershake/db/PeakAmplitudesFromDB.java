package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ListUtils;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PeakAmplitudesFromDB implements PeakAmplitudesFromDBAPI {

	
	protected DBAccess dbaccess;
	private Runs2DB runs2db;
	
	public static final String TABLE_NAME = "PeakAmplitudes";

	
	public PeakAmplitudesFromDB(DBAccess dbaccess){
		this.dbaccess = dbaccess;
		runs2db = new Runs2DB(dbaccess);
	}
	
	public ArrayList<Integer> getPeakAmpSites() {
		ArrayList<Integer> runs = getPeakAmpRunIDs();
		ArrayList<Integer> ids = new ArrayList<Integer>();
		
		for (int id : runs) {
			ids.add(runs2db.getSiteID(id));
		}
		
		return ids;
	}
	
	public ArrayList<Integer> getPeakAmpRunIDs() {
		return getDistinctIntVal("Run_ID", null);
	}
	
	public ArrayList<CybershakeRun> getPeakAmpRuns() {
		ArrayList<Integer> ids = getPeakAmpRunIDs();
		ArrayList<CybershakeRun> runs = new ArrayList<CybershakeRun>();
		
		for (int id : ids) {
			runs.add(runs2db.getRun(id));
		}
		
		return runs;
	}
	
	public ArrayList<CybershakeRun> getPeakAmpRuns(int siteID, int erfID, int sgtVarID, int rupVarScenID, int velModelID) {
		ArrayList<CybershakeRun> runs = runs2db.getRuns(siteID, erfID, sgtVarID, rupVarScenID, velModelID, null, null, null, null);
		ArrayList<Integer> ids = getPeakAmpRunIDs();
		
		ArrayList<CybershakeRun> ampsRuns = new ArrayList<CybershakeRun>();
		
		for (CybershakeRun run : runs) {
			for (int id : ids) {
				if (id == run.getRunID()) {
					ampsRuns.add(run);
					break;
				}
			}
		}
		
		return ampsRuns;
	}
	
	public ArrayList<Integer> getDistinctIntVal(String selectCol, String whereClause) {
		ArrayList<Integer> vals = new ArrayList<Integer>();
		
		String sql = "SELECT distinct " + selectCol + " FROM " + TABLE_NAME;
		
		if (whereClause != null && whereClause.length() > 0) {
			sql += " WHERE " + whereClause;
		}
		
//		System.out.println(sql);
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
				int id = rs.getInt(selectCol);
				vals.add(id);
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return vals;
	}
	
	public boolean hasAmps(int siteID, int erfID) {
		return hasAmps(siteID, erfID, -1, -1, -1);
	}
	
	public boolean hasAmps(int siteID, int erfID, int rupVarScenID, int sgtVarID, int velModelID) {
		ArrayList<Integer> runs = runs2db.getRunIDs(siteID, erfID, sgtVarID, rupVarScenID, velModelID, null, null, null, null);
		if (runs.size() == 0)
			return false;
		
		ArrayList<Integer> ampsRuns = getPeakAmpRunIDs();
		
		for (int id : runs) {
			for (int ampsRun : ampsRuns) {
				if (id == ampsRun)
					return true;
			}
		}
		
		return false;
	}
	
	public boolean hasAmps(int runID) {
		String sql = "SELECT * FROM " + TABLE_NAME + " WHERE Run_ID=" + runID + " LIMIT 1";
		
		try {
			ResultSet rs = dbaccess.selectData(sql);
			
			boolean good = rs.next();
			
			rs.close();
			
			return good;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * @return the supported SA Period as list of strings.
	 */
	public ArrayList<CybershakeIM>  getSupportedIMs(){
		String sql = "SELECT I.IM_Type_ID,I.IM_Type_Measure,I.IM_Type_Value,I.Units,I.IM_Type_Component"
				+ " from IM_Types I JOIN (";
		sql += "SELECT distinct IM_Type_ID from " + TABLE_NAME;
		sql += ") A ON A.IM_Type_ID=I.IM_Type_ID";
		
//		System.out.println(sql);

		ArrayList<CybershakeIM> ims = new ArrayList<CybershakeIM>();
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
				try {
					ims.add(CybershakeIM.fromResultSet(rs));
				} catch (IllegalStateException e) {
					System.out.println("Skipping IM: "+e.getMessage());
				}
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ims;
	}
	
	/**
	 * @return the supported SA Period as list of strings.
	 */
	public List<CybershakeIM>  getAllIMs(){
		String sql = "SELECT I.IM_Type_ID,I.IM_Type_Measure,I.IM_Type_Value,I.Units,I.IM_Type_Component"
				+ " from IM_Types I";
		
//		System.out.println(sql);

		ArrayList<CybershakeIM> ims = new ArrayList<CybershakeIM>();
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
				try {
					ims.add(CybershakeIM.fromResultSet(rs));
				} catch (IllegalStateException e) {
					System.out.println("Skipping IM: "+e.getMessage());
				}
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ims;
	}
	
	public List<CybershakeIM> getIMs(List<Double> periods, IMType imType, CyberShakeComponent comp) {
		Preconditions.checkArgument(imType != null, "must specify IM type");
		Preconditions.checkArgument(comp != null, "must specify component");
		Preconditions.checkArgument(periods != null && !periods.isEmpty(),
				"must supply at least one period");
		List<CybershakeIM> allIMs = getAllIMs();
		
		double maxPDiff = 1d;
		
		List<CybershakeIM> matches = Lists.newArrayList();
		
		for (double period : periods) {
			CybershakeIM closest = null;
			double minDiff = Double.POSITIVE_INFINITY;
			for (CybershakeIM im : allIMs) {
				if (im.getMeasure() != imType || im.getComponent() != comp)
					continue;
				double pDiff = DataUtils.getPercentDiff(im.getVal(), period);
				if (pDiff < minDiff) {
					minDiff = pDiff;
					closest = im;
				}
			}
			
			if (minDiff > maxPDiff) {
				matches.add(null);
				System.out.println("WARNING: No period within 1% of "+period
						+". Closest: "+closest.getVal());
			} else {
				matches.add(closest);
			}
		}
		
		return matches;
	}
	
	public int countAmps(int runID, CybershakeIM im) {
		String sql = "SELECT count(*) from " + TABLE_NAME + " where Run_ID=" + runID;
		if (im != null)
			sql += " and IM_Type_ID="+im.getID();
		System.out.println(sql);
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		int count;
		try {
			rs.next();
			count = rs.getInt(1);
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			count = -1;
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {}
		}
		return count;
	}
	
	/**
	 * @return the supported SA Period as list of strings.
	 */
	public ArrayList<CybershakeIM>  getSupportedIMs(int runID) {
		long startTime = System.currentTimeMillis();
//		String sql = "SELECT I.IM_Type_ID,I.IM_Type_Measure,I.IM_Type_Value,I.Units,I.IM_Type_Component"
//				+ " from IM_Types I JOIN (";
//		sql += "SELECT distinct IM_Type_ID from " + TABLE_NAME + " WHERE " + whereClause;
//		sql += ") A ON A.IM_Type_ID=I.IM_Type_ID";
		String sql = "SELECT DISTINCT I.IM_Type_ID,I.IM_Type_Measure,I.IM_Type_Value,I.Units,I.IM_Type_Component";
		sql += "\nFROM IM_Types I, PeakAmplitudes P";
		sql += "\nWHERE P.Run_ID="+runID+" AND P.IM_Type_ID=I.IM_Type_ID";
		sql += "\nAND P.Source_ID=(select min(Source_ID) from PeakAmplitudes where Run_ID="+runID+")";
		
		System.out.println(sql);
		
//		System.out.println(sql);
		ArrayList<CybershakeIM> ims = new ArrayList<CybershakeIM>();
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
				try {
					ims.add(CybershakeIM.fromResultSet(rs));
				} catch (IllegalStateException e) {
					System.out.println("Skipping IM: "+e.getMessage());
				}
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
//			e.printStackTrace();
		}
		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Total SA Period Select Time: " + ((double)duration / 1000) + " sec");
		return ims;
	}
	
	/**
	 * 
	 * @param erfId
	 * @param srcId
	 * @param rupId
	 * @return the rupture variation ids for the rupture
	 */
	public ArrayList<Integer> getRupVarationsForRupture(int erfId,int srcId, int rupId){
		String sql = "SELECT Rup_Var_ID from Rupture_Variations where Source_ID = '"+srcId+"' "+
		             "and ERF_ID =  '"+erfId +"' and Rup_Var_Scenario_ID='3' and Rupture_ID = '"+rupId+"'";
		
		ArrayList<Integer> rupVariationList = new ArrayList<Integer>();
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
			  String rupVariation = rs.getString("Rup_Var_ID");	
			  rupVariationList.add(Integer.parseInt(rupVariation));
			  rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rupVariationList;
	}
	
	/**
	 * 
	 * @param siteId
	 * @param erfId
	 * @param srcId
	 * @param rupId
	 * @param rupVarId
	 * @return the IM Value for the particular IM type
	 */
	public double getIM_Value(int runID, int srcId, int rupId, int rupVarId, CybershakeIM im) throws SQLException {
		String sql = "SELECT distinct IM_Value from " + TABLE_NAME + " where Source_ID = '"+srcId+"' "+
        "and Run_ID =  '"+runID +"' and Rupture_ID = '"+rupId+"' " +
        "and IM_Type_ID = '"+im.getID()+"' and Rup_Var_ID = '"+rupVarId+"'";
//		System.out.println(sql);
		ResultSet rs = dbaccess.selectData(sql);
		rs.next();
		double imVal = Double.parseDouble(rs.getString("IM_Value"));	
		rs.close();
		return imVal;
	}
	
	/**
	 * 
	 * @param siteId
	 * @param erfId
	 * @param srcId
	 * @param rupId
	 * @throws SQLException 
	 * @return the a list of IM Values for the particular IM type
	 */
	public List<Double> getIM_Values(int runID, int srcId, int rupId, CybershakeIM im) throws SQLException{
		String sql = "SELECT IM_Value from " + TABLE_NAME + " where Run_ID=" + runID + " and Source_ID = '"+srcId+"' "+
        "and Rupture_ID = '"+rupId+"' and IM_Type_ID = '"+im.getID()+"' ORDER BY Rup_Var_ID";
//		System.out.println(sql);
		ResultSet rs = null;
		ArrayList<Double> vals = new ArrayList<Double>();
		try {
			rs = dbaccess.selectData(sql);
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}
		rs.next();
		vals.add(rs.getDouble("IM_Value"));
//		vals.add(Double.parseDouble(rs.getString("IM_Value")));
		while (rs.next()) {
			vals.add(rs.getDouble("IM_Value"));
//			vals.add(Double.parseDouble(rs.getString("IM_Value")));
		}
		rs.close();
		return vals;
	}
	
	 /**
	  * @return all possible SGT Variation IDs
	  */
	public ArrayList<Integer> getSGTVarIDs() {
		ArrayList<Integer> vars = new ArrayList<Integer>();
		
		String sql = "SELECT SGT_Variation_ID from SGT_Variation_IDs order by SGT_Variation_ID desc";
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
			  int id = rs.getInt("SGT_Variation_ID");
			  vars.add(id);
			  rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return vars;
	}
	
	/**
	 * @return all possible Rup Var Scenario IDs
	 */
	public ArrayList<Integer> getRupVarScenarioIDs() {
		ArrayList<Integer> vars = new ArrayList<Integer>();
		
		String sql = "SELECT Rup_Var_Scenario_ID from Rupture_Variation_Scenario_IDs order by Rup_Var_Scenario_ID desc";
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			rs.next();
			while(!rs.isAfterLast()){
			  int id = rs.getInt("Rup_Var_Scenario_ID");
			  vars.add(id);
			  rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return vars;
	}
	
	public CybershakeIM getSupportedIMForPeriod(double period, IMType imType, CyberShakeComponent component, int runID) {
		return this.getSupportedIMForPeriod(period, imType, component, runID, null);
	}
	
	public CybershakeIM getSupportedIMForPeriod(double period, IMType imType, CyberShakeComponent component,
			int runID, HazardCurve2DB curve2db) {
		ArrayList<Double> periods = new ArrayList<Double>();
		periods.add(period);
		
		return getSupportedIMForPeriods(periods, imType, component, runID, curve2db).get(0);
	}
	
	public ArrayList<CybershakeIM> getSupportedIMForPeriods(
			List<Double> periods, IMType imType, CyberShakeComponent component, int runID) {
		return this.getSupportedIMForPeriods(periods, imType, component, runID, null);
	}
	
	public ArrayList<CybershakeIM> getSupportedIMForPeriods(
			List<Double> periods, IMType imType, CyberShakeComponent component, int runID, HazardCurve2DB curve2db) {
		ArrayList<CybershakeIM> supported = this.getSupportedIMs(runID);
		if (curve2db != null) {
			supported.addAll(curve2db.getSupportedIMs(runID));
		}
		
		ArrayList<CybershakeIM> matched = new ArrayList<CybershakeIM>();
		
		if (supported.size() == 0)
			return null;
		
		double maxDist = 0.5;
		
		for (double period : periods) {
			CybershakeIM closest = null;
			double dist = Double.POSITIVE_INFINITY;
			
			for (CybershakeIM im : supported) {
				if (component != null && component != im.getComponent())
					continue;
				if (imType != null && imType != im.getMeasure())
					continue;
				double val = Math.abs(period - im.getVal());
//				System.out.println("Comparing " + val + " to " + im.getVal());
				if (val < dist && val <= maxDist) {
					closest = im;
					dist = val;
				}
			}
			if (dist != Double.POSITIVE_INFINITY)
				System.out.println("Matched " + period + " with " + closest.getVal());
			else
				System.out.println("NO MATCH FOR period: "+period+", type: "+imType+", component: "+component+"!!!");
			matched.add(closest);
		}
		
		return matched;
	}
	
	public int deleteAllAmpsForSite(int siteID) {
		ArrayList<Integer> runs = runs2db.getRunIDs(siteID);
		int rows = 0;
		for (int runID : runs) {
			int num = deleteAmpsForRun(runID);
			if (num < 0)
				return -1;
			
		}
		return rows;
	}
	
	public int deleteAmpsForRun(int runID) {
		String sql = "DELETE FROM " + TABLE_NAME + " WHERE Run_ID="+runID;
		System.out.println(sql);
		try {
			return dbaccess.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
//			TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	public ArrayList<PeakAmplitudesRecord> getPeakAmpRecord(int runID, int sourceID, int rupID) {
		String sql = "SELECT * FROM "+TABLE_NAME+" WHERE Run_ID="+runID+" AND Source_ID="+sourceID+" AND Rupture_ID="+rupID;
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		ArrayList<PeakAmplitudesRecord> amps = new ArrayList<PeakAmplitudesRecord>();
		
		try {
			rs.next();
			while(!rs.isAfterLast()){
				amps.add(PeakAmplitudesRecord.fromResultSet(rs));
				rs.next();
			}
			
			return amps;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * CyberShake periods can be a little nasty, such as 3.00003. This will fix them.
	 * @param period
	 * @return
	 */
	public static double getCleanedCS_Period(double period) {
		if (period >= 1d)
			period = Math.round(period*1000d)/1000d;
		return period;
	}

	public static void main(String args[]) {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		PeakAmplitudesFromDB amps = new PeakAmplitudesFromDB(db);
		
//		ArrayList<CybershakeIM> ims = amps.getSupportedIMForPeriods(ListUtils.wrapInList(0.1d),
//				IMType.SA, CyberShakeComponent.GEOM_MEAN, 885, new HazardCurve2DB(db));
		ArrayList<CybershakeIM> ims = amps.getSupportedIMs(4266);
		for (CybershakeIM im : ims)
			System.out.println(im);
		
//		System.out.println(amps.getPeakAmpSites().size() + " sites");
//		System.out.println(amps.getPeakAmpRuns().size() + " runs");
//		
//		System.out.println("Amps for 90, 36? " + amps.hasAmps(90, 36) + " (false!)");
//		System.out.println("Amps for 90, 35? " + amps.hasAmps(90, 35) + " (true!)");
//		System.out.println("Amps for run 1? " + amps.hasAmps(1) + " (false!)");
//		System.out.println("Amps for run 216? " + amps.hasAmps(216) + " (true!)");
//		
//		for (CybershakeIM im : amps.getSupportedIMs()) {
//			System.out.println("IM: " + im);
//		}
//		
//		System.out.println("Count for 216: " + amps.countAmps(216, null));
		
		db.destroy();
		
		System.exit(0);
	}
	
}
