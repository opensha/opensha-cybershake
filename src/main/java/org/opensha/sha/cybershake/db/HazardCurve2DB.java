package org.opensha.sha.cybershake.db;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.gui.BatchSiteAddGUI.CybershakeCutoffSite;

import com.google.common.base.Preconditions;

public class HazardCurve2DB {
	
	public static final String TABLE_NAME = "Hazard_Curves";
	
	private DBAccess dbaccess;
	private Runs2DB runs2db;
	private HazardDataset2DB hd2db;
	private CybershakeSiteInfo2DB sites2db;
	
	public HazardCurve2DB(DBAccess dbaccess){
		this.dbaccess = dbaccess;
		runs2db = new Runs2DB(dbaccess);
		hd2db = new HazardDataset2DB(dbaccess);
		sites2db = new CybershakeSiteInfo2DB(dbaccess);
	}
	
	public ArrayList<Integer> getAllHazardCurveIDs(int erfID, int rupVarScenarioID, int sgtVarID,
			int velModelID, int imTypeID) {
		int probModelID = hd2db.getDefaultProbModelID(erfID);
		int timeSpanID = hd2db.getDefaultTimeSpanID(erfID);
		Date timeSpanStart = null; // TODO this might need to be changed
		int datasetID = hd2db.getDatasetID(erfID, rupVarScenarioID, sgtVarID, velModelID,
					probModelID, timeSpanID, timeSpanStart, Double.NaN, Double.NaN, -1);
		
		if (datasetID < 0)
			throw new RuntimeException("Hazard Dataset ID could not be loaded from the DB!");
		
		return this.getAllHazardCurveIDsForDataset(datasetID, imTypeID);
	}
	
	public ArrayList<CybershakeHazardCurveRecord> getHazardCurveRecordsForSite(int siteID) {
		ArrayList<Integer> runs = runs2db.getRunIDs(siteID);
		String runWhere = Runs2DB.getRunsWhereStatement(runs);
		
		if (runWhere == null)
			return new ArrayList<CybershakeHazardCurveRecord>();
		
		return getAllHazardCurveRecords(runWhere);
	}
	
	public ArrayList<CybershakeHazardCurveRecord> getHazardCurveRecordsForRun(int runID) {
		String runWhere = "Run_ID=" + runID;
		
		return getAllHazardCurveRecords(runWhere);
	}
	
	public ArrayList<CybershakeHazardCurveRecord> getHazardCurveRecordsForDataset(int datasetID) {
		String runWhere = "Hazard_Dataset_ID=" + datasetID;
		
		return getAllHazardCurveRecords(runWhere);
	}
	
	public ArrayList<CybershakeHazardCurveRecord> getAllHazardCurveRecords() {
		return getAllHazardCurveRecords(null);
	}
	
	private ArrayList<CybershakeHazardCurveRecord> getAllHazardCurveRecords(String whereClause) {
		
		String sql = "SELECT * FROM " + TABLE_NAME;
		
		if (whereClause != null && whereClause.length() > 0) {
			sql += " WHERE " + whereClause;
		}
		
		System.out.println(sql);
		
		ArrayList<CybershakeHazardCurveRecord> curves = new ArrayList<CybershakeHazardCurveRecord>();
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
			return null;
		}
		
		try {
			boolean valid = rs.next();
			while (valid) {
				int curveID = rs.getInt("Hazard_Curve_ID");
				int runID = rs.getInt("Run_ID");
				int imTypeID = rs.getInt("IM_Type_ID");
				Date date = rs.getDate("Curve_Date");
				String datasetStr = rs.getString("Hazard_Dataset_ID");
				int dataset;
				if (datasetStr == null || datasetStr.length() == 0 || datasetStr.toLowerCase().contains("null"))
					dataset = -1;
				else
					dataset = Integer.parseInt(datasetStr);
				
				CybershakeHazardCurveRecord record =
					new CybershakeHazardCurveRecord(curveID, runID, imTypeID, date, dataset);
				curves.add(record);
				
				valid = rs.next();
			}
			rs.close();
			
			return curves;
		} catch (SQLException e) {
//			e.printStackTrace();
			return null;
		}
	}
	
	private ArrayList<Integer> getAllHazardCurveIDs(String whereClause) {
		String sql = "SELECT Hazard_Curve_ID FROM " + TABLE_NAME + " " + whereClause;
		sql += " ORDER BY Curve_Date desc";
		
//		System.out.println(sql);
		
		ArrayList<Integer> ids = new ArrayList<Integer>();
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
			return null;
		}
		
		try {
			boolean valid = rs.next();
			if (!valid)
				return null;
			while (!rs.isAfterLast()) {
				int id = rs.getInt(1);
				boolean skip = false;
				for (int oldID : ids) {
					if (oldID == id) {
						// this means that it's a duplicate and the newest one is already in there
						skip = true;
						break;
					}
				}
				if (!skip)
					ids.add(id);
				rs.next();
			}
			rs.close();
			
			return ids;
		} catch (SQLException e) {
//			e.printStackTrace();
			return null;
		}
	}
	
	public ArrayList<Integer> getAllHazardCurveIDsForDataset(int datasetID, int imTypeID) {
		String whereClause = "WHERE Hazard_Dataset_ID=" + datasetID;
		if (imTypeID >= 0)
			whereClause += " AND IM_Type_ID=" + imTypeID; 
		return getAllHazardCurveIDs(whereClause);
	}
	
	public ArrayList<Integer> getAllHazardCurveIDsForRun(int runID, int datasetID, int imTypeID) {
		String whereClause = "WHERE Run_ID=" + runID;
		if (datasetID >= 0)
			whereClause += " AND Hazard_Dataset_ID="+datasetID;
		if (imTypeID >= 0)
			whereClause += " AND IM_Type_ID=" + imTypeID; 
		return getAllHazardCurveIDs(whereClause);
	}
	
	public ArrayList<Integer> getAllHazardCurveIDsForSite(int siteID, int erfID, int rupVarScenarioID, int sgtVarID, int velModelID) {
		ArrayList<Integer> ids = new ArrayList<Integer>();
		ArrayList<Integer> runIDs = runs2db.getRunIDs(siteID, erfID, sgtVarID, rupVarScenarioID, velModelID, null, null, null, null);
		
		String whereClause = Runs2DB.getRunsWhereStatement(runIDs);
		if (whereClause == null || whereClause.length() != 0)
			return ids;
		
		String sql = "SELECT Hazard_Curve_ID FROM " + TABLE_NAME + " WHERE " + whereClause + " ORDER BY Curve_Date desc";
		
//		System.out.println(sql);
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}
		
		try {
			rs.next();
			while (!rs.isAfterLast()) {
				int id = rs.getInt("Hazard_Curve_ID");
				boolean skip = false;
				for (int oldID : ids) {
					if (oldID == id) {
						// this means that it's a duplicate and the newest one is already in there
						skip = true;
						break;
					}
				}
				if (!skip)
					ids.add(id);
				rs.next();
			}
			rs.close();
			
			return ids;
		} catch (SQLException e) {
//			e.printStackTrace();
			return null;
		}
	}
	
	public int getNumHazardCurvePoints(int curveID) {
		String sql = "SELECT count(*) FROM Hazard_Curve_Points WHERE Hazard_Curve_ID=" + curveID;
		
		System.out.println(sql);
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		
		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			int id = rs.getInt(1);
			rs.close();
			
			return id;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
	}
	
	public int getHazardCurveID(int runID, int imTypeID) {
		return getHazardCurveID(runID, -1, imTypeID);
	}
	
	public int getHazardCurveID(int runID, int datasetID, int imTypeID) {
		if (datasetID < 0)
			datasetID = hd2db.getDefaultDatasetID(runs2db.getRun(runID));
//		System.out.println("DEFAULT DATASET: "+datasetID);
		String dIDClause;
		if (datasetID < 0)
			dIDClause = "Hazard_Dataset_ID IS NULL";
		else
			dIDClause = "Hazard_Dataset_ID="+datasetID;
		String whereClause = "WHERE Run_ID=" + runID + " AND IM_Type_ID=" + imTypeID
				+ " AND "+dIDClause;
		
//		System.out.println(sql);
		
		return getHazardCurveID(whereClause);
	}
	
	private int getHazardCurveID(String whereClause) {
		String sql = "SELECT Hazard_Curve_ID FROM " + TABLE_NAME + " " + whereClause;
		sql += " ORDER BY Curve_Date desc";
		
//		System.out.println(sql);
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		
		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			int id = rs.getInt("Hazard_Curve_ID");
			rs.close();
			
			return id;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
	}
	
	public int getHazardCurveID(int siteID, int erfID, int rupVarScenarioID, int sgtVarID, int velModelID, int imTypeID) {
		
		ArrayList<Integer> runIDs = runs2db.getRunIDs(siteID, erfID, sgtVarID, rupVarScenarioID, velModelID, null, null, null, null);
		
		String whereClause = Runs2DB.getRunsWhereStatement(runIDs);
		if (whereClause == null || whereClause.length() == 0)
			return -1;
		
		String sql = "SELECT Hazard_Curve_ID FROM " + TABLE_NAME + " WHERE " + whereClause + " AND IM_Type_ID=" + imTypeID
					+ " ORDER BY Curve_Date desc";
		
//		System.out.println(sql);
		
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		
		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			int id = rs.getInt("Hazard_Curve_ID");
			rs.close();
			
			return id;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
	}
	
	public int getSiteIDFromCurveID(int hcID) {
		String sql = "SELECT Run_ID FROM " + TABLE_NAME + " WHERE Hazard_Curve_ID=" + hcID;

//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
//			TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		int id = -1;
		
		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			id = rs.getInt(1);
			rs.close();
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
		if (id >= 0) {
			return runs2db.getSiteID(id);
		}
		return -1;
	}
	
	public Date getDateForCurve(int hcID) {
		String sql = "SELECT Curve_Date FROM " + TABLE_NAME + " WHERE Hazard_Curve_ID=" + hcID;

//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
//			TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}

		try {
			rs.next();
			if (rs.isAfterLast())
				return null;
			Date date;
			try {
				date = rs.getDate("Curve_Date");
			} catch (Exception e) {
				// fall back to String parse
				String dateStr = rs.getString("Curve_Date");
				date = curveDF.parse(dateStr);
			}
			rs.close();

			return date;
		} catch (SQLException | ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	private static final SimpleDateFormat curveDF = new SimpleDateFormat("yyyy-MM-dd");
	
	public DiscretizedFunc getHazardCurve(int id) {
		DiscretizedFunc hazardFunc = null;
		
		String sql = "SELECT X_Value, Y_Value FROM Hazard_Curve_Points WHERE Hazard_Curve_ID=" + id + 
						" ORDER BY X_Value";
//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
			return null;
		}

		try {
			while (rs.next()) {
				if (hazardFunc == null)
					hazardFunc = new ArbitrarilyDiscretizedFunc();
				double x = rs.getDouble(1);
				double y = rs.getDouble(2);
				hazardFunc.set(x, y);
			}
			rs.close();
			return hazardFunc;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public int insertHazardCurve(CybershakeRun run, int imTypeID, DiscretizedFunc hazardFunc) {
		int datasetID = hd2db.getDefaultDatasetID(run);
		if (datasetID < 0)
			throw new RuntimeException("No dataset ID exists for run: "+run+"\nCurve cannot be inserted.");
		return insertHazardCurve(run.getRunID(), imTypeID, hazardFunc, datasetID);
	}
	
	public int insertHazardCurve(int runID, int imTypeID, DiscretizedFunc hazardFunc, int datasetID) {
		int id = this.insertHazardCurveID(runID, imTypeID, datasetID);
		if (datasetID >= 0) {
			int checkDataset = getDatasetIDForCurve(id);
			Preconditions.checkState(checkDataset == datasetID,
					"Got back a curve with a different dataset ID! "+datasetID+" != "+checkDataset);
		}
		this.insertHazardCurvePoints(id, hazardFunc);
		return id;
	}
	
	public boolean deleteHazardCurve(int curveID) {
		int ptRows = deleteHazardCurvePoints(curveID);
		int idRows = deleteHazardCurveID(curveID);
		
		return ptRows > 0 || idRows > 0;
	}
	
	public boolean deleteCurvesForDatasetID(int datasetID) {
		ArrayList<CybershakeHazardCurveRecord> records = getHazardCurveRecordsForDataset(datasetID);
		
		System.out.println("deleting "+records.size()+" curves!");
		
		boolean didSomething = false;
		
		for (CybershakeHazardCurveRecord record : records) {
			didSomething = deleteHazardCurve(record.getCurveID()) || didSomething;
		}
		
		return didSomething;
	}
	
	public boolean deleteCurvesForDatasetID(int datasetID, int siteTypeID) {
		List<CybershakeHazardCurveRecord> records = getHazardCurveRecordsForDataset(datasetID);
		List<CybershakeRun> runs = runs2db.getRuns();
		List<CybershakeSite> sites = sites2db.getAllSitesFromDB();
		
		System.out.println("deleting "+records.size()+" curves!");
		
		boolean didSomething = false;
		
		for (CybershakeHazardCurveRecord record : records) {
			int myTypeID = getSiteTypeForCurve(record, runs, sites);
			if (myTypeID == siteTypeID)
				didSomething = deleteHazardCurve(record.getCurveID()) || didSomething;
		}
		
		return didSomething;
	}
	
	public int getSiteTypeForCurve(CybershakeHazardCurveRecord record) {
		return getSiteTypeForCurve(record, runs2db.getRuns(), sites2db.getAllSitesFromDB());
	}
	
	public int getSiteTypeForCurve(CybershakeHazardCurveRecord record,
			List<CybershakeRun> runs, List<CybershakeSite> sites) {
		for (CybershakeRun run : runs) {
			if (run.getRunID() == record.getRunID()) {
				for (CybershakeSite site : sites) {
					if (site.id == run.getSiteID())
						return site.type_id;
				}
			}
		}
		return -1;
	}
	
	public int deleteHazardCurveID(int curveID) {
		String sql = "DELETE FROM " + TABLE_NAME + " WHERE Hazard_Curve_ID=" + curveID;
		System.out.println(sql);
		try {
			return dbaccess.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
//			TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	public int deleteHazardCurvePoints(int curveID) {
		String sql = "DELETE FROM Hazard_Curve_Points WHERE Hazard_Curve_ID=" + curveID;
		System.out.println(sql);
		try {
			return dbaccess.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
//			TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	public void replaceHazardCurve(int curveID, DiscretizedFunc hazardFunc) {
		this.deleteHazardCurvePoints(curveID);
		
		this.insertHazardCurvePoints(curveID, hazardFunc);
		
		// update the curve date
		Date now = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String date = format.format(now);
		
		String sql = "UPDATE " + TABLE_NAME + " SET Curve_Date='" + date + "' WHERE Hazard_Curve_ID="+curveID;
		System.out.println(sql);
		try {
			dbaccess.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
//			TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}
	
	private int insertHazardCurveID(int runID, int imTypeID, int datasetID) {
		
		Date now = new Date();
		
		String date = DBAccess.SQL_DATE_FORMAT.format(now);
		
		String dataset = "(null)";
		if (datasetID > 0)
			dataset = datasetID+"";
		
		String sql = "INSERT into " + TABLE_NAME + 
		"(Run_ID,IM_Type_ID,Curve_Date,Hazard_Dataset_ID)"+
		"VALUES("+runID+","+imTypeID+",'"+date+"',"+dataset+")";
		System.out.println(sql);
		try {
			dbaccess.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
			ExceptionUtils.throwAsRuntimeException(e);
		}
		
		return this.getHazardCurveID(runID, datasetID, imTypeID);
	}
	
	public void insertHazardCurvePoints(int id, DiscretizedFunc hazardFunc) {
		String sql = "INSERT into Hazard_Curve_Points "+ 
				"(Hazard_Curve_ID,X_Value,Y_Value) "+
				"VALUES";
		int numPoints = hazardFunc.size();
		for (int i=0; i<numPoints; i++) {
			Point2D pt = hazardFunc.get(i);
			sql += " (" + id + "," + pt.getX() + "," + pt.getY() + ")";
			if (i < numPoints -1)
				sql += ",";
		}
		System.out.println(sql);
		try {
			dbaccess.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
			ExceptionUtils.throwAsRuntimeException(e);
		}
	}
	
	public int getIMTypeIDForCurve(int curveID) {
		String sql = "SELECT IM_Type_ID FROM " + TABLE_NAME + " WHERE Hazard_Curve_ID=" + curveID;

//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
//			TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}

		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			int id = rs.getInt("IM_Type_ID");
			rs.close();

			return id;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
	}
	
	public int getDatasetIDForCurve(int curveID) {
		String sql = "SELECT Hazard_Dataset_ID FROM " + TABLE_NAME + " WHERE Hazard_Curve_ID=" + curveID;

//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
//			TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}

		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			int id = rs.getInt("Hazard_Dataset_ID");
			rs.close();

			return id;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
	}
	
	public int getRunIDForCurve(int curveID) {
		String sql = "SELECT Run_ID FROM " + TABLE_NAME + " WHERE Hazard_Curve_ID=" + curveID;

//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
//			TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}

		try {
			rs.next();
			if (rs.isAfterLast())
				return -1;
			int id = rs.getInt("Run_ID");
			rs.close();

			return id;
		} catch (SQLException e) {
//			e.printStackTrace();
			return -1;
		}
	}
	
	public CybershakeIM getIMForCurve(int curveID) {
		int imTypeID = getIMTypeIDForCurve(curveID);
		
		return this.getIMFromID(imTypeID);
	}
	
	public CybershakeIM getIMFromID(int imTypeID) {
		String sql = "SELECT * FROM IM_Types WHERE IM_Type_ID=" + imTypeID;

//		System.out.println(sql);

		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql);
		} catch (SQLException e1) {
//			TODO Auto-generated catch block
			e1.printStackTrace();
			return null;
		}

		try {
			rs.next();
			if (rs.isAfterLast())
				return null;
			CybershakeIM im = CybershakeIM.fromResultSet(rs);
			
			rs.close();

			return im;
		} catch (SQLException e) {
//			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * @return the supported SA Period as list of strings.
	 */
	public ArrayList<CybershakeIM>  getSupportedIMs(int runID) {
		long startTime = System.currentTimeMillis();
		String sql = "SELECT I.IM_Type_ID,I.IM_Type_Measure,I.IM_Type_Value,I.Units,I.IM_Type_Component"
				+ " from IM_Types I JOIN (";
		sql += "SELECT distinct IM_Type_ID from " + TABLE_NAME + " WHERE Run_ID=" + runID;
		sql += ") A ON A.IM_Type_ID=I.IM_Type_ID ORDER BY I.IM_Type_ID";
		
//		System.out.println(sql);
		
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
				CybershakeIM im = CybershakeIM.fromResultSet(rs);
				ims.add(im);
//				System.out.println(im);
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
//			e.printStackTrace();
		}
		long duration = System.currentTimeMillis() - startTime;
//		System.out.println("Total SA Period Select Time: " + ((double)duration / 1000) + " sec");
		return ims;
	}
	
	/**
	 * @return the supported SA Period as list of strings.
	 */
	public ArrayList<CybershakeIM>  getSupportedIMs(int siteID, int erfID, int rupVarID, int sgtVariation, int velModelID) {
		ArrayList<Integer> runIDs = runs2db.getRunIDs(siteID, erfID, sgtVariation, rupVarID, velModelID, null, null, null, null);
		ArrayList<CybershakeIM> ims = new ArrayList<CybershakeIM>();
		
		for (int runID : runIDs) {
			ims.addAll(getSupportedIMs(runID));
		}
		return ims;
	}
	
	public static void main(String args[]) {
		HazardCurve2DB curve2db;
		try {
			curve2db = new HazardCurve2DB(Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(true, true));
//			curve2db.deleteCurvesForDatasetID(51); // cs oef parkfield, 1 week
//			curve2db.deleteCurvesForDatasetID(53); // cs oef parkfield, 1 day
//			curve2db.deleteCurvesForDatasetID(49); // cs oef bombay, 1 week
//			curve2db.deleteCurvesForDatasetID(54); // cs oef bombay, 1 day
//			curve2db.deleteCurvesForDatasetID(50); // cs oef UCERF2 mapped, 1 week
			for (int curveID : curve2db.getAllHazardCurveIDsForRun(7214, -1, -1)) {
				System.out.println("deleting curve "+curveID);
				curve2db.deleteHazardCurve(curveID);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(0);
		
		HazardCurve2DB hc = new HazardCurve2DB(Cybershake_OpenSHA_DBApplication.getDB());
		
		System.out.println("ID: " + hc.getHazardCurveID(2, 34, 3, 5, 1, 21));
		System.out.println("ID: " + hc.getHazardCurveID(26, 34, 3, 5, 1, 21));
		
		DiscretizedFunc hazardFunc = hc.getHazardCurve(1);
		
		System.out.println(hazardFunc.toString());
		
		for (int id : hc.getAllHazardCurveIDs(34, 3, 5, 1, 21)) {
			System.out.println("Haz Curve For: " + id);
		}
		
		for (CybershakeIM im : hc.getSupportedIMs(33, 34, 3, 5, 1)) {
			System.out.println(im);
		}
	}
	
}
