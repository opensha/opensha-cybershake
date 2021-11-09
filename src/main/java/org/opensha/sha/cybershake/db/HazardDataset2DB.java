package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;

public class HazardDataset2DB {
	
	private DBAccess db;
	
	public HazardDataset2DB(DBAccess db) {
		Preconditions.checkNotNull(db, "Passed in DBAccess is null!");
		this.db = db;
	}
	
	public int getDefaultProbModelID(int erfID) {
		return getERF_Field("Default_Prob_Model_ID", erfID);
	}
	
	public int getDefaultTimeSpanID(int erfID) {
		return getERF_Field("Default_Time_Span_ID", erfID);
	}
	
	private int getERF_Field(String field, int erfID) {
		String sql = "SELECT "+field;
		sql += " FROM ERF_IDs WHERE ERF_ID="+erfID;
		
//		System.out.println(sql);
		
		return DB_Utils.getSingleInt(db, sql);
	}
	
	public int getDefaultDatasetID(CybershakeRun run) {
		int probModelID = getDefaultProbModelID(run.getERFID());
		int timeSpanID = getDefaultTimeSpanID(run.getERFID());
		int backSeisAttenRelID = -1;
		
//		System.out.println("PM: "+probModelID+" TS: "+timeSpanID);
		
		return getDatasetID(run.getERFID(), run.getRupVarScenID(), run.getSgtVarID(), run.getVelModelID(),
				probModelID, timeSpanID, null, run.getMaxFreq(), run.getLowFreqCutoff(), backSeisAttenRelID);
	}
	
	public int getDatasetID(int erfID, int rvScenID, int sgtVarID,
					int velModelID, int probModelID, int timeSpanID, Date timeSpanStart,
					double maxFreq, double lowFreqCutoff, int backSeisAttenRelID) {
		String dateStr;
		if (timeSpanStart != null) {
			dateStr = "='"+DBAccess.SQL_DATE_FORMAT.format(timeSpanStart)+"'";
		} else {
			dateStr = " IS NULL";
		}
		String maxFreqStr;
		if (maxFreq > 0) {
			maxFreqStr = "='"+maxFreq+"'";
		} else {
			maxFreqStr = " IS NULL";
		}
		String lowFreqStr;
		if (lowFreqCutoff > 0) {
			lowFreqStr = "='"+lowFreqCutoff+"'";
		} else {
			lowFreqStr = " IS NULL";
		}
		
//		System.out.println("DATE: " + timeSpanStart + " DATE_STR: " + dateStr);
		
		String sql = "SELECT Hazard_Dataset_ID";
		sql += " FROM Hazard_Datasets";
		sql += " WHERE ERF_ID="+erfID;
		sql += " AND Rup_Var_Scenario_ID="+rvScenID;
		sql += " AND SGT_Variation_ID="+sgtVarID;
		sql += " AND Velocity_Model_ID="+velModelID;
		sql += " AND Prob_Model_ID="+probModelID;
		sql += " AND Time_Span_ID="+timeSpanID;
		sql += " AND Time_Span_Start_Date"+dateStr;
		if (Doubles.isFinite(maxFreq))
			sql += " AND Max_Frequency"+maxFreqStr;
		if (Doubles.isFinite(lowFreqCutoff))
			sql += " AND Low_Frequency_Cutoff"+lowFreqStr;
		if (backSeisAttenRelID > 0)
			sql += " AND Background_Seis_AR_ID="+backSeisAttenRelID;
		else
			sql += " AND Background_Seis_AR_ID IS NULL";
		
//		System.out.println(sql);
		
		return DB_Utils.getSingleInt(db, sql);
	}
	
	public int addNewDataset(int erfID, int rvScenID, int sgtVarID, int velModelID, int probModelID,
			int timeSpanID, Date timeSpanStart, double maxFreq, double lowCutoffFreq, int backSeisAttenRelID) {
		String dateField;
		String dateStr;
		if (timeSpanStart != null) {
			dateField = ",Time_Span_Start_Date";
			dateStr = ",'"+DBAccess.SQL_DATE_FORMAT.format(timeSpanStart)+"'";
		} else {
			dateField = "";
			dateStr = "";
		}
		String maxFreqField;
		String maxFreqStr;
		if (maxFreq > 0) {
			maxFreqField = ",Max_Frequency";
			maxFreqStr = ",'"+maxFreq+"'";
		} else {
			maxFreqField = "";
			maxFreqStr = "";
		}
		String lowFreqField;
		String lowFreqStr;
		if (maxFreq > 0) {
			lowFreqField = ",Low_Frequency_Cutoff";
			lowFreqStr = ",'"+lowCutoffFreq+"'";
		} else {
			lowFreqField = "";
			lowFreqStr = "";
		}
		String bgSeisField;
		String bgSeisStr;
		if (maxFreq > 0) {
			bgSeisField = ",Background_Seis_AR_ID";
			bgSeisStr = ",'"+backSeisAttenRelID+"'";
		} else {
			bgSeisField = "";
			bgSeisStr = "";
		}
		String sql = "INSERT INTO Hazard_Datasets" + 
				"(ERF_ID,Rup_Var_Scenario_ID,SGT_Variation_ID,Velocity_Model_ID," +
				"Prob_Model_ID,Time_Span_ID"+dateField+maxFreqField+lowFreqField+bgSeisField+")"+
				"VALUES("+erfID+","+rvScenID+","+sgtVarID+","+velModelID
				+","+probModelID+","+timeSpanID+dateStr+maxFreqStr+lowFreqStr+bgSeisStr+")";
		
		try {
			db.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		
		return getDatasetID(erfID, rvScenID, sgtVarID, velModelID, probModelID, timeSpanID,
				timeSpanStart, maxFreq, lowCutoffFreq, backSeisAttenRelID);
	}
	
	public int getProbModelID(int datasetID) {
		String sql = "SELECT Prob_Model_ID FROM Hazard_Datasets WHERE Hazard_Dataset_ID="+datasetID;
		
		return DB_Utils.getSingleInt(db, sql);
	}
	
	public int getBackSeisAttenRelID(int datasetID) {
		String sql = "SELECT Background_Seis_AR_ID FROM Hazard_Datasets WHERE Hazard_Dataset_ID="+datasetID;
		
		int ret = DB_Utils.getSingleInt(db, sql);
		if (ret == 0)
			// this means null
			return -1;
		return ret;
	}
	
	public CybershakeHazardDataset getDataset(int datasetID) throws SQLException {
		List<CybershakeHazardDataset> datasets = getDatasets("WHERE Hazard_Dataset_ID="+datasetID);
		Preconditions.checkState(datasets != null && datasets.size() == 1, "Dataset %s not found", datasetID);
		CybershakeHazardDataset ret = datasets.get(0);
		Preconditions.checkState(ret.datasetID == datasetID);
		return ret;
	}
	
	public List<CybershakeHazardDataset> getDatasets() throws SQLException {
		return getDatasets("");
	}
	
	private List<CybershakeHazardDataset> getDatasets(String whereClause) throws SQLException {
		String sql = "SELECT * from Hazard_Datasets "+whereClause;
		
		ResultSet rs = db.selectData(sql);
		
		List<CybershakeHazardDataset> ret = new ArrayList<>();
		
//		System.out.println(sql);
		
		while (rs.next()) {
			int datasetID = rs.getInt("Hazard_Dataset_ID");
//			System.out.println(datasetID);
			int erfID = rs.getInt("ERF_ID");
			int rvScenID = rs.getInt("Rup_Var_Scenario_ID");
			int sgtVarID = rs.getInt("SGT_Variation_ID");
			int velModelID = rs.getInt("Velocity_Model_ID");
			int probModelID = rs.getInt("Prob_Model_ID");
			int timeSpanID = rs.getInt("Time_Span_ID");
			Date timeSpanStart = rs.getDate("Time_Span_Start_Date");
			double maxFreq = rs.getDouble("Max_Frequency");
			if (rs.wasNull())
				maxFreq = Double.NaN;
			double lowFreqCutoff = rs.getDouble("Low_Frequency_Cutoff");
			if (rs.wasNull())
				lowFreqCutoff = Double.NaN;
			int backSeisAttenRelID = rs.getInt("Background_Seis_AR_ID");
			
			ret.add(new CybershakeHazardDataset(datasetID, erfID, rvScenID, sgtVarID, velModelID,
					probModelID, timeSpanID, timeSpanStart, maxFreq, lowFreqCutoff, backSeisAttenRelID));
		}
		rs.close();
		
		return ret;
	}
	
	public static void main(String[] args) {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
//		Runs2DB r2db = new Runs2DB(db);
//		CybershakeRun run = r2db.getRun(776);
//		HazardDataset2DB hd2db = new HazardDataset2DB(db);
//		System.out.println("Dataset: " + hd2db.getDefaultDatasetID(run));
		
		db.destroy();
		
		System.exit(0);
	}

}
