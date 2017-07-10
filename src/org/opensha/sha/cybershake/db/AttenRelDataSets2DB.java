package org.opensha.sha.cybershake.db;

import java.sql.SQLException;
import java.util.Date;

import org.opensha.commons.util.ExceptionUtils;

public class AttenRelDataSets2DB {
	
	private static final String TABLE_NAME = "AR_Hazard_Datasets";
	
	private DBAccess db;
	
	public AttenRelDataSets2DB(DBAccess db) {
		this.db = db;
	}
	
	public int getDataSetID(int attenRelID, int erfID, int velModelID, int probModelID,
			int timeSpanID, Date date) {
		String sql = "SELECT AR_Hazard_Dataset_ID FROM "+TABLE_NAME+" WHERE"
						+" AR_ID="+attenRelID
						+" AND ERF_ID="+erfID
						+" AND Prob_Model_ID="+probModelID
						+" AND Time_Span_ID="+timeSpanID;
		
		if (velModelID >= 0)
			sql += " AND Velocity_Model_ID="+velModelID;
		else
			sql += " AND Velocity_Model_ID IS NULL";
		
		if (date == null)
			sql += " AND Time_Span_Start_Date IS NULL";
		else
			sql += " AND Time_Span_Start_Date='"+DBAccess.SQL_DATE_FORMAT.format(date)+"'";
		
		return DB_Utils.getSingleInt(db, sql);
	}
	
	public int addDataSetID(int attenRelID, int erfID, int velModelID, int probModelID, int timeSpanID, Date date,
			double minLat, double maxLat, double minLon, double maxLon, double gridSpacing) {
		String dateStr;
		if (date == null)
			dateStr = "NULL";
		else
			dateStr = DBAccess.SQL_DATE_FORMAT.format(date);
		String velStr;
		if (velModelID >= 0)
			velStr = velModelID+"";
		else
			velStr = "NULL";
		String sql = "INSERT INTO "+TABLE_NAME+" (AR_ID,ERF_ID,Velocity_Model_ID,Prob_Model_ID,Time_Span_ID,Time_Span_Start_Date"
				+ ",Min_Lat,Max_Lat,Min_Lon,Max_Lon,Grid_Spacing)"
				+ " VALUES ("+attenRelID+","+erfID+","+velStr+","+probModelID+","+timeSpanID+","+dateStr
				+ ","+(float)minLat+","+(float)maxLat+","+(float)minLon+","+(float)maxLon+","+(float)gridSpacing+")";
		
		try {
			db.insertUpdateOrDeleteData(sql);
		} catch (SQLException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		return getDataSetID(attenRelID, erfID, velModelID, probModelID, timeSpanID, date);
	}

}
