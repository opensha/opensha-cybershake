package org.opensha.sha.cybershake.db;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.opensha.commons.exceptions.ParameterException;
import org.opensha.commons.param.Parameter;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncLevelParam;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncTypeParam;

public class AttenRels2DB {
	
	private static final String ATTEN_REL_TABLE_NAME = "Atten_Rels";
	private static final String ATTEN_REL_METADATA_TABLE_NAME = "Atten_Rel_Metadata";
	
	private DBAccess db;
	
	public AttenRels2DB(DBAccess db) {
		this.db = db;
	}
	
	public int insertAttenRel(ScalarIMR imr) throws SQLException {
		// first make sure we haven't tried inserting it before
		int attenRelID = getAttenRelIDWithZeroParams(imr.getShortName());
		if (attenRelID >= 0) {
			System.out.println("Found partial insert for "+imr.getShortName()+": "+attenRelID);
		} else {
			System.out.println("Inserting new AR ID");
			String sql = "INSERT INTO "+ATTEN_REL_TABLE_NAME+" (AR_Name, AR_Short_Name)"
			+"VALUES ('"+imr.getName()+"', '"+imr.getShortName()+"')";
	
			int ret = db.insertUpdateOrDeleteData(sql);
	
			if (ret < 1)
				throw new SQLException("No rows modified on insert!");
			
			attenRelID = getAttenRelIDWithZeroParams(imr.getShortName());
		}
		
		// now insert the params
		insertAttenRelMetadata(attenRelID, imr);
		
		return attenRelID;
	}
	
	private int getAttenRelIDWithZeroParams(String shortName) throws SQLException {
		ArrayList<Integer> candidateIDs = getAttenRelIDs(shortName);
		for (int candidateID : candidateIDs) {
			if (getParamsForIMR(candidateID).size() == 0) {
				// we found a matching ID with zero params, insert with that;
				return candidateID;
			}
		}
		return -1;
	}
	
	public int getAttenRelID(ScalarIMR imr) throws SQLException {
		ArrayList<Integer> ids = getAttenRelIDs(imr.getShortName());
		
		for (int attenRelID : ids) {
//			System.out.println("Potential match: "+attenRelID);
			ArrayList<String[]> params = getParamsForIMR(attenRelID);
			boolean match = true;
			for (String[] param : params) {
				String pname = param[0];
				String pval = param[1];
				
				try {
					Parameter<?> imrParam = imr.getParameter(pname);
					String imrParamVal = imrParam.getValue().toString();
					if (imrParamVal.length() > 100)
						imrParamVal = imrParamVal.substring(0, 100);
					if (!imrParamVal.equals(pval)) {
						// there's a chance that it's truncated
						if (imrParamVal.length()>50) {
							if (pval.length() == 50 && imrParamVal.startsWith(pval))
								// it was truncated, but matches
								continue;
						}
//						System.out.println("Param '"+pname+"' doesn't match: "+imrParamVal+" != "+pval);
						match = false;
						break;
					}
				} catch (ParameterException e) {
					continue;
				}
			}
			if (match) {
//				System.out.println("It's a match!");
				return attenRelID;
			}
		}
		return -1;
	}
	
	private ArrayList<String[]> getParamsForIMR(int attenRelID) throws SQLException {
		ArrayList<String[]> params = new ArrayList<String[]>();
		
		String sql = "SELECT * FROM "+ATTEN_REL_METADATA_TABLE_NAME
					+" WHERE AR_ID="+attenRelID;
		
		ResultSet rs = db.selectData(sql);
		boolean success = rs.first();
		if (!success)
			return params;
		
		while (!rs.isAfterLast()) {
			String pname = rs.getString("AR_Attr_Name");
			String pval = rs.getString("AR_Attr_Value");
			String[] param = {pname, pval};
			params.add(param);
			
			rs.next();
		}
		
		return params;
	}
	
	public ArrayList<Integer> getAttenRelIDs(String shortName) throws SQLException {
		ArrayList<Integer> ids = new ArrayList<Integer>();
		
		String sql = "SELECT AR_ID FROM "+ATTEN_REL_TABLE_NAME;
		if (shortName != null && shortName.length() > 0)
			sql += " WHERE AR_Short_Name='"+shortName+"'";
		sql += " ORDER BY AR_ID desc";
		
		ResultSet rs = db.selectData(sql);
		rs.first();
		boolean success = rs.first();
		if (!success)
			return ids;
		
		while (!rs.isAfterLast()) {
			int id = rs.getInt(1);
			ids.add(id);
			rs.next();
		}
		
		return ids;
	}
	
	private void insertAttenRelMetadata(int attenRelID, ScalarIMR imr)
	throws SQLException {
		String sql = "INSERT INTO "+ATTEN_REL_METADATA_TABLE_NAME+" VALUES ";
		
		boolean first = true;
		for (Parameter<?> param : imr.getOtherParams()) {
			if (first) {
				first = false;
				sql += "\n";
			} else {
				sql += ",\n";
			}
			sql += "(";
			String pname = param.getName();
			Object pvalObj = param.getValue();
			if (pvalObj == null)
				throw new IllegalStateException("param '"+pname+"' is null...can't insert!");
			String pvalStr = pvalObj.toString().replaceAll("'", "");
			if (pvalStr.length() > 100)
				pvalStr = pvalStr.substring(0, 100);
			String type = param.getType();
			String units = param.getUnits();
			
			sql += attenRelID+", '"+pname+"', '"+pvalStr+"', ";
			if (type == null || type.length() == 0)
				sql += "null";
			else {
				type = type.replaceAll("Parameter", "");
				sql += "'"+type+"'";
			}
			sql += ", ";
			if (units == null || units.length() == 0)
				sql += "null";
			else
				sql += "'"+units+"'";
			sql += ")";
		}
		System.out.println(sql);
		db.insertUpdateOrDeleteData(sql);
	}
	
	public static void main(String[] args) throws IOException {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(
				true, false, Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
//		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		try {
			AttenRels2DB atten2db = new AttenRels2DB(db);
			ScalarIMR imr = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
			imr.setParamDefaults();
			imr.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_1SIDED);
			imr.getParameter(SigmaTruncLevelParam.NAME).setValue(3d);
//			atten2db.insertAttenRelMetadata(0, imr);
			atten2db.insertAttenRel(imr);
//			int id = atten2db.getAttenRelID(imr);
//			System.out.println("ID for "+imr.getShortName()+": "+id);
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			db.destroy();
		}
		System.exit(0);
	}

}