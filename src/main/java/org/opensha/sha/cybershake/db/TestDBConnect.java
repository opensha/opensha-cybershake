package org.opensha.sha.cybershake.db;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;



public class TestDBConnect {

	public static void main(String[] args) throws IOException {
		DBAccess dbc = new DBAccess("surface.usc.edu","CyberShake");
		ResultSet rs = null;
		try {
			rs = dbc.selectData("SHOW TABLES");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//dbc.deleteOrUpdateData("Delete from ERF_Metadata");
		//try {
		//	dbc.insertUpdateOrDeleteData("Delete from CyberShake_Site_Ruptures");
		//} catch (SQLException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		//}
		//try {
			//dbc.insertUpdateOrDeleteData("Delete from CyberShake_Site_Regions");
		//} catch (SQLException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		//}
		//dbc.deleteOrUpdateData("Delete from Ruptures");
		//dbc.deleteOrUpdateData("Delete from Points");	
		try {
			dbc.insertUpdateOrDeleteData("Delete from CyberShake_Sites");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//dbc.deleteOrUpdateData("Delete from ERF_IDs");
		try {
			System.out.println(rs.getMetaData().getColumnCount());
			rs.next();
			while (!rs.isAfterLast()) {
				System.out.println(rs.getString(1));
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

