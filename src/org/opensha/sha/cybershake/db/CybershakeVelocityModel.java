package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CybershakeVelocityModel {
	
	public enum Models {
		
		CVM_S4(1, "CVM-SCEC", "4.0"),
		CVM_H_11_2(2, "CVM-H", "11.2.0"),
		HALFSPACE(3, "halfspace", "vp=1500 vs=500 rho=2200"),
		CVM_H_11_9(4, "CVM-H", "11.9"),
		CVM_S4_26(5, "CVM-S4.26", "4.26"),
		CVM_S_1D(6, "SCEC 1D", "13.9.0"),
		CVM_H_11_9_NO_GTL(7, "CVM-H_no_GTL", "11.9"),
		BBP_1D(8, "BBP 1D", "13.9.0"),
		CCA_1D(9, "CCA 1D", "1.0"),
		CCA_06(10, "CCA", "iteration 6");
		
		private CybershakeVelocityModel model;
		private Models(int id, String name, String version) {
			this.model = new CybershakeVelocityModel(id, name, version);
		}
		
		public CybershakeVelocityModel instance() {
			return model;
		}
	}
	
	private int id;
	private String name;
	private String version;
	
	public CybershakeVelocityModel(int id, String name, String version) {
		this.id = id;
		this.name = name;
		this.version = version;
	}

	public int getID() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getVersion() {
		return version;
	}

	@Override
	public String toString() {
		return id + ". " + name + " (" + version + ")";
	}
	
	public static CybershakeVelocityModel fromResultSet(ResultSet rs) throws SQLException {
		int id = rs.getInt("Velocity_Model_ID");
		String name = rs.getString("Velocity_Model_Name");
		String version = rs.getString("Velocity_Model_Version");
		
		return new CybershakeVelocityModel(id, name, version);
	}

}
