package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CybershakeRuptureVariation {
	
	private int id;
	private int erfID;
	private String name;
	private String description;
	
	public CybershakeRuptureVariation(int id, int erfID, String name, String description) {
		this.id = id;
		this.erfID = erfID;
		this.name = name;
		this.description = description;
	}

	public int getID() {
		return id;
	}
	
	public int getERFID() {
		return erfID;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public String toString() {
		return id + ". " + name + " (" + description + ")";
	}
	
	public static CybershakeRuptureVariation fromResultSet(ResultSet rs) throws SQLException {
		int id = rs.getInt("Rup_Var_Scenario_ID");
		int erfID = rs.getInt("ERF_ID");
		String name = rs.getString("Rup_Var_Scenario_Name");
		String description = rs.getString("Rup_Var_Scenario_Description");
		
		return new CybershakeRuptureVariation(id, erfID, name, description);
	}

}
