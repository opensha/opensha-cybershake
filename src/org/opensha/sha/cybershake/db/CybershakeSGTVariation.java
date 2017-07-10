package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CybershakeSGTVariation {
	
	private int id;
	private String name;
	private String description;
	
	public CybershakeSGTVariation(int id, String name, String description) {
		this.id = id;
		this.name = name;
		this.description = description;
	}

	public int getID() {
		return id;
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
	
	public static CybershakeSGTVariation fromResultSet(ResultSet rs) throws SQLException {
		int id = rs.getInt("SGT_Variation_ID");
		String name = rs.getString("SGT_Variation_Name");
		String description = rs.getString("SGT_Variation_Description");
		
		return new CybershakeSGTVariation(id, name, description);
	}

}
