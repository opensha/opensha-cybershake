package org.opensha.sha.cybershake.db;

public class CybershakeSiteType {
	
	private int id;
	private String name;
	private String shortName;
	
	public CybershakeSiteType(int id, String shortName, String name) {
		this.id = id;
		this.shortName = shortName;
		this.name = name;
	}

	public int getID() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getShortName() {
		return shortName;
	}
	
	public String toString() {
		return "ID: " + id + ", Short Name: " + shortName + ", Long Name: " + name;
	}

}
