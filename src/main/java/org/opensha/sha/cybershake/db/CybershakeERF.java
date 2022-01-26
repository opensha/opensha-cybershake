package org.opensha.sha.cybershake.db;

public class CybershakeERF {
	public int id;
	public String name;
	public String description;
	
	public CybershakeERF(int id, String name, String description) {
		this.id = id;
		this.name = name;
		this.description = description;
	}
	
	public String toString() {
		return "ID: " + id + "\tName: " + name + "\tDesc: " + description;
	}
}
