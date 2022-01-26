package org.opensha.sha.cybershake.db;

import java.util.Date;

public class CybershakeHazardCurveRecord implements Comparable<CybershakeHazardCurveRecord> {
	
	private int curveID;
	private int runID;
	private int imTypeID;
	private Date date;
	private int datasetID;
	
	public CybershakeHazardCurveRecord(int curveID, int runID, int imTypeID, Date date, int datasetID) {
		this.curveID = curveID;
		this.runID = runID;
		this.imTypeID = imTypeID;
		this.date = date;
		this.datasetID = datasetID;
	}

	public int getCurveID() {
		return curveID;
	}

	public int getRunID() {
		return runID;
	}

	public int getImTypeID() {
		return imTypeID;
	}
	
	public int getDataSetID() {
		return datasetID;
	}

	public Date getDate() {
		return date;
	}

	public int compareTo(CybershakeHazardCurveRecord o) {
		if (o.getImTypeID() < this.getImTypeID())
			return -1;
		else if (o.getImTypeID() > this.getImTypeID())
			return 1;
		return 0;
	}

	public String toString() {
		return "curveID: " + curveID + ", runID: " + runID + ", imTypeID: " + imTypeID
				+ ", date: " + date + ", datasetID: " + datasetID;
	}
}
