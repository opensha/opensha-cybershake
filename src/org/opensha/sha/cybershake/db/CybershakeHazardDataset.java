package org.opensha.sha.cybershake.db;

import java.util.Date;

public class CybershakeHazardDataset {
	
	public final int datasetID;
	public final int erfID;
	public final int rvScenID;
	public final int sgtVarID;
	public final int velModelID;
	public final int probModelID;
	public final int timeSpanID;
	public final Date timeSpanStart;
	public final double maxFreq;
	public final double lowFreqCutoff;
	public final int backSeisAttenRelID;
	
	public CybershakeHazardDataset(int datasetID, int erfID, int rvScenID, int sgtVarID, int velModelID,
			int probModelID, int timeSpanID, Date timeSpanStart, double maxFreq, double lowFreqCutoff,
			int backSeisAttenRelID) {
		super();
		this.datasetID = datasetID;
		this.erfID = erfID;
		this.rvScenID = rvScenID;
		this.sgtVarID = sgtVarID;
		this.velModelID = velModelID;
		this.probModelID = probModelID;
		this.timeSpanID = timeSpanID;
		this.timeSpanStart = timeSpanStart;
		this.maxFreq = maxFreq;
		this.lowFreqCutoff = lowFreqCutoff;
		this.backSeisAttenRelID = backSeisAttenRelID;
	}

}
