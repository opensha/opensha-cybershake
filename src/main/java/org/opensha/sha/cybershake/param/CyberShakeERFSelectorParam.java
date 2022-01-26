package org.opensha.sha.cybershake.param;

import java.util.ArrayList;

import org.opensha.commons.param.constraint.impl.StringConstraint;
import org.opensha.commons.param.impl.StringParameter;
import org.opensha.sha.cybershake.db.CybershakeERF;

public class CyberShakeERFSelectorParam extends StringParameter {
	
	public static final String NAME = "CyberShake Earthquake Rupture Forecast";
	
	private ArrayList<String> erfNames;
	private ArrayList<CybershakeERF> erfs;
	
	public CyberShakeERFSelectorParam(ArrayList<CybershakeERF> erfs) {
		super(NAME);
		this.erfs = erfs;
		erfNames = new ArrayList<String>();
		
		for (CybershakeERF erf : erfs) {
	    	erfNames.add(erf.id + ": " + erf.description);
	    }
		
		this.setConstraint(new StringConstraint(erfNames));
		
		int maxID = 0;
		int maxIndex = -1;
		for (int i=0; i<erfs.size(); i++) {
			CybershakeERF erf = erfs.get(i);
			if (erf.id > maxID) {
				maxID = erf.id;
				maxIndex = i;
			}
		}
		if (maxIndex >= 0) {
			this.setValue(erfNames.get(maxIndex));
		}
	}
	
	public CybershakeERF getSelectedERF() {
		String name = (String)this.getValue();
		for (int i=0; i<erfNames.size(); i++) {
			if (name.equals(erfNames.get(i)))
				return erfs.get(i);
		}
		return null;
	}
}
