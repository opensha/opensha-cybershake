package org.opensha.sha.cybershake.openshaAPIs;

import java.util.List;

import org.opensha.commons.param.event.ParameterChangeEvent;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkSource;

public class CyberShakeWrapper_ERF extends AbstractERF {
	
	public static final String NAME = "CyberShake ERF Wrapper";
	
	private int erfID;
	private AbstractERF erf = null;
	
	public CyberShakeWrapper_ERF(int erfID, AbstractERF erf) {
		this.erfID = erfID;
		this.erf = erf;
		
		this.timeSpan = erf.getTimeSpan();
	}
	
	private AbstractERF getERF() {
		return erf;
	}

	public int getNumSources() {
		return getERF().getNumSources();
	}

	public CyberShakeProbEqkSource getSource(int sourceID) {
		ProbEqkSource source = getERF().getSource(sourceID);
		
		CyberShakeProbEqkSource csSource = new CyberShakeProbEqkSource(source, sourceID, erfID);
		
		return csSource;
	}

	public List<ProbEqkSource> getSourceList() {
		return getERF().getSourceList();
	}

	public String getName() {
		return NAME;
	}

	public void updateForecast() {
		getERF().updateForecast();
	}

	@Override
	public void parameterChange(ParameterChangeEvent event) {
		erf = null;
		super.parameterChange(event);
	}

}
