/*******************************************************************************
 * Copyright 2009 OpenSHA.org in partnership with
 * the Southern California Earthquake Center (SCEC, http://www.scec.org)
 * at the University of Southern California and the UnitedStates Geological
 * Survey (USGS; http://www.usgs.gov)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

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
