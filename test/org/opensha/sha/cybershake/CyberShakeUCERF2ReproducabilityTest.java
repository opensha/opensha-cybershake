package org.opensha.sha.cybershake;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensha.commons.data.CSVFile;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkSource;

import com.google.common.collect.Maps;

public class CyberShakeUCERF2ReproducabilityTest {
	
	private static Map<Integer, SourceInfo> sourceInfoMap;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		sourceInfoMap = Maps.newHashMap();
		CSVFile<String> csv = CSVFile.readStream(
				CyberShakeUCERF2ReproducabilityTest.class.getResourceAsStream("ucerf2_cybershake_sources.csv"), true);
		for (int i=1; i<csv.getNumRows(); i++) {
			List<String> line = csv.getLine(i);
			int id = Integer.parseInt(line.get(0));
			String name = line.get(1);
			int rupCount = Integer.parseInt(line.get(2));
			double totProb = Double.parseDouble(line.get(3));
			
			sourceInfoMap.put(id, new SourceInfo(id, name, rupCount, totProb));
		}
	}

	@Test
	public void test() {
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		assertEquals("Source count changed!", erf.getNumSources(), sourceInfoMap.size());
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			SourceInfo info = sourceInfoMap.get(sourceID);
			info.check(source);
		}
	}
	
	private static class SourceInfo {
		int id;
		String name;
		double totProb;
		int rupCount;
		public SourceInfo(int id, String name, int rupCount, double totProb) {
			super();
			this.id = id;
			this.name = name;
			this.rupCount = rupCount;
			this.totProb = totProb;
		}
		
		public void check(ProbEqkSource source) {
			assertEquals("Name change for "+id,  name, source.getName());
			assertEquals("Rup count change for "+id,  rupCount, source.getNumRuptures());
			assertEquals("Tot prob change for "+id,  totProb, source.computeTotalProb(), 1e-10);
		}
	}

}
