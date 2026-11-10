package org.opensha.sha.calc.mcer;

import static org.junit.Assert.*;

import org.junit.Test;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;

/**
 * Checks TL lookup for a valid outline and for a self-crossing outline that {@link Region} rejects.
 */
public class TLDataLoaderTest {

	// bowtie: edges cross at (lon 0.5, lat 0.5), leaving a left and a right triangle
	private static final double[][] BOWTIE = {{0, 0}, {1, 1}, {1, 0}, {0, 1}};
	private static final double[][] SQUARE = {{10, 10}, {11, 10}, {11, 11}, {10, 11}};

	private static TLDataLoader load() {
		CSVFile<String> nodes = new CSVFile<>(true);
		nodes.addLine("shapeid", "x", "y");
		for (double[] pt : BOWTIE)
			nodes.addLine("0", pt[0]+"", pt[1]+"");
		for (double[] pt : SQUARE)
			nodes.addLine("1", pt[0]+"", pt[1]+"");
		CSVFile<String> attributes = new CSVFile<>(true);
		attributes.addLine("shapeid", "objectid", "value");
		attributes.addLine("0", "1", "8");
		attributes.addLine("1", "2", "12");
		return new TLDataLoader(nodes, attributes);
	}

	@Test
	public void testBowtieNeedsFallback() {
		LocationList bowtie = new LocationList();
		for (double[] pt : BOWTIE)
			bowtie.add(new Location(pt[1], pt[0]));
		assertThrows(IllegalArgumentException.class, () -> new Region(bowtie, BorderType.GREAT_CIRCLE));
	}

	@Test
	public void testLookup() {
		TLDataLoader tl = load();
		assertEquals("inside valid square", 12d, tl.getValue(new Location(10.5, 10.5)), 0d);
		assertEquals("inside bowtie, left triangle", 8d, tl.getValue(new Location(0.5, 0.1)), 0d);
		assertEquals("inside bowtie, right triangle", 8d, tl.getValue(new Location(0.5, 0.9)), 0d);
		// outside the bowtie, about 4 km from its (0, 0) corner: near-miss buffer applies
		assertEquals("near miss of bowtie", 8d, tl.getValue(new Location(-0.04, 0)), 0d);
		assertTrue("far from every zone", Double.isNaN(tl.getValue(new Location(5, 5))));
	}

	@Test
	public void testPackagedTLMap() {
		// the USGS TL map is copied in by build.gradle; TL is 8 s across Southern California
		assertEquals(8d, ASCEDetLowerLimitCalc.getTl(new Location(33.9215, -117.8676)), 0d);
	}
}
