package org.opensha.sha.cybershake.calc.mcer;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.calc.mcer.UGMS_GridReport.Term;

/**
 * Checks the grid layout and the controlling-term rule used by {@link UGMS_GridCalc} and {@link UGMS_GridReport}.
 */
public class UGMS_GridTest {

	@Test
	public void testGridLocations() {
		// 773 coordinates at 0.05 deg inside the v18.4 web-tool region
		List<Location> locs = UGMS_GridCalc.gridLocations(0.05);
		assertEquals(773, locs.size());
		assertTrue(locs.contains(new Location(34.05, -118.25)));
		assertFalse("outside the region", locs.contains(new Location(34.9, -117.1)));
	}

	@Test
	public void testControl() {
		assertEquals(Term.PROB, UGMS_GridReport.control(0.3, 0.5, 0.2));
		assertEquals(Term.DET, UGMS_GridReport.control(0.6, 0.5, 0.2));
		assertEquals(Term.DET_LL, UGMS_GridReport.control(0.6, 0.1, 0.2));
		// ties go to Prob, then Det
		assertEquals(Term.PROB, UGMS_GridReport.control(0.5, 0.5, 0.2));
		assertEquals(Term.DET, UGMS_GridReport.control(0.6, 0.2, 0.2));
		// no USGS 84th spectrum: no deterministic cap
		assertEquals(Term.PROB, UGMS_GridReport.control(0.6, Double.POSITIVE_INFINITY, 0.2));
	}
}
