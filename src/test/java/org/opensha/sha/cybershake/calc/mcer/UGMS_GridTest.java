package org.opensha.sha.cybershake.calc.mcer;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.opensha.commons.geo.Location;

/**
 * Checks the grid layout used by {@link UGMS_GridCalc}.
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
}
