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

	@Test
	public void testASCE7_16Point() {
		// lat, lon, period, vs30, prob_cs, det_cs, det_ll_cs, prob/det GMPE (none), det_ll, tool CS, GMPE, final
		List<String> row = List.of("34.05", "-118.25", "2.0", "351.9", "0.57", "1.02", "0.76", "NaN", "NaN", "false",
				"NaN", "0.5701", "0.5", "0.53");
		UGMS_GridReport.Point p = new UGMS_GridReport.Point(row, true);
		assertEquals(Term.PROB, p.csTerm[3]);
		assertEquals("option 3 is the tool's own MCER_CS", 0.5701, p.cs[3], 0d);
		assertEquals(0.57, p.cs3Formula, 1e-12);
		assertNull("no GMPE term without GMPE ingredients", p.gmpeTerm);
		assertTrue(p.raised[2] && p.raised[3]);
		assertEquals(MCERDataProductsCalc.calcFinalMCER(0.5701, 0.5, 2d), p.fin[3], 0d);
	}
}
