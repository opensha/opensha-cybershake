package org.opensha.sha.cybershake.calc.mcer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;

/**
 * Checks {@link USGS_NSHM_HazardClient} against a saved reply (USC 34.0192, -118.286, Vs30 260, periods 2-10 s,
 * retrieved 2026-11-20). No network access.
 */
public class USGS_NSHM_HazardClientTest {

	private static String readFixture(String name) throws IOException {
		try (InputStream is = USGS_NSHM_HazardClientTest.class.getResourceAsStream(name)) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/** Each requested period has a Total curve whose exceedance rate falls as ground motion rises. */
	@Test
	public void testParse() throws IOException {
		Map<Double, DiscretizedFunc> curves = USGS_NSHM_HazardClient.parse(
				readFixture("usgs_nshm_hazard_usc_D.json")).getTotalCurves();
		assertEquals(List.of(2d, 3d, 4d, 5d, 7.5, 10d), List.copyOf(curves.keySet()));
		for (DiscretizedFunc curve : curves.values()) {
			assertEquals(20, curve.size());
			for (int i=1; i<curve.size(); i++)
				assertTrue(curve.getY(i) < curve.getY(i-1));
		}
		// SA3P0 Total curve, first point
		assertEquals(0.002, curves.get(3d).getX(0), 1e-9);
		assertEquals(0.49244644, curves.get(3d).getY(0), 1e-9);
	}

	/** An error reply (e.g. Vs30 out of range) is rejected. */
	@Test(expected = IllegalStateException.class)
	public void testErrorStatus() {
		USGS_NSHM_HazardClient.parse("{\"status\":\"error\",\"response\":\"vs30 [50.0] not in range\"}");
	}

	/** Periods map to the service's IMT names, and Vs30 has no trailing ".0", which the service rejects. */
	@Test
	public void testURL() {
		assertEquals(USGS_NSHM_HazardClient.DEFAULT_URL+"/-118.286/34.0192/260?imt=SA2P0&imt=SA7P5&imt=SA10P0",
				new USGS_NSHM_HazardClient().buildURL(new Location(34.0192, -118.286), 260, 2, 7.5, 10));
	}

}
