package org.opensha.sha.cybershake.calc.mcer;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Checks the Web Mercator tile maths {@link EsriBasemap} uses to line the basemap up with the map's latitude and
 * longitude axes (no network access).
 */
public class EsriBasemapTest {

	@Test
	public void testMercator() {
		int zoom = 10;
		double world = 256 * (1 << zoom);
		assertEquals(0d, EsriBasemap.mercX(-180, zoom), 1e-9);
		assertEquals(world, EsriBasemap.mercX(180, zoom), 1e-9);
		assertEquals(world/2, EsriBasemap.mercY(0, zoom), 1e-9);
		// symmetric about the equator
		assertEquals(world, EsriBasemap.mercY(34.05, zoom) + EsriBasemap.mercY(-34.05, zoom), 1e-6);
		// downtown Los Angeles is in tile 175/408 at zoom 10
		assertEquals(175, (int)(EsriBasemap.mercX(-118.25, zoom)/256));
		assertEquals(408, (int)(EsriBasemap.mercY(34.05, zoom)/256));
	}
}
