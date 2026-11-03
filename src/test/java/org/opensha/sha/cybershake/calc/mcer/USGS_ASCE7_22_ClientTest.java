package org.opensha.sha.cybershake.calc.mcer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Checks the USGS ASCE 7-22 field mapping and the ASCE 7-22 rules used by {@link UGMS_WebToolCalc} against
 * saved service replies (Los Angeles 34.05, -118.25, class D, and USC 34.0192, -118.286, classes C, CD, D and
 * Default, retrieved 2026-10-20). No network access.
 */
public class USGS_ASCE7_22_ClientTest {

	// the service rounds to 2 decimals
	private static final double TOL = 0.01;

	private static USGS_ASCE7_22_Client.Response response;

	/**
	 * Parses the saved Los Angeles class D reply used by most tests.
	 * @throws IOException if the fixture cannot be read
	 */
	@BeforeClass
	public static void setUp() throws IOException {
		response = USGS_ASCE7_22_Client.parse(readFixture("usgs_asce7-22_la_D.json"));
	}
	
	private static String readFixture(String name) throws IOException {
		try (InputStream is = USGS_ASCE7_22_ClientTest.class.getResourceAsStream(name)) {
			return new String(is.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/** The parsed reply exposes the expected spectra and scalar values. */
	@Test
	public void testFieldMapping() {
		assertEquals(22, response.getRiskTargetedSpectrum().size());
		assertEquals(22, response.getEightyFourthSpectrum().size());
		assertEquals(22, response.getDetLowerLimitSpectrum().size());
		assertEquals(0.87, response.getPGAM(), 1e-6);
		assertEquals(8d, response.getTL(), 1e-6);
		assertEquals(260d, response.getVs30(), 1e-6);
		assertEquals("v5.0.x", response.getModelVersion());
		assertEquals(1.06, response.getEightyFourthSpectrum().getY(0.01), 1e-6);
		assertEquals(0.70, response.getDetLowerLimitSpectrum().getY(0.01), 1e-6);
	}

	/** Lesser of probabilistic and greater of (84th, DLL) reproduces the service's MCER spectrum. */
	@Test
	public void testMCErRule() {
		// min(probabilistic, max(84th, DLL)) must reproduce the service's own MCER spectrum
		DiscretizedFunc mcer = calcMCER(response);
		DiscretizedFunc usgs = response.getMCErSpectrum();
		for (int i=0; i<usgs.size(); i++)
			assertEquals("MCER at T="+usgs.getX(i), usgs.getY(i), mcer.getY(usgs.getX(i)), TOL);
	}

	/** The 7-22 SMS and SM1 rules reproduce the service's own values. */
	@Test
	public void testDesignValueRules() {
		DiscretizedFunc mcer = calcMCER(response);
		assertEquals(response.getSMS(), UGMS_WebToolCalc.calcSXS(mcer), TOL);
		assertEquals(response.getSM1(), UGMS_WebToolCalc.calcSX1(mcer), TOL);
	}

	/**
	 * The production Default calculation over classes C, CD and D reproduces the service's Default MCER, SMS and SM1.
	 * @throws IOException if a fixture cannot be read
	 */
	@Test
	public void testDefaultEnvelope() throws IOException {
		// the Default class must equal the service's own Default result, which has no underlying spectra
		List<DiscretizedFunc> probs = new ArrayList<>(), dets = new ArrayList<>(), detLowers = new ArrayList<>();
		for (String siteClass : new String[] {"C", "CD", "D"}) {
			USGS_ASCE7_22_Client.Response reply = USGS_ASCE7_22_Client.parse(
					readFixture("usgs_asce7-22_usc_"+siteClass+".json"));
			probs.add(reply.getRiskTargetedSpectrum());
			dets.add(reply.getEightyFourthSpectrum());
			detLowers.add(reply.getDetLowerLimitSpectrum());
		}
		DiscretizedFunc envelope = UGMS_WebToolCalc.calcDefaultMCER(probs, dets, detLowers);

		JsonObject data = JsonParser.parseString(readFixture("usgs_asce7-22_usc_Default.json")).getAsJsonObject()
				.getAsJsonObject("response").getAsJsonObject("data");
		JsonObject usgs = data.getAsJsonObject("multiPeriodMCErSpectrum");
		JsonArray periods = usgs.getAsJsonArray("periods");
		JsonArray ordinates = usgs.getAsJsonArray("ordinates");
		assertEquals(periods.size(), envelope.size());
		for (int i=0; i<periods.size(); i++) {
			double period = periods.get(i).getAsDouble();
			assertEquals("Default MCER at T="+period, ordinates.get(i).getAsDouble(), envelope.getY(period), TOL);
		}
		assertEquals(data.get("sms").getAsDouble(), UGMS_WebToolCalc.calcSXS(envelope), TOL);
		assertEquals(data.get("sm1").getAsDouble(), UGMS_WebToolCalc.calcSX1(envelope), TOL);
	}

	/** Default takes the largest class MCER, not the MCER of the largest ingredients, when classes control differently. */
	@Test
	public void testDefaultOrder() {
		// class C: P 1.0, Det 0.6 -> MCER 0.6; class D: P 0.7, Det 0.9 -> MCER 0.7; enveloping ingredients first gives 0.9
		DiscretizedFunc mcer = UGMS_WebToolCalc.calcDefaultMCER(List.of(func(1.0), func(0.7)),
				List.of(func(0.6), func(0.9)), List.of(func(0.5), func(0.5)));
		assertEquals(0.7, mcer.getY(1d), 1e-9);
	}

	private static DiscretizedFunc func(double y) {
		DiscretizedFunc f = new ArbitrarilyDiscretizedFunc();
		f.set(1d, y);
		return f;
	}

	/** A reply whose status is not "success" is rejected. */
	@Test(expected = IllegalStateException.class)
	public void testErrorStatus() {
		USGS_ASCE7_22_Client.parse("{\"request\":{\"status\":\"error\"},\"response\":\"bad input\"}");
	}

	private static DiscretizedFunc calcMCER(USGS_ASCE7_22_Client.Response response) {
		return UGMS_WebToolCalc.calcMCER(response.getRiskTargetedSpectrum(), response.getEightyFourthSpectrum(),
				response.getDetLowerLimitSpectrum());
	}

}
