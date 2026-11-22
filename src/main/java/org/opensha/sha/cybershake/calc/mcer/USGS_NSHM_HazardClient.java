package org.opensha.sha.cybershake.calc.mcer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Thin client for the USGS NSHM 2018 dynamic hazard-curve web service
 * (<a href="https://earthquake.usgs.gov/ws/nshmp/conus-2018.R1/dynamic">earthquake.usgs.gov/ws/nshmp/conus-2018.R1/dynamic</a>),
 * the model underlying ASCE 7-22.
 *
 * <p>Only the {@code Total} curve for each spectral period is exposed, from
 * {@code response.hazardCurves[].data[component=Total].values}: x is ground motion (g) and y is annual frequency of
 * exceedance. The curves are RotD50 (geometric mean), not the maximum-direction values of {@link USGS_ASCE7_22_Client}.</p>
 */
public class USGS_NSHM_HazardClient {

	/** The model the endpoint serves, as named in reports */
	public static final String MODEL_NAME = "NSHM 2018";
	/** Address of the USGS NSHM 2018 dynamic hazard endpoint */
	public static final String DEFAULT_URL = "https://earthquake.usgs.gov/ws/nshmp/conus-2018.R1/dynamic/hazard";

	private static final DecimalFormat periodDF = new DecimalFormat("0.0#");
	// the service fails on "260.0" ("Cannot read properties of undefined"), so no trailing zeros
	private static final DecimalFormat vs30DF = new DecimalFormat("0.##");

	private final HttpClient httpClient;

	/** Creates a client that follows redirects and allows 30 s to connect. */
	public USGS_NSHM_HazardClient() {
		this.httpClient = HttpClient.newBuilder()
				.followRedirects(HttpClient.Redirect.NORMAL)
				.connectTimeout(Duration.ofSeconds(30))
				.build();
	}

	/**
	 * Service IMT name for a spectral period, e.g. 2 -> SA2P0, 7.5 -> SA7P5
	 */
	static String toIMT(double period) {
		return "SA"+periodDF.format(period).replace('.', 'P');
	}

	/**
	 * Builds the request URL for the given location, Vs30 (150-1500 m/s) and spectral periods.
	 */
	public String buildURL(Location loc, double vs30, double... periods) {
		StringBuilder url = new StringBuilder(DEFAULT_URL).append("/").append(loc.getLongitude())
				.append("/").append(loc.getLatitude()).append("/").append(vs30DF.format(vs30));
		for (int i=0; i<periods.length; i++)
			url.append(i == 0 ? "?" : "&").append("imt=").append(toIMT(periods[i]));
		return url.toString();
	}

	/**
	 * Fetches the hazard curves for the given location, Vs30 and spectral periods.
	 * @throws IOException if the request fails, returns a non-200 status, or the reply status is not "success"
	 * (e.g. Vs30 out of range)
	 */
	public Response calculate(Location loc, double vs30, double... periods) throws IOException {
		String url = buildURL(loc, vs30, periods);
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(url))
				.GET()
				.header("Accept", "application/json")
				.timeout(Duration.ofSeconds(120))
				.build();
		HttpResponse<String> response;
		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("USGS NSHM hazard request interrupted: "+url, e);
		}
		if (response.statusCode() != 200)
			throw new IOException("USGS NSHM hazard request failed (HTTP "+response.statusCode()+"): "+url
					+"\n"+response.body());
		try {
			return parse(response.body());
		} catch (RuntimeException e) {
			throw new IOException("Could not parse USGS NSHM hazard reply for "+url+": "+e.getMessage(), e);
		}
	}

	/**
	 * Parses a USGS NSHM hazard JSON reply. Package visible so tests can use saved replies.
	 * @throws IllegalStateException if the reply status is not "success" or a curve is missing or malformed
	 */
	static Response parse(String json) {
		JsonObject root = JsonParser.parseString(json).getAsJsonObject();
		String status = root.has("status") ? root.get("status").getAsString() : null;
		if (!"success".equals(status))
			throw new IllegalStateException("USGS NSHM hazard reply status is '"+status+"': "+root.get("response"));
		Map<Double, DiscretizedFunc> curves = new TreeMap<>();
		for (JsonElement el : root.getAsJsonObject("response").getAsJsonArray("hazardCurves")) {
			JsonObject curve = el.getAsJsonObject();
			String imt = curve.getAsJsonObject("imt").get("value").getAsString();
			if (!imt.startsWith("SA"))
				continue;
			double period = Double.parseDouble(imt.substring(2).replace('P', '.'));
			JsonObject values = null;
			for (JsonElement data : curve.getAsJsonArray("data"))
				if ("Total".equals(data.getAsJsonObject().get("component").getAsString()))
					values = data.getAsJsonObject().getAsJsonObject("values");
			if (values == null)
				throw new IllegalStateException("No Total curve for "+imt+" in USGS NSHM hazard reply");
			JsonArray xs = values.getAsJsonArray("xs");
			JsonArray ys = values.getAsJsonArray("ys");
			if (xs.size() == 0 || xs.size() != ys.size())
				throw new IllegalStateException("Bad curve for "+imt+": "+xs.size()+" xs, "+ys.size()+" ys");
			ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc("USGS "+MODEL_NAME);
			for (int i=0; i<xs.size(); i++)
				func.set(xs.get(i).getAsDouble(), ys.get(i).getAsDouble());
			curves.put(period, func);
		}
		return new Response(json, Collections.unmodifiableMap(curves));
	}

	/**
	 * One parsed reply from the USGS NSHM hazard service.
	 */
	public static class Response {

		private final String json;
		private final Map<Double, DiscretizedFunc> totalCurves;

		private Response(String json, Map<Double, DiscretizedFunc> totalCurves) {
			this.json = json;
			this.totalCurves = totalCurves;
		}

		/** Total hazard curve (x = SA in g, y = annual frequency of exceedance) for each period (s), sorted */
		public Map<Double, DiscretizedFunc> getTotalCurves() { return totalCurves; }

		/** Raw JSON reply, saved with each run so it can be reproduced */
		public String getJson() { return json; }
	}

}
