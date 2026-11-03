package org.opensha.sha.cybershake.calc.mcer;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.geo.Location;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Thin client for the USGS ASCE 7-22 building-codes web service
 * (<a href="https://earthquake.usgs.gov/ws/building-codes/asce7-22">earthquake.usgs.gov/ws/building-codes/asce7-22</a>),
 * which provides the GMPE side of the ASCE 7-22 UGMS calculation in {@link UGMS_WebToolCalc}.
 *
 * <p>Only the fields the UGMS calculation uses are exposed. They map to the JSON reply as follows:</p>
 * <ul>
 * <li>{@link Response#getRiskTargetedSpectrum()}: {@code response.data.underlyingData.riskTargetedSpectrum}
 * (probabilistic MCER)</li>
 * <li>{@link Response#getEightyFourthSpectrum()}: {@code response.data.underlyingData.eightyFourthSpectrum}
 * (deterministic 84th percentile). {@code pga84th} is the PGA value only.</li>
 * <li>{@link Response#getDetLowerLimitSpectrum()}: {@code response.metadata.sadFloor}
 * (deterministic lower limit, DLL)</li>
 * <li>{@link Response#getMCErSpectrum()}: {@code response.data.multiPeriodMCErSpectrum}, equal to
 * min(probabilistic, max(84th, DLL)) at each period. For checks only.</li>
 * </ul>
 *
 * <p>The service labels its spectra "maximum-direction" ({@code metadata.maxDirectionFactor}), which should
 * correspond to RotD100 as used for CyberShake.  Values are rounded to 2 decimals (some to 3),
 * so long-period ordinates lose precision.</p>
 *
 * <p>The risk category only changes the seismic design category, not the spectra, SMS, SM1 or PGAM, so
 * {@link #RISK_CATEGORY} is always sent.</p>
 */
public class USGS_ASCE7_22_Client {

	/** Address of the USGS ASCE 7-22 calculate endpoint */
	public static final String DEFAULT_URL = "https://earthquake.usgs.gov/ws/building-codes/asce7-22/calculate";
	/** Risk category sent with every request; it does not change the spectra or design values we use */
	public static final String RISK_CATEGORY = "II";

	private final HttpClient httpClient;

	/** Creates a client that follows redirects and allows 30 s to connect. */
	public USGS_ASCE7_22_Client() {
		this.httpClient = HttpClient.newBuilder()
				.followRedirects(HttpClient.Redirect.NORMAL)
				.connectTimeout(Duration.ofSeconds(30))
				.build();
	}

	/**
	 * Builds the request URL for the given location and ASCE 7-22 site class.
	 */
	public String buildURL(Location loc, String siteClass) {
		return DEFAULT_URL+"?latitude="+loc.getLatitude()+"&longitude="+loc.getLongitude()
				+"&riskCategory="+RISK_CATEGORY+"&siteClass="+URLEncoder.encode(siteClass, StandardCharsets.UTF_8)
				+"&title=UGMS";
	}

	/**
	 * Fetches the ASCE 7-22 results for the given location and site class (e.g. "D", "CD", "Default").
	 * @throws IOException if the request fails, returns a non-200 status (the service returns 422 with a
	 * message for invalid inputs), or the reply status is not "success"
	 */
	public Response calculate(Location loc, String siteClass) throws IOException {
		String url = buildURL(loc, siteClass);
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
			throw new IOException("USGS ASCE 7-22 request interrupted: "+url, e);
		}
		if (response.statusCode() != 200)
			throw new IOException("USGS ASCE 7-22 request failed (HTTP "+response.statusCode()+"): "+url
					+"\n"+response.body());
		try {
			return parse(response.body());
		} catch (RuntimeException e) {
			throw new IOException("Could not parse USGS ASCE 7-22 reply for "+url+": "+e.getMessage(), e);
		}
	}

	/**
	 * Parses a USGS ASCE 7-22 JSON reply. Package visible so tests can use saved replies.
	 * @throws IllegalStateException if the reply status is not "success" or an expected field is missing
	 */
	static Response parse(String json) {
		JsonObject root = JsonParser.parseString(json).getAsJsonObject();
		JsonObject request = root.getAsJsonObject("request");
		String status = request == null || !request.has("status") ? null : request.get("status").getAsString();
		if (!"success".equals(status))
			throw new IllegalStateException("USGS ASCE 7-22 reply status is '"+status+"': "+root.get("response"));
		return new Response(json, request.get("url").getAsString(), root.getAsJsonObject("response"));
	}

	/**
	 * One parsed reply from the USGS ASCE 7-22 service.
	 */
	public static class Response {

		private final String json;
		private final String url;
		private final ArbitrarilyDiscretizedFunc riskTargeted, eightyFourth, detLowerLimit, mcer;
		private final double pgam, tl, sms, sm1, vs30;
		private final String modelVersion;

		private Response(String json, String url, JsonObject response) {
			this.json = json;
			this.url = url;
			JsonObject data = response.getAsJsonObject("data");
			JsonObject underlying = data.getAsJsonObject("underlyingData");
			JsonObject metadata = response.getAsJsonObject("metadata");
			riskTargeted = toFunc(underlying, "riskTargetedSpectrum", "USGS Probabilistic");
			eightyFourth = toFunc(underlying, "eightyFourthSpectrum", "USGS 84th Percentile");
			detLowerLimit = toFunc(metadata, "sadFloor", "USGS Deterministic Lower Limit");
			mcer = toFunc(data, "multiPeriodMCErSpectrum", "USGS MCER");
			pgam = data.get("pgam").getAsDouble();
			tl = data.get("tl").getAsDouble();
			sms = data.get("sms").getAsDouble();
			sm1 = data.get("sm1").getAsDouble();
			vs30 = metadata.get("vs30").getAsDouble();
			modelVersion = metadata.get("modelVersion").getAsString();
		}

		private static ArbitrarilyDiscretizedFunc toFunc(JsonObject parent, String name, String funcName) {
			JsonObject obj = parent.getAsJsonObject(name);
			if (obj == null)
				throw new IllegalStateException("Missing spectrum '"+name+"' in USGS ASCE 7-22 reply");
			JsonArray periods = obj.getAsJsonArray("periods");
			JsonArray ordinates = obj.getAsJsonArray("ordinates");
			if (periods.size() == 0 || periods.size() != ordinates.size())
				throw new IllegalStateException("Bad spectrum '"+name+"': "+periods.size()+" periods, "
						+ordinates.size()+" ordinates");
			ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc(funcName);
			int i = 0;
			for (JsonElement period : periods)
				func.set(period.getAsDouble(), ordinates.get(i++).getAsDouble());
			return func;
		}

		/** Probabilistic risk-targeted spectrum ({@code underlyingData.riskTargetedSpectrum}), includes T=0 (PGA) */
		public ArbitrarilyDiscretizedFunc getRiskTargetedSpectrum() { return riskTargeted; }

		/** Deterministic 84th-percentile spectrum ({@code underlyingData.eightyFourthSpectrum}), includes T=0 */
		public ArbitrarilyDiscretizedFunc getEightyFourthSpectrum() { return eightyFourth; }

		/** Deterministic lower limit (DLL) spectrum ({@code metadata.sadFloor}), includes T=0 */
		public ArbitrarilyDiscretizedFunc getDetLowerLimitSpectrum() { return detLowerLimit; }

		/** USGS's own multi-period MCER spectrum. For checks only; UGMS recomputes MCER after merging. */
		public ArbitrarilyDiscretizedFunc getMCErSpectrum() { return mcer; }

		/** @return PGAM (g), the MCER peak ground acceleration from the service */
		public double getPGAM() { return pgam; }

		/** @return TL (s), the long-period transition period, which depends only on location */
		public double getTL() { return tl; }

		/** USGS's own SMS, for reference only; UGMS recomputes it from the merged spectrum */
		public double getSMS() { return sms; }

		/** USGS's own SM1, for reference only; UGMS recomputes it from the merged spectrum */
		public double getSM1() { return sm1; }

		/** Vs30 (m/s) the service used for the requested site class */
		public double getVs30() { return vs30; }

		/** USGS hazard model version, e.g. "v5.0.x" */
		public String getModelVersion() { return modelVersion; }

		/** Request URL echoed by the service */
		public String getURL() { return url; }

		/** Raw JSON reply, saved with each run so it can be reproduced */
		public String getJson() { return json; }
	}

}
