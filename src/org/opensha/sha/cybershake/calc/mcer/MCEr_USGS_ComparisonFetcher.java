package org.opensha.sha.cybershake.calc.mcer;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.calc.mcer.MCErCalcUtils;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scratch.aftershockStatistics.ComcatAccessor;

public class MCEr_USGS_ComparisonFetcher {
	
	public static DiscretizedFunc getMCEr(Site site, DiscretizedFunc xValsFunc)
			throws IOException, ParseException {
		Location loc = site.getLocation();
		double vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
		
		URL url = buildURL(loc, vs30);
		
		ArbitrarilyDiscretizedFunc ret = null;
		
		InputStream is = null;
		try {
			is = getInputStream(url);

			JSONParser parser = new JSONParser();
			Object o = parser.parse(new InputStreamReader(is));
			Preconditions.checkState(o instanceof JSONObject);
			JSONObject j = (JSONObject)o;
//			ComcatAccessor.printJSON(j);
			
			JSONObject output = (JSONObject)j.get("output");
			
			Calculator calc = new Calculator(site, output);
			
			double sms = calc.getSms();
			double sm1 = calc.getSM1();
			double tl = calc.getTl();
			
			double sm1_psv = MCErCalcUtils.saToPsuedoVel(sm1, 1d);
			
			System.out.println("Site class ID for vs30="+vs30+": "+getSiteClassID(vs30));
			System.out.println("Sms = "+sms);
			System.out.println("Sm1 = "+sm1+" (PSV: "+sm1_psv+")");
			System.out.println("Tl = "+tl);
			
			List<Double> xVals = Lists.newArrayList();
			for (Point2D pt : xValsFunc)
				xVals.add(pt.getX());
			xVals.add(tl); // make sure that TL is in there
			
			ret = new ArbitrarilyDiscretizedFunc();
			
			for (double x : xVals) {
				if (x <= tl)
					ret.set(x, sm1_psv);
				else
					ret.set(x, sm1_psv*(tl/x));
			}
		} finally {
			try {
				is.close();
			} catch (NullPointerException npx) {
				// Don't throw null pointer exceptions because they mask the real
				// exception, eg, that an error occurred during I/O.
			}
		}
		
		return ret;
	}

	private static int getDesignCodeID() {
		return 1;
	}

	private static int getSiteClassID(double vs30) {
		// convert to ft/s
		vs30 *= 3.28084;

		// from Table 20.3-1 on http://earthquake.usgs.gov/designmaps/beta/us/ accessed 11/16/2015
		if (vs30 > 5000) {
			// Hard Rock, A
			return 1;
		} else if (vs30 > 2500) {
			// Rock, b
			// TODO unmeasured? return 3
			return 2;
		} else if (vs30 > 1200) {
			// Very dense soil and soft rick, C
			return 4;
		} else if (vs30 > 600) {
			// Stiff Soil, D
			return 5;
		} else {
			// Undetermined (the default) TODO
			return 6;
		}
	}
	
	private static Map<Integer, DiscretizedFunc> faTable;
	
	private static void initFaTable() {
		faTable = Maps.newHashMap();
		
//		  ssInfo: {
//		    bins: [0.25, 0.50, 0.75, 1.00, 1.25, 1.50],
//		    siteClasses: {
//		      'A': [0.8, 0.8, 0.8, 0.8, 0.8, 0.8],
//		      'B': [0.9, 0.9, 0.9, 0.9, 0.9, 0.9],
//		      'B-U': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
//		      'C': [1.3, 1.3, 1.2, 1.2, 1.2, 1.2],
//		      'D': [1.6, 1.4, 1.2, 1.1, 1.0, 1.0],
//		      // 'E': [2.4, 1.7, 1.3, 1.1, 1.0, 0.8],
//		      'U-D': [1.6, 1.4, 1.2, 1.2, 1.2, 1.2]
//		    }
//		  },
		
		double[] xVals = { 0.25, 0.50, 0.75, 1.00, 1.25, 1.50 };
		double[] aVals = { 0.8, 0.8, 0.8, 0.8, 0.8, 0.8 };
		double[] bVals = { 0.9, 0.9, 0.9, 0.9, 0.9, 0.9 };
		double[] b_uVals = { 1.0, 1.0, 1.0, 1.0, 1.0, 1.0 };
		double[] cVals = { 1.3, 1.3, 1.2, 1.2, 1.2, 1.2 };
		double[] dVals = { 1.6, 1.4, 1.2, 1.1, 1.0, 1.0 };
		double[] u_dVals = { 1.6, 1.4, 1.2, 1.2, 1.2, 1.2 };
		
		faTable.put(1, buildFunc(xVals, aVals));
		faTable.put(2, buildFunc(xVals, bVals));
		faTable.put(3, buildFunc(xVals, b_uVals));
		faTable.put(4, buildFunc(xVals, cVals));
		faTable.put(5, buildFunc(xVals, dVals));
		faTable.put(6, buildFunc(xVals, u_dVals));
	}
	
	public static synchronized double getFa(double vs30, double ss) {
		if (faTable == null)
			initFaTable();
		int siteClassID = getSiteClassID(vs30);
		DiscretizedFunc func = faTable.get(siteClassID);
		if (ss < func.getMinX())
			return func.getY(0);
		else if (ss > func.getMaxX())
			return func.getY(func.size()-1);
		return func.getInterpolatedY(ss);
	}
	
	private static Map<Integer, DiscretizedFunc> fvTable;
	
	private static void initFvTable() {
		fvTable = Maps.newHashMap();
		
//		  s1Info: {
//		    bins: [0.10, 0.20, 0.30, 0.40, 0.50, 0.60],
//		    siteClasses: {
//		      'A': [0.8, 0.8, 0.8, 0.8, 0.8, 0.8],
//		      'B': [0.8, 0.8, 0.8, 0.8, 0.8, 0.8],
//		      'B-U': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
//		      'C': [1.5, 1.5, 1.5, 1.5, 1.5, 1.4],
//		      'D': [2.4, 2.2, 2.0, 1.9, 1.8, 1.7],
//		      // 'E': [4.2, 3.3, 2.8, 2.4, 2.2, 2.0],
//		      'U-D': [2.4, 2.2, 2.0, 1.9, 1.8, 1.7]
//		    }
//		  },
		
		double[] xVals = { 0.10, 0.20, 0.30, 0.40, 0.50, 0.60 };
		double[] aVals = { 0.8, 0.8, 0.8, 0.8, 0.8, 0.8 };
		double[] bVals = { 0.8, 0.8, 0.8, 0.8, 0.8, 0.8 };
		double[] b_uVals = { 1.0, 1.0, 1.0, 1.0, 1.0, 1.0 };
		double[] cVals = { 1.5, 1.5, 1.5, 1.5, 1.5, 1.4 };
		double[] dVals = { 2.4, 2.2, 2.0, 1.9, 1.8, 1.7 };
		double[] u_dVals = { 2.4, 2.2, 2.0, 1.9, 1.8, 1.7 };
		
		fvTable.put(1, buildFunc(xVals, aVals));
		fvTable.put(2, buildFunc(xVals, bVals));
		fvTable.put(3, buildFunc(xVals, b_uVals));
		fvTable.put(4, buildFunc(xVals, cVals));
		fvTable.put(5, buildFunc(xVals, dVals));
		fvTable.put(6, buildFunc(xVals, u_dVals));
	}
	
	private static ArbitrarilyDiscretizedFunc buildFunc(double[] xVals, double[] yVals) {
		Preconditions.checkArgument(xVals.length == yVals.length);
		
		ArbitrarilyDiscretizedFunc func = new ArbitrarilyDiscretizedFunc();
		for (int i=0; i<xVals.length; i++)
			func.set(xVals[i], yVals[i]);
		return func;
	}
	
	public static synchronized double getFv(double vs30, double s1) {
		if (fvTable == null)
			initFvTable();
		int siteClassID = getSiteClassID(vs30);
		DiscretizedFunc func = fvTable.get(siteClassID);
		if (s1 < func.getMinX())
			return func.getY(0);
		else if (s1 > func.getMaxX())
			return func.getY(func.size()-1);
		return func.getInterpolatedY(s1);
	}

	private static int getRiskCategoryID() {
		return 1;
	}

	private static URL buildURL(Location loc, double vs30) throws MalformedURLException {
		String title = "OpenSHA-request-"+System.currentTimeMillis();
		return new URL("http://earthquake.usgs.gov/designmaps/beta/us/service/"+getDesignCodeID()
		+"/"+getSiteClassID(vs30)+"/"+getRiskCategoryID()+"/"
		+loc.getLongitude()+"/"+loc.getLatitude()+"/"+title);
	}

	private static final String GZIP_ENCODING = "gzip";

	private static InputStream getInputStream(final URL url) throws IOException {
		// request gzip
		URLConnection conn = url.openConnection();
		conn.addRequestProperty("Accept-encoding", GZIP_ENCODING);
		conn.connect();

		InputStream in = conn.getInputStream();

		// ungzip response
		if (GZIP_ENCODING.equals(conn.getContentEncoding())) {
			in = new GZIPInputStream(in);
		}

		return in;
	}

	private static class Calculator {
		double cr1;
		double max_direction_s1;
		double mapped_s1;
		double geomean_s1d;
		double percentile_s1;
		double deterministic_floor_s1;
		double crs;
		double max_direction_ss;
		double mapped_ss;
		double geomean_ssd;
		double percentile_ss;
		double deterministic_floor_ss;
		double tl;
		
		double vs30;
		
		public Calculator(Site site, JSONObject output) {
			Preconditions.checkNotNull(output);
			JSONObject metadata = (JSONObject) output.get("metadata");
			Preconditions.checkNotNull(metadata);
			String dataStr = output.get("data").toString();
			Preconditions.checkState(dataStr.startsWith("[") && dataStr.endsWith("]"));
			dataStr = dataStr.substring(1, dataStr.length()-1);
			List<String> dataStrings = Lists.newArrayList();
			while (!dataStr.isEmpty() && dataStr.contains("}")) {
				String myData = dataStr.substring(0, dataStr.indexOf("}")+1);
				if (myData.startsWith(","))
					myData = myData.substring(1);
				System.out.println(myData);
				dataStrings.add(myData);
				dataStr = dataStr.substring(dataStr.indexOf("}")+1);
			}
			double minDist = Double.POSITIVE_INFINITY;
			JSONObject data = null;
			JSONParser parser = new JSONParser();
			for (String dataString : dataStrings) {
				try {
					JSONObject myData = (JSONObject) parser.parse(dataString);
					Location dataLoc = new Location(
							Double.parseDouble(myData.get("latitude").toString()),
							Double.parseDouble(myData.get("longitude").toString()));
					double dist = LocationUtils.horzDistanceFast(site.getLocation(), dataLoc);
					if (dist < minDist) {
						minDist = dist;
						data = myData;
					}
				} catch (ParseException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			}
			
			cr1 = Double.parseDouble(data.get("cr1").toString());
			max_direction_s1 = Double.parseDouble(metadata.get("max_direction_s1").toString());
			mapped_s1 = Double.parseDouble(data.get("mapped_s1").toString());
			geomean_s1d = Double.parseDouble(data.get("geomean_s1d").toString());
			percentile_s1 = Double.parseDouble(metadata.get("percentile_s1").toString());
			deterministic_floor_s1 = Double.parseDouble(metadata.get("deterministic_floor_s1").toString());
			crs = Double.parseDouble(data.get("crs").toString());
			max_direction_ss = Double.parseDouble(metadata.get("max_direction_ss").toString());
			mapped_ss = Double.parseDouble(data.get("mapped_ss").toString());
			geomean_ssd = Double.parseDouble(data.get("geomean_ssd").toString());
			percentile_ss = Double.parseDouble(metadata.get("percentile_ss").toString());
			deterministic_floor_ss = Double.parseDouble(metadata.get("deterministic_floor_ss").toString());
			tl = Double.parseDouble(output.get("tl").toString());
			
			this.vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
		}

		public double getSM1() {
			return getFv() * getS1();
		}

		private double getFv() {
			return MCEr_USGS_ComparisonFetcher.getFv(vs30, getS1());
		}

		private double getS1() {
			return Math.min(getS1ur(), getS1d());
		}

		private double getS1ur() {
			return getS1uh() * cr1;
		}

		private double getS1d() {
			double pgdv841 = percentile_s1 * geomean_s1d;
			double maxD841 = max_direction_s1 * pgdv841;

			return Math.max(maxD841, deterministic_floor_s1);
		}

		private double getS1uh() {
			return max_direction_s1 * mapped_s1;
		}
		
		public double getSms() {
			return getFa() * getSs();
		}

		private double getFa() {
			return MCEr_USGS_ComparisonFetcher.getFa(vs30, getSs());
		}
		
		private double getSs() {
			return Math.min(getSsur(), getSsd());
		}
		
		private double getSsur() {
			return getSsuh() * crs;
		}
		
		private double getSsuh() {
			return max_direction_ss * mapped_ss;
		}
		
		private double getSsd() {
			double pgdv84 = percentile_ss * geomean_ssd;
			double maxD84 = max_direction_ss * pgdv84;

			return Math.max(maxD84, deterministic_floor_ss);
		}
		
		public double getTl() {
			return tl;
		}
	}

	public static void main(String[] args) throws IOException, ParseException {
		double vs30 = 748;
		Site site = new Site(new Location(34.148426,  -118.17119));
		Vs30_Param vs30Param = new Vs30_Param(vs30);
		vs30Param.setValueAsDefault();
		site.addParameter(vs30Param);
		URL url = buildURL(site.getLocation(), vs30);
		System.out.println(url);
		InputStream is = null;
		try {
			is = getInputStream(url);

			JSONParser parser = new JSONParser();
			Object o = parser.parse(new InputStreamReader(is));
			Preconditions.checkState(o instanceof JSONObject);
			JSONObject j = (JSONObject)o;
			ComcatAccessor.printJSON(j);
			//			JSONObject output = (JSONObject)j.get("output");
			//			System.out.println(output);
			//			Preconditions.checkNotNull(output);
			//			JSONObject metadata = (JSONObject)output.get("metadata");
			//			Preconditions.checkNotNull(metadata);
			//			
			//			System.out.println(metadata);
			//			
			//			System.out.println(metadata.get("sms"));
			
			JSONObject output = (JSONObject)j.get("output");
			
			Calculator calc = new Calculator(site, output);
			System.out.println("S1 = "+calc.getS1());
			System.out.println("Fv = "+calc.getFv());
			System.out.println("Sm1 = "+calc.getSM1());
			
			System.out.println("Ss = "+calc.getSs());
			System.out.println("Fa = "+calc.getFa());
			System.out.println("Sms = "+calc.getSms());
			
			System.out.println("Tl = "+calc.getTl());
		} finally {
			try {
				is.close();
			} catch (NullPointerException npx) {
				// Don't throw null pointer exceptions because they mask the real
				// exception, eg, that an error occurred during I/O.
			}
		}
	}

}
