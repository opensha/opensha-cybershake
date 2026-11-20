package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.Duration;

import javax.imageio.ImageIO;

import org.jfree.chart.annotations.AbstractXYAnnotation;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotRenderingInfo;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.ui.RectangleEdge;

import com.google.common.base.Preconditions;

/**
 * Esri World Light Gray Canvas basemap (no API key needed), as the earlier Python maps used: the base layer and its
 * transparent reference-label layer, stitched from Web Mercator tiles and resampled to the plain latitude/longitude
 * axes of the report maps. Tiles are cached on disk, so later builds don't download them again.
 */
class EsriBasemap {

	static final String ATTRIBUTION = "Esri World Light Gray Canvas (Esri, DeLorme, NAVTEQ)";
	private static final String URL = "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/%s/MapServer/tile/%d/%d/%d";
	static final String BASE = "World_Light_Gray_Base";
	static final String LABELS = "World_Light_Gray_Reference";
	private static final int TILE = 256;

	private final File cacheDir;
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();

	EsriBasemap(File cacheDir) {
		this.cacheDir = cacheDir;
	}

	static double mercX(double lon, int zoom) {
		return (lon + 180d)/360d * TILE * (1 << zoom);
	}

	static double mercY(double lat, int zoom) {
		double rad = Math.toRadians(lat);
		return (1d - Math.log(Math.tan(rad) + 1d/Math.cos(rad))/Math.PI)/2d * TILE * (1 << zoom);
	}

	/**
	 * One layer over the given extent, with rows evenly spaced in latitude (to match the map axes)
	 */
	BufferedImage image(String layer, int zoom, double minLat, double maxLat, double minLon, double maxLon)
			throws IOException, InterruptedException {
		double x0 = mercX(minLon, zoom), x1 = mercX(maxLon, zoom);
		double y0 = mercY(maxLat, zoom), y1 = mercY(minLat, zoom);
		int tx0 = (int)(x0/TILE), tx1 = (int)(x1/TILE), ty0 = (int)(y0/TILE), ty1 = (int)(y1/TILE);
		BufferedImage mosaic = new BufferedImage((tx1-tx0+1)*TILE, (ty1-ty0+1)*TILE, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g = mosaic.createGraphics();
		for (int tx=tx0; tx<=tx1; tx++)
			for (int ty=ty0; ty<=ty1; ty++)
				g.drawImage(tile(layer, zoom, tx, ty), (tx-tx0)*TILE, (ty-ty0)*TILE, null);
		g.dispose();

		// Web Mercator rows are not evenly spaced in latitude: pick each output row's source row
		int width = (int)Math.round(x1 - x0), height = (int)Math.round(y1 - y0);
		BufferedImage out = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		for (int r=0; r<height; r++) {
			double lat = maxLat - (r + 0.5)/height*(maxLat - minLat);
			int srcY = (int)(mercY(lat, zoom) - ty0*TILE);
			for (int c=0; c<width; c++)
				out.setRGB(c, r, mosaic.getRGB((int)(x0 - tx0*TILE + c + 0.5), srcY));
		}
		return out;
	}

	private BufferedImage tile(String layer, int zoom, int x, int y) throws IOException, InterruptedException {
		File file = new File(cacheDir, layer+"/"+zoom+"/"+x+"_"+y+".png");
		byte[] bytes;
		if (file.exists()) {
			bytes = Files.readAllBytes(file.toPath());
		} else {
			HttpRequest req = HttpRequest.newBuilder(URI.create(String.format(URL, layer, zoom, y, x)))
					.timeout(Duration.ofSeconds(30)).header("User-Agent", "opensha-cybershake UGMS_GridReport").build();
			HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
			if (resp.statusCode() != 200)
				throw new IOException("HTTP "+resp.statusCode()+" for "+req.uri());
			bytes = resp.body();
		}
		BufferedImage img = ImageIO.read(new ByteArrayInputStream(bytes));
		if (img == null)
			throw new IOException("Not an image: "+(file.exists() ? file : String.format(URL, layer, zoom, y, x)));
		if (!file.exists()) {
			// cache only tiles that decode, so a bad reply isn't kept
			Preconditions.checkState(file.getParentFile().exists() || file.getParentFile().mkdirs());
			Files.write(file.toPath(), bytes);
		}
		return img;
	}

	/**
	 * Draws an image over a latitude/longitude rectangle of the plot, scaled to it
	 */
	static class ImageAnnotation extends AbstractXYAnnotation {

		private final BufferedImage image;
		private final double minLon, maxLon, minLat, maxLat;

		ImageAnnotation(BufferedImage image, double minLat, double maxLat, double minLon, double maxLon) {
			this.image = image;
			this.minLon = minLon;
			this.maxLon = maxLon;
			this.minLat = minLat;
			this.maxLat = maxLat;
		}

		@Override
		public void draw(Graphics2D g2, XYPlot plot, Rectangle2D dataArea, ValueAxis domainAxis, ValueAxis rangeAxis,
				int rendererIndex, PlotRenderingInfo info) {
			RectangleEdge xEdge = plot.getDomainAxisEdge(), yEdge = plot.getRangeAxisEdge();
			double x0 = domainAxis.valueToJava2D(minLon, dataArea, xEdge);
			double x1 = domainAxis.valueToJava2D(maxLon, dataArea, xEdge);
			double y0 = rangeAxis.valueToJava2D(maxLat, dataArea, yEdge);
			double y1 = rangeAxis.valueToJava2D(minLat, dataArea, yEdge);
			g2.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
			g2.drawImage(image, (int)Math.round(x0), (int)Math.round(y0), (int)Math.round(x1-x0),
					(int)Math.round(y1-y0), null);
		}
	}
}
