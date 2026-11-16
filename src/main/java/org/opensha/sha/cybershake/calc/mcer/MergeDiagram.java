package org.opensha.sha.cybershake.calc.mcer;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.RenderingHints;
import java.awt.geom.Line2D;
import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.awt.geom.RoundRectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.opensha.sha.cybershake.calc.mcer.UGMS_GridReport.Term;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfWriter;

/**
 * The report's diagram of how the final MCER is formed: each side's MCER, the merge test, and which map cell and
 * statistics-table row each outcome gives. Written as a PNG for the HTML and a vector PDF for the PDF report.
 */
class MergeDiagram {

	private static final int W = 720, H = 380;
	private static final Color HEADER = new Color(36, 86, 90);
	private static final Color BOX = new Color(244, 244, 241);
	private static final Color RULE = new Color(160, 170, 170);
	private static final Color INK = new Color(31, 31, 30);
	private static final Font TITLE = new Font(Font.SANS_SERIF, Font.BOLD, 12);
	private static final Font BODY = new Font(Font.SANS_SERIF, Font.PLAIN, 10);
	private static final Font MONO = new Font(Font.MONOSPACED, Font.PLAIN, 10);

	/**
	 * @param weights the CyberShake weights, written under the blend formula
	 */
	static void write(File dir, String prefix, String weights) throws IOException {
		int scale = 3;
		BufferedImage img = new BufferedImage(W*scale, H*scale, BufferedImage.TYPE_INT_RGB);
		Graphics2D g = img.createGraphics();
		g.setColor(Color.WHITE);
		g.fillRect(0, 0, W*scale, H*scale);
		g.scale(scale, scale);
		draw(g, weights);
		g.dispose();
		ImageIO.write(img, "png", new File(dir, prefix+".png"));

		Document doc = new Document(new Rectangle(W, H), 0, 0, 0, 0);
		try (FileOutputStream out = new FileOutputStream(new File(dir, prefix+".pdf"))) {
			PdfWriter writer = PdfWriter.getInstance(doc, out);
			doc.open();
			Graphics2D pdf = writer.getDirectContent().createGraphics(W, H);
			draw(pdf, weights);
			pdf.dispose();
			doc.close();
		} catch (DocumentException e) {
			throw new IOException(e);
		}
	}

	private static void draw(Graphics2D g, String weights) {
		g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

		box(g, 10, 10, 340, 120, "CyberShake side: MCER_CS", null, new String[] {
				"Option 1:  min[ Prob_CS, max( Det_GMPE, Det_LL ) ]",
				"Option 2:  Prob_CS",
				"Option 3:  min[ Prob_CS, max( Det_CS, Det_LL ) ]",
				"It equals one of its terms: the term that sets it."}, new boolean[] {true, true, true, false});
		box(g, 370, 10, 340, 120, "GMPE side: MCER_GMPE (every option)", null, new String[] {
				"min[ Prob_GMPE, max( Det_GMPE, Det_LL ) ]",
				"From the USGS ASCE 7-22 service.",
				"",
				"It equals one of its terms: the term that sets it."}, new boolean[] {true, false, false, false});

		// the merge test
		RoundRectangle2D test = new RoundRectangle2D.Double(220, 150, 280, 36, 10, 10);
		g.setColor(Color.WHITE);
		g.fill(test);
		g.setColor(INK);
		g.setStroke(new BasicStroke(1.2f));
		g.draw(test);
		g.setFont(TITLE);
		centered(g, "Is MCER_GMPE >= MCER_CS ?", 360, 173);
		arrow(g, 180, 130, 290, 150);
		arrow(g, 540, 130, 430, 150);
		arrow(g, 260, 186, 180, 230);
		arrow(g, 460, 186, 540, 230);
		g.setFont(TITLE);
		g.setColor(INK);
		g.drawString("yes", 172, 214);
		g.drawString("no", 524, 214);

		box(g, 10, 230, 340, 140, "Yes: GMPE kept", Term.PROB.hatched(), new String[] {
				"Final = MCER_GMPE",
				"Map: hatched cell, coloured by the GMPE term",
				"Table row 2: counted by the GMPE term"}, new boolean[] {true, false, false});
		box(g, 370, 230, 340, 140, "No: CS raised", Term.PROB.solid(), new String[] {
				"Final = exp[ w ln MCER_CS + (1 - w) ln MCER_GMPE ]",
				weights,
				"Map: solid cell, coloured by the CS term",
				"Table row 1: counted by the CS term",
				"Table row 3: the same cells, counted by the GMPE term"}, new boolean[] {true, false, false, false, false});
	}

	/**
	 * A titled box; mono marks formula lines. A swatch, if given, is drawn beside the "Map:" line.
	 */
	private static void box(Graphics2D g, double x, double y, double w, double h, String title, Paint swatch,
			String[] lines, boolean[] mono) {
		g.setColor(BOX);
		g.fill(new Rectangle2D.Double(x, y, w, h));
		g.setColor(RULE);
		g.setStroke(new BasicStroke(0.8f));
		g.draw(new Rectangle2D.Double(x, y, w, h));
		g.setColor(HEADER);
		g.fill(new Rectangle2D.Double(x, y, w, 24));
		g.setColor(Color.WHITE);
		g.setFont(TITLE);
		g.drawString(title, (float)(x+10), (float)(y+16.5));
		double ty = y + 46;
		for (int i=0; i<lines.length; i++, ty += 20) {
			g.setColor(INK);
			g.setFont(mono[i] ? MONO : BODY);
			double tx = x + 10;
			if (swatch != null && lines[i].startsWith("Map:")) {
				Rectangle2D sw = new Rectangle2D.Double(tx, ty-10, 12, 12);
				g.setPaint(swatch);
				g.fill(sw);
				g.setColor(Color.GRAY);
				g.setStroke(new BasicStroke(0.5f));
				g.draw(sw);
				g.setColor(INK);
				tx += 18;
			}
			g.drawString(lines[i], (float)tx, (float)ty);
		}
	}

	private static void centered(Graphics2D g, String text, double cx, double baseline) {
		g.drawString(text, (float)(cx - g.getFontMetrics().stringWidth(text)/2d), (float)baseline);
	}

	private static void arrow(Graphics2D g, double x0, double y0, double x1, double y1) {
		g.setColor(INK);
		g.setStroke(new BasicStroke(1.2f));
		double angle = Math.atan2(y1-y0, x1-x0), len = 8;
		double bx = x1 - len*Math.cos(angle), by = y1 - len*Math.sin(angle);
		g.draw(new Line2D.Double(x0, y0, bx, by));
		Path2D head = new Path2D.Double();
		head.moveTo(x1, y1);
		head.lineTo(bx + 4*Math.sin(angle), by - 4*Math.cos(angle));
		head.lineTo(bx - 4*Math.sin(angle), by + 4*Math.cos(angle));
		head.closePath();
		g.fill(head);
	}
}
