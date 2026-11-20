package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TableBlock;
import org.commonmark.ext.gfm.tables.TableCell;
import org.commonmark.ext.gfm.tables.TableHead;
import org.commonmark.ext.gfm.tables.TableRow;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.BulletList;
import org.commonmark.node.Code;
import org.commonmark.node.Emphasis;
import org.commonmark.node.FencedCodeBlock;
import org.commonmark.node.HardLineBreak;
import org.commonmark.node.Heading;
import org.commonmark.node.HtmlInline;
import org.commonmark.node.Image;
import org.commonmark.node.Link;
import org.commonmark.node.ListItem;
import org.commonmark.node.Node;
import org.commonmark.node.Paragraph;
import org.commonmark.node.SoftLineBreak;
import org.commonmark.node.StrongEmphasis;
import org.commonmark.node.Text;
import org.commonmark.parser.Parser;

import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfWriter;

/**
 * Renders the Markdown subset the UGMS grid report uses (headings, paragraphs, inline code, bold, &lt;sub&gt;, bullet
 * lists, fenced code and GFM tables with images) to a PDF with iText, so the PDF has the same content as the HTML
 * report. The "Table of Contents" section is left out, since its links only work in HTML.
 */
class MarkdownToPDF {

	private static final BaseColor HEADER_BG = new BaseColor(36, 86, 90);
	private static final BaseColor STRIPE_BG = new BaseColor(241, 243, 242);
	private static final BaseColor CODE_BG = new BaseColor(244, 244, 241);
	private static final BaseColor RULE = new BaseColor(200, 205, 205);

	private final File baseDir;
	private final BiFunction<Integer, String, Rectangle> pageBreak;
	private final Document doc = new Document(PageSize.LETTER, 54, 54, 54, 54);
	private PdfWriter writer;
	private boolean sub, sup; // inside <sub>...</sub> or <sup>...</sup>

	/**
	 * @param baseDir directory that image paths are relative to
	 * @param pageBreak for a heading (level, text): the page size to start a new page with, or null for no break
	 */
	private MarkdownToPDF(File baseDir, BiFunction<Integer, String, Rectangle> pageBreak) {
		this.baseDir = baseDir;
		this.pageBreak = pageBreak;
	}

	static void write(List<String> markdownLines, File baseDir, File pdfFile,
			BiFunction<Integer, String, Rectangle> pageBreak) throws IOException {
		List<Extension> extensions = List.of(TablesExtension.create());
		Node root = Parser.builder().extensions(extensions).build().parse(String.join("\n", markdownLines));
		MarkdownToPDF renderer = new MarkdownToPDF(baseDir, pageBreak);
		try {
			renderer.writer = PdfWriter.getInstance(renderer.doc, new FileOutputStream(pdfFile));
			renderer.doc.open();
			renderer.blocks(root);
			renderer.doc.close();
		} catch (DocumentException e) {
			throw new IOException(e);
		}
	}

	private void blocks(Node root) throws DocumentException, IOException {
		boolean skipList = false;
		for (Node n = root.getFirstChild(); n != null; n = n.getNext()) {
			if (n instanceof Heading h) {
				String title = plainText(h);
				if (title.equals("Table of Contents")) {
					skipList = true;
					continue;
				}
				Rectangle pageSize = pageBreak.apply(h.getLevel(), title);
				if (pageSize != null) {
					doc.setPageSize(pageSize);
					doc.newPage();
				}
				float size = switch (h.getLevel()) { case 1 -> 17f; case 2 -> 13.5f; case 3 -> 11.5f; default -> 10.5f; };
				com.itextpdf.text.Paragraph p = paragraph(h, size, true);
				p.setSpacingBefore(h.getLevel() <= 2 ? 12f : 8f);
				p.setSpacingAfter(4f);
				doc.add(p);
			} else if (n instanceof BulletList list) {
				if (skipList) {
					skipList = false;
					continue;
				}
				for (Node item = list.getFirstChild(); item != null; item = item.getNext()) {
					com.itextpdf.text.Paragraph p = new com.itextpdf.text.Paragraph();
					p.add(new Chunk("•  ", font(9f, false, false)));
					inline(p, ((ListItem)item).getFirstChild(), 9f, false, false);
					p.setIndentationLeft(10f);
					doc.add(p);
				}
			} else if (n instanceof FencedCodeBlock code) {
				PdfPCell cell = new PdfPCell(new Phrase(code.getLiteral().stripTrailing(),
						new Font(Font.FontFamily.COURIER, 7.5f)));
				cell.setBackgroundColor(CODE_BG);
				cell.setBorderColor(RULE);
				cell.setPadding(6f);
				PdfPTable box = new PdfPTable(1);
				box.setWidthPercentage(100);
				box.addCell(cell);
				box.setSpacingBefore(4f);
				box.setSpacingAfter(6f);
				doc.add(box);
			} else if (n instanceof TableBlock table) {
				doc.add(table(table));
			} else if (n instanceof Paragraph para && para.getFirstChild() instanceof Image img
					&& img.getNext() == null) {
				// a figure on its own: full text width
				com.itextpdf.text.Image image = image(img);
				image.scaleToFit(doc.getPageSize().getWidth() - doc.leftMargin() - doc.rightMargin(), 10000f);
				image.setAlignment(Element.ALIGN_CENTER);
				image.setSpacingAfter(6f);
				doc.add(image);
			} else if (n instanceof Paragraph para) {
				com.itextpdf.text.Paragraph p = paragraph(para, 9.5f, false);
				p.setSpacingAfter(6f);
				p.setLeading(13f);
				doc.add(p);
			}
		}
	}

	private com.itextpdf.text.Paragraph paragraph(Node parent, float size, boolean bold) {
		com.itextpdf.text.Paragraph p = new com.itextpdf.text.Paragraph();
		inline(p, parent, size, bold, false);
		return p;
	}

	private static Font font(float size, boolean bold, boolean italic) {
		int style = (bold ? Font.BOLD : 0) | (italic ? Font.ITALIC : 0);
		return new Font(Font.FontFamily.HELVETICA, size, style == 0 ? Font.NORMAL : style);
	}

	private void inline(Phrase phrase, Node parent, float size, boolean bold, boolean italic) {
		for (Node n = parent.getFirstChild(); n != null; n = n.getNext()) {
			if (n instanceof Text t) {
				Chunk chunk = new Chunk(t.getLiteral(), font(sub || sup ? size*0.7f : size, bold, italic));
				if (sub)
					chunk.setTextRise(-size*0.2f);
				else if (sup)
					chunk.setTextRise(size*0.35f);
				phrase.add(chunk);
			} else if (n instanceof Code c) {
				phrase.add(new Chunk(c.getLiteral(), new Font(Font.FontFamily.COURIER, size*0.92f)));
			} else if (n instanceof StrongEmphasis) {
				inline(phrase, n, size, true, italic);
			} else if (n instanceof Emphasis) {
				inline(phrase, n, size, bold, true);
			} else if (n instanceof Link) {
				inline(phrase, n, size, bold, italic);
			} else if (n instanceof HtmlInline html) {
				if (html.getLiteral().equalsIgnoreCase("<sub>"))
					sub = true;
				else if (html.getLiteral().equalsIgnoreCase("</sub>"))
					sub = false;
				else if (html.getLiteral().equalsIgnoreCase("<sup>"))
					sup = true;
				else if (html.getLiteral().equalsIgnoreCase("</sup>"))
					sup = false;
			} else if (n instanceof SoftLineBreak) {
				phrase.add(new Chunk(" ", font(size, bold, italic)));
			} else if (n instanceof HardLineBreak) {
				phrase.add(Chunk.NEWLINE);
			}
		}
	}

	private PdfPTable table(TableBlock block) throws IOException, DocumentException {
		int cols = 0;
		TableRow first = (TableRow)block.getFirstChild().getFirstChild();
		for (Node c = first.getFirstChild(); c != null; c = c.getNext())
			cols++;
		// wide statistics tables need a small font on portrait pages
		float size = cols > 6 && doc.getPageSize().getWidth() < doc.getPageSize().getHeight() ? 6.8f : 8.5f;
		PdfPTable table = new PdfPTable(cols);
		table.setWidthPercentage(100);
		table.setSpacingBefore(4f);
		table.setSpacingAfter(8f);
		// map and chart tables: the first row is a label row like the others, not a header
		boolean images = hasImage(block);
		if (!images)
			table.setHeaderRows(1);
		int rows = 0;
		for (Node section = block.getFirstChild(); section != null; section = section.getNext())
			for (Node row = section.getFirstChild(); row != null; row = row.getNext())
				rows++;
		// short tables move to the next page rather than splitting
		table.setKeepTogether(rows <= 12);
		int rowIndex = 0;
		for (Node section = block.getFirstChild(); section != null; section = section.getNext()) {
			boolean head = section instanceof TableHead && !images;
			for (Node row = section.getFirstChild(); row != null; row = row.getNext(), rowIndex++) {
				for (Node c = row.getFirstChild(); c != null; c = c.getNext()) {
					PdfPCell cell = cell((TableCell)c, size, head);
					if (head)
						cell.setBackgroundColor(HEADER_BG);
					else if (rowIndex % 2 == 0 && !images)
						cell.setBackgroundColor(STRIPE_BG);
					cell.setBorderColor(RULE);
					table.addCell(cell);
				}
			}
		}
		return table;
	}

	private PdfPCell cell(TableCell c, float size, boolean head) throws IOException, DocumentException {
		Node child = c.getFirstChild();
		if (child instanceof Image img) {
			// a map or chart, scaled to the cell width
			com.itextpdf.text.Image image = image(img);
			PdfPCell cell = new PdfPCell(image, true);
			cell.setPadding(2f);
			cell.setBackgroundColor(BaseColor.WHITE);
			return cell;
		}
		Phrase phrase = new Phrase();
		inline(phrase, c, size, head, false);
		if (head)
			for (Chunk chunk : phrase.getChunks())
				chunk.getFont().setColor(BaseColor.WHITE);
		PdfPCell cell = new PdfPCell(phrase);
		cell.setPadding(3f);
		cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
		return cell;
	}

	/**
	 * The image a Markdown image refers to: its vector PDF version when there is one, else the file itself
	 */
	private com.itextpdf.text.Image image(Image img) throws IOException, DocumentException {
		File file = new File(baseDir, img.getDestination());
		File vector = new File(file.getParentFile(), file.getName().replaceFirst("\\.png$", ".pdf"));
		if (vector.exists())
			return com.itextpdf.text.Image.getInstance(writer.getImportedPage(new PdfReader(vector.getAbsolutePath()), 1));
		return com.itextpdf.text.Image.getInstance(file.getAbsolutePath());
	}

	private static boolean hasImage(Node node) {
		for (Node n = node.getFirstChild(); n != null; n = n.getNext())
			if (n instanceof Image || hasImage(n))
				return true;
		return false;
	}

	private static String plainText(Node node) {
		StringBuilder sb = new StringBuilder();
		for (Node n = node.getFirstChild(); n != null; n = n.getNext()) {
			if (n instanceof Text t)
				sb.append(t.getLiteral());
			else if (n instanceof Code c)
				sb.append(c.getLiteral());
			else
				sb.append(plainText(n));
		}
		return sb.toString();
	}
}
