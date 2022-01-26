package org.opensha.sha.cybershake.plot;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.opensha.commons.data.function.AbstractDiscretizedFunc;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.gui.plot.PlotColorAndLineTypeSelectorControlPanel;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.metadata.XMLSaveable;
import org.opensha.commons.util.XMLUtils;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.gui.infoTools.IMT_Info;

public class HazardCurvePlotCharacteristics implements XMLSaveable {
	
	public static final String XML_METADATA_NAME = "HazardCurvePlotCharacteristics";
	
	public static final String SHORT_NAME = "%s";
	public static final String LONG_NAME = "%l";
	public static final String COMBINED_NAME = "%n";
	public static final String PERIOD = "%p";
	
	public static HazardCurvePlotCharacteristics createRobPlotChars() {
		double xMin = 0;
		double xMax = 2;
		double yMin = Double.parseDouble("1.0E-6");
		double yMax = 1;
		
		boolean xLog = false;
		boolean yLog = true;
		
		HazardCurvePlotCharacteristics chars = new HazardCurvePlotCharacteristics(xMin, xMax, yMin, yMax, xLog, yLog);
		
		chars.setCyberShakeColor(null);
		
		chars.addPeriodDependantXMax(3d, 2d);
		chars.addPeriodDependantXMax(5d, 1d);
		chars.addPeriodDependantXMax(10d, 0.5);
		
		return chars;
	}
	
	public static HazardCurvePlotCharacteristics createTomPlotChars() {
		double xMin = Double.parseDouble("1.0E-2");
		double xMax = 3;
		double yMin = Double.parseDouble("1.0E-5");
		double yMax = 0.2;
		
		boolean xLog = true;
		boolean yLog = true;
		
		HazardCurvePlotCharacteristics chars = new HazardCurvePlotCharacteristics(xMin, xMax, yMin, yMax, xLog, yLog);
		
		chars.setCyberShakeColor(null);
		
//		ArrayList<Color> attenRelColors = new ArrayList<Color>();
//		attenRelColors.add(new Color(0, 100, 0));
//		attenRelColors.add(Color.BLACK);
//		attenRelColors.add(Color.YELLOW);
//		attenRelColors.add(Color.GREEN);
//		attenRelColors.add(Color.BLUE);
//		
//		chars.setAttenRelColors(attenRelColors);
		
		chars.addPeriodDependantXMax(3d, 3d);
		chars.addPeriodDependantXMax(5d, 1.5d);
		chars.addPeriodDependantXMax(10d, 1d);
		
		return chars;
	}
	
	enum CyberShakeColors {
		// "noramal colors"
		BLACK("Black",				Color.BLACK),
		BLUE("Blue",				Color.BLUE),
		CYAN("Cyan",				Color.CYAN),
		DARK_GRAY("Dark Gray",		Color.DARK_GRAY),
		GRAY("Gray",				Color.GRAY),
		GREEN("Green",				Color.GREEN),
		LIGHT_GRAY("Light Gray",	Color.LIGHT_GRAY),
		MAGENTA("Magenta",			Color.MAGENTA),
		ORANGE("Orange",			Color.ORANGE),
		PINK("Pink",				Color.PINK),
		RED("Red",					Color.RED),
		WHITE("White",				Color.WHITE),
		YELLOW("Yellow",			Color.YELLOW),
		
		// "custom colors"
		LIGHT_RED("Light Red",			new Color(255, 150, 150)),
		LIGHT_BLUE("Light Blue",		new Color(150, 150, 255)),
		LIGHT_GREEN("Light Green",		new Color(150, 255, 150)),
		LIGHT_ORANGE("Light Orange",	new Color(255, 180, 35)),
		LIGHT_CYAN("Light Cyan",		new Color(160, 255, 255));
		
		private Color color;
		private String name;
		private CyberShakeColors(String name, Color color) {
			this.name = name;
			this.color = color;
		}
		
		@Override
		public String toString() {
			return name;
		}
		
		public Color getColor() {
			return color;
		}
		
		public static CyberShakeColors forColor(Color c) {
			for (CyberShakeColors cs : values()) {
				if (c.equals(cs.getColor()))
					return cs;
			}
			return null;
		}
	}
	
	public static String getColorName(Color c) {
		CyberShakeColors cs = CyberShakeColors.forColor(c);
		if (cs == null)
			return "r: "+c.getRed()+", g: "+c.getGreen()+", b: "+c.getBlue();
		else
			return cs.toString();
	}
	
	public static ArrayList<Color> getDefaultColors() {
		ArrayList<Color> attenRelColors = new ArrayList<Color>();
		
		attenRelColors.add(CyberShakeColors.LIGHT_RED.color); // light red
		attenRelColors.add(CyberShakeColors.LIGHT_BLUE.color); // light blue
		attenRelColors.add(CyberShakeColors.LIGHT_GREEN.color); // light green
		attenRelColors.add(CyberShakeColors.LIGHT_ORANGE.color); // light orange
		attenRelColors.add(CyberShakeColors.LIGHT_CYAN.color); // light cyan
		
//		attenRelColors.add(Color.blue);
//		attenRelColors.add(Color.green);
//		attenRelColors.add(Color.orange);
//		attenRelColors.add(Color.CYAN);
//		attenRelColors.add(Color.MAGENTA);
		
		return attenRelColors;
	}
	
	private ArrayList<Color> attenRelColors;
	
	private double xMin = 0;
	private double xMax = 10;
	private double yMin = 0;
	private double yMax = 1;
	
	private boolean xLog = false;
	private boolean yLog = false;
	
	private boolean customAxis = true;
	
	private String title = COMBINED_NAME;
	
	private String xAxisLabel = PERIOD + "s SA (g)";
	private String yAxisLabel = "Probability Rate (1/yr)";
	
	// this can be null now, and if so, color will be set by velocity model
	private Color cyberShakeColor = null;
	
	// default to null now, as it can be set from rup var scen ID
	private PlotLineType cyberShakeLineType = PlotLineType.SOLID;
	private PlotSymbol cyberShakeSymbol = null;
	
	private float csLineWidth = 2f;
	private float attenRelLineWidth = 2f;
	
	private PlotLineType attenRelLineType = PlotLineType.DASHED;
	private PlotSymbol attenRelSymbol = null;
	
	private ArrayList<double[]> periodDependantXMaxes = new ArrayList<double[]>();
	
	private ArbitrarilyDiscretizedFunc hazardFunc = null;

	public HazardCurvePlotCharacteristics() {
		this.customAxis = false;
	}
	
	public HazardCurvePlotCharacteristics(double xMin, double xMax, double yMin, double yMax, boolean xLog, boolean yLog) {
		this.attenRelColors = getDefaultColors();
		
		this.customAxis = true;
		
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;
		
		this.xLog = xLog;
		this.yLog = yLog;
		
	}
	
	public ArrayList<Color> getAttenRelColors() {
		return attenRelColors;
	}

	public void setAttenRelColors(ArrayList<Color> attenRelColors) {
		this.attenRelColors = attenRelColors;
	}
	
	public void addPeriodDependantXMax(double period, double xMax) {
		double it[] = {period, xMax};
		
		this.periodDependantXMaxes.add(it);
	}
	
	public double getXMin() {
		return xMin;
	}

	public void setXMin(double min) {
		xMin = min;
	}

	public double getXMax() {
		return xMax;
	}
	
	public double getXMax(double period) {
		int perInt = (int)(period * 100 + 0.5);
		for (double[] perMax : periodDependantXMaxes) {
			int perMatch = (int)(perMax[0] * 100 + 0.5);
			if (perInt == perMatch) {
				System.out.println("Matched xMax of " + perMax[1] + " with period=" + period);
				return perMax[1];
			}
		}
		return xMax;
	}

	public void setXMax(double max) {
		xMax = max;
	}

	public double getYMin() {
		return yMin;
	}

	public void setYMin(double min) {
		yMin = min;
	}

	public double getYMax() {
		return yMax;
	}

	public void setYMax(double max) {
		yMax = max;
	}

	public boolean isXLog() {
		return xLog;
	}

	public void setXLog(boolean log) {
		xLog = log;
	}

	public boolean isYLog() {
		return yLog;
	}

	public void setYLog(boolean log) {
		yLog = log;
	}

	public boolean isCustomAxis() {
		return customAxis;
	}

	public void setCustomAxis(boolean customAxis) {
		this.customAxis = customAxis;
	}
	
	public float getLineWidth() {
		return csLineWidth;
	}

	public void setCsLineWidth(float csLineWidth) {
		this.csLineWidth = csLineWidth;
	}

	public float getAttenRelLineWidth() {
		return attenRelLineWidth;
	}

	public void setAttenRelLineWidth(float attenRelLineWidth) {
		this.attenRelLineWidth = attenRelLineWidth;
	}

	public static String getReplacedTitle(String title, CybershakeSite site) {
		title = title.replace(COMBINED_NAME, site.getFormattedName());
		title = title.replace(SHORT_NAME, site.short_name);
		title = title.replace(LONG_NAME, site.name);
		
		return title;
	}
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public static String getReplacedXAxisLabel(String xAxisLabel, double period) {
		return xAxisLabel.replace(PERIOD, HazardCurvePlotter.getPeriodStr(period));
	}

	public String getXAxisLabel() {
		return xAxisLabel;
	}

	public void setXAxisLabel(String axisLabel) {
		xAxisLabel = axisLabel;
	}

	public String getYAxisLabel() {
		return yAxisLabel;
	}

	public void setYAxisLabel(String axisLabel) {
		yAxisLabel = axisLabel;
	}

	public Color getCyberShakeColor() {
		return cyberShakeColor;
	}

	public void setCyberShakeColor(Color cyberShakeColor) {
		this.cyberShakeColor = cyberShakeColor;
	}

	public PlotLineType getCyberShakeLineType() {
		return cyberShakeLineType;
	}

	public void setCyberShakeLineType(PlotLineType cyberShakeLineType) {
		this.cyberShakeLineType = cyberShakeLineType;
	}
	
	public PlotLineType getAttenRelLineType() {
		return attenRelLineType;
	}

	public void setAttenRelLineType(PlotLineType attenRelLineType) {
		this.attenRelLineType = attenRelLineType;
	}
	
	public PlotSymbol getCyberShakeSymbol() {
		return cyberShakeSymbol;
	}

	public void setCyberShakeSymbol(PlotSymbol cyberShakeSymbol) {
		this.cyberShakeSymbol = cyberShakeSymbol;
	}

	public PlotSymbol getAttenRelSymbol() {
		return attenRelSymbol;
	}

	public void setAttenRelSymbol(PlotSymbol attenRelSymbol) {
		this.attenRelSymbol = attenRelSymbol;
	}

	public ArbitrarilyDiscretizedFunc getHazardFunc() {
		if (hazardFunc == null)
			hazardFunc = IMT_Info.getUSGS_PGA_Function();
		return hazardFunc;
	}

	public void setHazardFunc(ArbitrarilyDiscretizedFunc hazardFunc) {
		this.hazardFunc = hazardFunc;
	}

	/**
	 * @param args
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws URISyntaxException {
		File robXML = new File(HazardCurvePlotCharacteristics.class.getResource(
				"/org/opensha/sha/cybershake/conf/robPlot.xml").toURI());
		File tomXML = new File(HazardCurvePlotCharacteristics.class.getResource(
				"/org/opensha/sha/cybershake/conf/tomPlot.xml").toURI());
		
		robXML = new File(robXML.getAbsolutePath().replaceAll("classes", "src"));
		tomXML = new File(tomXML.getAbsolutePath().replaceAll("classes", "src"));
		
		HazardCurvePlotCharacteristics chars = createRobPlotChars();
		try {
			System.out.println("Writing: "+robXML.getAbsolutePath());
			XMLUtils.writeObjectToXMLAsRoot(chars, robXML);
			
			chars = createTomPlotChars();
			System.out.println("Writing: "+tomXML.getAbsolutePath());
			XMLUtils.writeObjectToXMLAsRoot(chars, tomXML);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.exit(0);
	}
	
	

	public Element toXMLMetadata(Element root) {
		Element el = root.addElement(XML_METADATA_NAME);
		
		el.addAttribute("xMin", xMin + "");
		el.addAttribute("xMax", xMax + "");
		el.addAttribute("yMin", yMin + "");
		el.addAttribute("yMax", yMax + "");
		
		el.addAttribute("xLog", xLog + "");
		el.addAttribute("yLog", yLog + "");
		
		if (getCyberShakeColor() != null)
			XMLUtils.colorToXML(el, this.getCyberShakeColor(), "CyberShakeColor");
		
		Element attenRelEl = el.addElement("AttenRelColors");
		
		for (Color color : this.getAttenRelColors()) {
			XMLUtils.colorToXML(attenRelEl, color);
		}
		
		Element periodDepXMaxEl = el.addElement("PeriodDependentXMaxes");
		
		for (double[] depMax : this.periodDependantXMaxes) {
			Element per = periodDepXMaxEl.addElement("PeriodDependentXMax");
			
			per.addAttribute("period", depMax[0] + "");
			per.addAttribute("xMax", depMax[1] + "");
		}
		
		el.addAttribute("title", this.getTitle());
		el.addAttribute("xAxisLabel", this.getXAxisLabel());
		el.addAttribute("yAxisLabel", this.getYAxisLabel());
		
		el.addAttribute("csLineWidth", csLineWidth+"");
		el.addAttribute("attenRelLineWidth", attenRelLineWidth+"");
		
		el.addAttribute("customAxis", this.isCustomAxis() + "");
		
		if (cyberShakeLineType != null)
			el.addAttribute("cyberShakeLineType", this.cyberShakeLineType.toString());
		if (attenRelLineType != null)
			el.addAttribute("attenRelLineType", this.attenRelLineType.toString());
		if (cyberShakeSymbol != null)
			el.addAttribute("cyberShakeSymbol", this.cyberShakeSymbol.toString());
		if (attenRelSymbol != null)
			el.addAttribute("attenRelSymbol", this.attenRelSymbol.toString());
		
		ArbitrarilyDiscretizedFunc func = this.getHazardFunc();
		
		el = func.toXMLMetadata(el);
		
		return el;
	}
	
	public static HazardCurvePlotCharacteristics fromXMLMetadata(String xmlFile) throws MalformedURLException, DocumentException {
		SAXReader reader = new SAXReader();
        Document document = reader.read(new File(xmlFile));
        
        Element el = document.getRootElement();
        
        Element charsEl = el.element(XML_METADATA_NAME);
        
        return fromXMLMetadata(charsEl);
	}
	
	public static HazardCurvePlotCharacteristics fromXMLMetadata(Element charsEl) {
		double xMin = Double.parseDouble(charsEl.attributeValue("xMin"));
		double xMax = Double.parseDouble(charsEl.attributeValue("xMax"));
		double yMin = Double.parseDouble(charsEl.attributeValue("yMin"));
		double yMax = Double.parseDouble(charsEl.attributeValue("yMax"));
		
		boolean xLog = Boolean.parseBoolean(charsEl.attributeValue("xLog"));
		boolean yLog = Boolean.parseBoolean(charsEl.attributeValue("yLog"));
		
		HazardCurvePlotCharacteristics chars = new HazardCurvePlotCharacteristics(xMin, xMax, yMin, yMax, xLog, yLog);
		
		Element csColorEl = charsEl.element("CyberShakeColor");
		if (csColorEl == null)
			chars.setCyberShakeColor(null);
		else
			chars.setCyberShakeColor(XMLUtils.colorFromXML(csColorEl));
		
		Iterator<Element> attenRelColorsIt = charsEl.element("AttenRelColors").elementIterator();
		
		ArrayList<Color> attenRelColors = new ArrayList<Color>();
		while (attenRelColorsIt.hasNext()) {
			Element colorEl = attenRelColorsIt.next();
			attenRelColors.add(XMLUtils.colorFromXML(colorEl));
		}
		
		Iterator<Element> periodDependantXMaxesIt = charsEl.element("PeriodDependentXMaxes").elementIterator();
		
		while (periodDependantXMaxesIt.hasNext()) {
			Element perEl = periodDependantXMaxesIt.next();
			double period = Double.parseDouble(perEl.attributeValue("period"));
			double xMaxPer = Double.parseDouble(perEl.attributeValue("xMax"));
			chars.addPeriodDependantXMax(period, xMaxPer);
		}
		
		chars.setTitle(charsEl.attributeValue("title"));
		chars.setXAxisLabel(charsEl.attributeValue("xAxisLabel"));
		chars.setYAxisLabel(charsEl.attributeValue("yAxisLabel"));
		
		float csLineWidth = Float.parseFloat(charsEl.attribute("csLineWidth").getStringValue());
		float attenRelLineWidth = Float.parseFloat(charsEl.attribute("attenRelLineWidth").getStringValue());
		chars.setCsLineWidth(csLineWidth);
		chars.setAttenRelLineWidth(attenRelLineWidth);
		
		Attribute csLineTypeAtt = charsEl.attribute("cyberShakeLineType");
		if (csLineTypeAtt != null)
			chars.setCyberShakeLineType(PlotLineType.forString(csLineTypeAtt.getStringValue()));
		Attribute attenRelLineTypeAtt = charsEl.attribute("attenRelLineType");
		if (attenRelLineTypeAtt != null)
			chars.setAttenRelLineType(PlotLineType.forString(attenRelLineTypeAtt.getStringValue()));
		Attribute csSymbolAtt = charsEl.attribute("cyberShakeSymbol");
		if (csSymbolAtt != null)
			chars.setCyberShakeSymbol(PlotSymbol.forString(csSymbolAtt.getStringValue()));
		Attribute attenRelSymbolAtt = charsEl.attribute("attenRelSymbol");
		if (attenRelSymbolAtt != null)
			chars.setAttenRelSymbol(PlotSymbol.forString(attenRelSymbolAtt.getStringValue()));
		
		Element funcEl = charsEl.element(ArbitrarilyDiscretizedFunc.XML_METADATA_NAME);
		
		ArbitrarilyDiscretizedFunc arbDiscrFunc;
		AbstractDiscretizedFunc func = ArbitrarilyDiscretizedFunc.fromXMLMetadata(funcEl);
		if (func instanceof ArbitrarilyDiscretizedFunc)
			arbDiscrFunc = (ArbitrarilyDiscretizedFunc)func;
		else
			arbDiscrFunc = new ArbitrarilyDiscretizedFunc(func);
		
		chars.setHazardFunc(arbDiscrFunc);
		
		return chars;
	}

}
