package org.opensha.sha.cybershake.plot;

import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.sha.cybershake.db.CybershakeSite;

public class ScatterSymbol {
	
	public static final String SYMBOL_SQUARE = PSXYSymbol.Symbol.SQUARE.val();
	public static final String SYMBOL_DIAMOND = PSXYSymbol.Symbol.DIAMOND.val();
	public static final String SYMBOL_CIRCLE = PSXYSymbol.Symbol.CIRCLE.val();
	public static final String SYMBOL_STAR = PSXYSymbol.Symbol.STAR.val();
	public static final String SYMBOL_OCTAGON = PSXYSymbol.Symbol.OCTAGON.val();
	public static final String SYMBOL_HEXAGON = PSXYSymbol.Symbol.HEXAGON.val();
	public static final String SYMBOL_INVERTED_TRIANGLE = PSXYSymbol.Symbol.INVERTED_TRIANGLE.val();
	public static final String SYMBOL_PENTAGON = PSXYSymbol.Symbol.PENTAGON.val();
	public static final String SYMBOL_CROSS = PSXYSymbol.Symbol.CROSS.val();
	public static final String SYMBOL_Y_DASH = PSXYSymbol.Symbol.Y_DASH.val();
	
	public static final String SYMBOL_INVISIBLE = "DO NOT DISPLAY";
	
	String sym;
	
	int siteTypeID;
	
	double scale = 1;
	
	public ScatterSymbol(String sym, int siteTypeID, double scale) {
		this.sym = sym;
		this.siteTypeID = siteTypeID;
		this.scale = scale;
	}
	
	public ScatterSymbol(String sym, int siteTypeID) {
		this(sym, siteTypeID, 1d);
	}
	
	public boolean use(CybershakeSite site) {
		return site.type_id == siteTypeID;
	}
	
	public String getSymbol() {
		return sym;
	}
	
	public void setSymbol(String symbol) {
		this.sym = symbol;
	}
	
	public int getSiteTypeID() {
		return siteTypeID;
	}
	
	public double getScaleFactor() {
		return scale;
	}
}
