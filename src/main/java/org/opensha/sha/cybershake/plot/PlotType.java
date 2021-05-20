package org.opensha.sha.cybershake.plot;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public enum PlotType {
	PDF("pdf"),
	PNG("png"),
	JPG("jpg"),
	JPEG("jpeg"),
	TXT("txt"),
	CSV("csv");
	
	private String extension;
	
	private PlotType(String extention) {
		this.extension = extention;
	}
	
	public String getExtension() {
		return extension;
	}
	
	public static PlotType fromExtension(String extension) {
		extension = extension.toLowerCase();
		for (PlotType t : PlotType.values())
			if (t.getExtension().equals(extension))
				return t;
		throw new NoSuchElementException("Unknown extension: "+extension);
	}
	
	public static List<PlotType> fromExtensions(List<String> extensions) {
		return fromExtensions(extensions, null);
	}
	
	public static List<PlotType> fromExtensions(List<String> extensions, List<PlotType> allowed) {
		ArrayList<PlotType> types = new ArrayList<PlotType>();
		
		for (String extension : extensions) {
			PlotType type = fromExtension(extension);
			if (allowed == null || allowed.contains(type))
				types.add(type);
			else
				throw new IllegalArgumentException("Type not allowed: "+type.getExtension());
		}
		
		return types;
	}
}