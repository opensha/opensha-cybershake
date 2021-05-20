package org.opensha.sha.cybershake.maps;

import java.awt.Color;

import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.util.cpt.CPT;

public class InterpDiffMap extends GMT_Map {
	
	public enum InterpDiffMapType {
		BASEMAP("basemap", "GMPE Basemap", null),
		INTERP_NOMARKS("interpolated", "Interpolated CyberShake Map", null),
		INTERP_MARKS("interpolated_marks", "Interpolated CyberShake Map w/ Sites Marked", Color.WHITE),
		DIFF("diff", "Difference: CyberShake - GMPE Basemap", Color.BLACK),
		RATIO("ratio", "Ratio: CyberShake / GMPE Basemap", Color.BLACK);
		
		private String prefix;
		private String name;
		private Color markerColor;
		
		private InterpDiffMapType(String prefix, String name, Color markerColor) {
			this.prefix = prefix;
			this.name = name;
			this.markerColor = markerColor;
		}
		
		public String getPrefix() {
			return prefix;
		}
		
		public String getName() {
			return name;
		}
		
		public Color getMarkerColor() {
			return markerColor;
		}
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private GeoDataSet scatter;
	private InterpDiffMapType[] mapTypes;
	
	private boolean useCPTForScatterColor = false;
	private boolean autoLabel = false;
	
	public InterpDiffMap(Region region, GeoDataSet baseMap, double basemapInc, CPT cpt,
			GeoDataSet scatter, GMT_InterpolationSettings interpSettings,
			InterpDiffMapType[] mapTypes) {
		super(region, baseMap, basemapInc, cpt);
		this.scatter = scatter;
		this.mapTypes = mapTypes;
		this.setBlackBackground(false);
		this.setInterpSettings(interpSettings);
	}

	public GeoDataSet getScatter() {
		return scatter;
	}

	public void setScatter(GeoDataSet scatter) {
		this.scatter = scatter;
	}

	public InterpDiffMapType[] getMapTypes() {
		return mapTypes;
	}

	public void setMapTypes(InterpDiffMapType[] mapTypes) {
		this.mapTypes = mapTypes;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public boolean isUseCPTForScatterColor() {
		return useCPTForScatterColor;
	}

	public void setUseCPTForScatterColor(boolean useCPTForScatterColor) {
		this.useCPTForScatterColor = useCPTForScatterColor;
	}

	public boolean isAutoLabel() {
		return autoLabel;
	}

	public void setAutoLabel(boolean autoLabel) {
		this.autoLabel = autoLabel;
	}

}
