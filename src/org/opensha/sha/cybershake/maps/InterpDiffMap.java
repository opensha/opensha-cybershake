package org.opensha.sha.cybershake.maps;

import java.awt.Color;

import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.util.cpt.CPT;

public class InterpDiffMap extends GMT_Map {
	
	public enum InterpDiffMapType {
		BASEMAP("basemap", null),
		INTERP_NOMARKS("interpolated", null),
		INTERP_MARKS("interpolated_marks", Color.WHITE),
		DIFF("diff", Color.BLACK),
		RATIO("ratio", Color.BLACK);
		
		private String prefix;
		private Color markerColor;
		
		private InterpDiffMapType(String prefix, Color markerColor) {
			this.prefix = prefix;
			this.markerColor = markerColor;
		}
		
		public String getPrefix() {
			return prefix;
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
