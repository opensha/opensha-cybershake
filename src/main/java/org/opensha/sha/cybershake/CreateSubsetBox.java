package org.opensha.sha.cybershake;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.commons.util.FileUtils;


public class CreateSubsetBox {

	Region region;
	public CreateSubsetBox(){
		LocationList locList = new LocationList();
		locList.add(new Location(31.082920,-116.032285));
		locList.add(new Location(33.122341,-113.943965));
		locList.add(new Location(36.621696,-118.9511292));
		locList.add(new Location(34.5,-121));
		region = new Region(locList, BorderType.MERCATOR_LINEAR);
		createSubsetFile();
	}
	
	
	public void createSubsetFile(){
		
		try {
			FileWriter fw = new FileWriter("RegionSubset.txt");
			ArrayList fileLines = FileUtils.loadFile("map_data.txt");
			int numLines = fileLines.size();
			for(int i=0;i<numLines;++i){
				StringTokenizer st = new StringTokenizer((String)fileLines.get(i));
				double lat = Double.parseDouble(st.nextToken().trim());
				double lon = Double.parseDouble(st.nextToken().trim());
				Location loc = new Location(lat,lon);
				if(region.contains(loc)){
					fw.write((String)fileLines.get(i)+"\n");
				}
			}
			fw.close();
				
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		new CreateSubsetBox();
	}
	
}
