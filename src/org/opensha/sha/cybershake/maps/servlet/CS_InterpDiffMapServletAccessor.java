package org.opensha.sha.cybershake.maps.servlet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.sha.cybershake.maps.InterpDiffMap;

public class CS_InterpDiffMapServletAccessor {
	
	public static String DEFAULT_METADATA_FILE_NAME = "metadata.txt";
	
	public static void checkLog(InterpDiffMap map) {
		if (map.isLogPlot()) {
			GeoDataSet gridded = map.getGriddedData();
			GeoDataSet scatter = map.getScatter();
			
			if (gridded != null) {
				gridded = gridded.copy();
				gridded.log10();
				map.setGriddedData(gridded);
			}
			if (scatter != null) {
				scatter = scatter.copy();
				scatter.log10();
				map.setScatter(scatter);
			}
		}
	}
	
	public static String makeMap(String dirName, InterpDiffMap map, String metadata) throws IOException, ClassNotFoundException {
		URL gmtMapServlet = new URL(CS_InterpDiffMapServlet.SERVLET_URL);
		
		checkLog(map);

		System.out.println("Initializing connection");
		URLConnection servletConnection = gmtMapServlet.openConnection();
		
		// inform the connection that we will send output and accept input
		servletConnection.setDoInput(true);
		servletConnection.setDoOutput(true);

		// Don't use a cached version of URL connection.
		servletConnection.setUseCaches (false);
		servletConnection.setDefaultUseCaches (false);
		// Specify the content type that we will send binary data
		servletConnection.setRequestProperty ("Content-Type","application/octet-stream");

		System.out.println("Initializing output to servlet");
		ObjectOutputStream outputToServlet = new
		ObjectOutputStream(servletConnection.getOutputStream());

		//sending the directory name to the servlet
		System.out.println("Writing dir name");
		outputToServlet.writeObject(dirName);

		//sending the map specification
		System.out.println("Writing map");
		outputToServlet.writeObject(map);

		//sending the contents of the Metadata file to the server.
		System.out.println("Writing metadata");
		outputToServlet.writeObject(metadata);

		//sending the name of the MetadataFile to the server.
		System.out.println("Writing metadata file name");
		outputToServlet.writeObject(DEFAULT_METADATA_FILE_NAME);

		System.out.println("Flushing/Closing");
		outputToServlet.flush();
		outputToServlet.close();

		// Receive the "actual webaddress of all the gmt related files"
		// from the servlet after it has received all the data
		System.out.println("Initializing input from servlet");
		ObjectInputStream inputToServlet = new
		ObjectInputStream(servletConnection.getInputStream());

		System.out.println("Receiving message");
		Object messageFromServlet = inputToServlet.readObject();
		System.out.println("Closing and returning");
		inputToServlet.close();
		if (messageFromServlet instanceof RuntimeException)
			throw (RuntimeException)messageFromServlet;
		return (String)messageFromServlet;
	}

}
