/*******************************************************************************
 * Copyright 2009 OpenSHA.org in partnership with
 * the Southern California Earthquake Center (SCEC, http://www.scec.org)
 * at the University of Southern California and the UnitedStates Geological
 * Survey (USGS; http://www.usgs.gov)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.opensha.sha.cybershake.maps.servlet;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.opensha.commons.mapping.servlet.GMT_MapGeneratorServlet;
import org.opensha.commons.util.ServerPrefUtils;
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;
import org.opensha.sha.cybershake.maps.InterpDiffMap;

import com.google.common.base.Preconditions;



/**
 * <p>Title: GMT_MapGeneratorServlet </p>
 * <p>Description: this servlet runs the GMT script based on the parameters and generates the
 * image file and returns that back to the calling application applet </p>
 * 
 * * ****** NEW VERSION - more secure *******
 * This is the order of operations:
 * Client ==> Server:
 * * directory name (String), or null for auto dirname
 * * InterpDiff GMT Map specification (InterpDiffMap)
 * * Metadata (String)
 * * Metadata filename (String)
 * Server ==> Client:
 * * Directory URL path **OR** error message
 * 
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: </p>
 * @author :Nitin Gupta, Vipin Gupta, and Kevin Milner
 * @version 1.0
 */

public class CS_InterpDiffMapServlet
extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private static final File GMT_DATA_DIR = GMT_MapGeneratorServlet.GMT_DATA_DIR;
	
	public static final String SERVLET_URL = ServerPrefUtils.SERVER_PREFS.getServletBaseURL()
					+ "CS_InterpDiffMapServlet";
	
	private CyberShake_GMT_MapGenerator csGMT = new CyberShake_GMT_MapGenerator();

	//Process the HTTP Get request
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws
	ServletException, IOException {

		// get an ouput stream from the applet
		System.out.println("Initializing output to application");
		ObjectOutputStream outputToApplet = new ObjectOutputStream(response.
				getOutputStream());

		try {
			//create the main directory if it does not exist already
			if (!GMT_DATA_DIR.exists())
				GMT_DATA_DIR.mkdir();
			Preconditions.checkState(GMT_DATA_DIR.exists());

			// get an input stream from the applet
			System.out.println("Initializing input from application");
			ObjectInputStream inputFromApplet = new ObjectInputStream(request.
					getInputStream());

			//receiving the name of the input directory
			System.out.println("Receiving dir name");
			String dirName = (String) inputFromApplet.readObject();

			//gets the object for the GMT_MapGenerator script
			System.out.println("Receiving map");
			InterpDiffMap map = (InterpDiffMap)inputFromApplet.readObject();

			//Metadata content: Map Info
			System.out.println("Receiving metadata");
			String metadata = (String) inputFromApplet.readObject();

			//Name of the Metadata file
			System.out.println("Receiving metadata file name");
			String metadataFileName = (String) inputFromApplet.readObject();
			
			System.out.println("Generating map");
			System.out.flush();
			String mapImagePath = GMT_MapGeneratorServlet.createMap(csGMT, map, dirName, metadata, metadataFileName);
			System.out.println("Returning address: "+mapImagePath);
			
			//returns the URL to the folder where map image resides
			outputToApplet.writeObject(mapImagePath);
			
			outputToApplet.close();

		}catch (Throwable t) {
			//sending the error message back to the application
			outputToApplet.writeObject(new RuntimeException(t));
			outputToApplet.close();
		}
	}
	
	//Process the HTTP Post request
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws
	ServletException, IOException {
		// call the doPost method
		doGet(request, response);
	}

}
