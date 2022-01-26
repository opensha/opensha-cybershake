package org.opensha.sha.cybershake.gui.util;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.swing.JPanel;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.opensha.commons.util.DevStatus;
import org.opensha.sha.earthquake.ERF_Ref;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.gui.beans.ERF_GuiBean;


/**
 * This is a class for quickly saving an XML representation of an ERF
 * 
 * @author kevin
 *
 */
public class ERFSaver extends XMLSaver {
	
	private ERF_GuiBean bean;
	
	public ERFSaver() {
		super();
		bean = createERF_GUI_Bean();
		super.init();
	}
	
	private ERF_GuiBean createERF_GUI_Bean() {
		try{
			return new ERF_GuiBean(ERF_Ref.get(false, DevStatus.PRODUCTION, DevStatus.DEVELOPMENT, DevStatus.EXPERIMENTAL, DevStatus.DEPRECATED));
		}catch(InvocationTargetException e){
			throw new RuntimeException("Connection to ERF servlets failed");
		}
	}

	@Override
	public JPanel getPanel() {
		return bean;
	}

	@Override
	public Element getXML(Element root) {
		try {
			AbstractERF erf = (AbstractERF)bean.getSelectedERF_Instance();
			
			return erf.toXMLMetadata(root);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static AbstractERF LOAD_ERF_FROM_FILE(URL file) throws DocumentException, InvocationTargetException, MalformedURLException {
		SAXReader reader = new SAXReader();
		
		Document doc = reader.read(file);
		
		Element el = doc.getRootElement().element(AbstractERF.XML_METADATA_NAME);
		
		return AbstractERF.fromXMLMetadata(el);
	}
	
	public static AbstractERF LOAD_ERF_FROM_FILE(String fileName) throws DocumentException, InvocationTargetException, MalformedURLException {
		return LOAD_ERF_FROM_FILE(new File(fileName).toURI().toURL());
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ERFSaver saver = new ERFSaver();
		saver.setVisible(true);
	}

}
