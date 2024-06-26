package org.opensha.sha.cybershake.gui.util;

import java.awt.BorderLayout;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.swing.JPanel;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.opensha.sha.gui.beans.IMR_GuiBean;
import org.opensha.sha.gui.beans.IMR_GuiBeanAPI;
import org.opensha.sha.gui.beans.IMT_GuiBean;
import org.opensha.sha.imr.AttenuationRelationship;
import org.opensha.sha.imr.AbstractIMR;
import org.opensha.sha.imr.ScalarIMR;


/**
 * This is a class for quickly saving an XML representation of an Attenuation Relationship.
 * 
 * @author kevin
 *
 */
public class AttenRelSaver extends XMLSaver implements IMR_GuiBeanAPI {
	
	private IMR_GuiBean bean;
	private IMT_GuiBean imtBean;
	
	public AttenRelSaver() {
		super();
		bean = createIMR_GUI_Bean();
		ScalarIMR imr = bean.getSelectedIMR_Instance();
		imtBean = new IMT_GuiBean(imr, imr.getSupportedIntensityMeasuresIterator());
		super.init();
	}
	
	private IMR_GuiBean createIMR_GUI_Bean() {
		return new IMR_GuiBean(this);
	}
	
	public ScalarIMR getSelectedAttenRel() {
		return bean.getSelectedIMR_Instance();
	}

	@Override
	public JPanel getPanel() {
		JPanel panel = new JPanel(new BorderLayout());
		panel.add(bean, BorderLayout.CENTER);
		panel.add(imtBean, BorderLayout.SOUTH);
		return panel;
	}

	@Override
	public Element getXML(Element root) {
		AttenuationRelationship attenRel = (AttenuationRelationship)bean.getSelectedIMR_Instance();
		attenRel.setIntensityMeasure(imtBean.getIntensityMeasure());
		
		return attenRel.toXMLMetadata(root);
	}
	
	public static AttenuationRelationship LOAD_ATTEN_REL_FROM_FILE(String fileName) throws DocumentException, InvocationTargetException, MalformedURLException {
		return LOAD_ATTEN_REL_FROM_FILE(new File(fileName).toURI().toURL());
	}
	
	public static AttenuationRelationship LOAD_ATTEN_REL_FROM_FILE(URL file) throws DocumentException, InvocationTargetException, MalformedURLException {
		SAXReader reader = new SAXReader();
		
		Document doc = reader.read(file);
		
		Element el = doc.getRootElement().element(AbstractIMR.XML_METADATA_NAME);
		
		return (AttenuationRelationship)AttenuationRelationship.fromXMLMetadata(el, null);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AttenRelSaver saver = new AttenRelSaver();
		saver.setVisible(true);
		
//		CY_2008_AttenRel cy08 = new CY_2008_AttenRel(new FakeParameterListener());
//		
//		cy08.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_1SIDED);
//		cy08.getParameter(SigmaTruncLevelParam.NAME).setValue(Double.valueOf(3.0));
//		cy08.getParameter(PeriodParam.NAME).setValue(Double.valueOf(3.0));
//		
//		
	}

	public void updateIM() {
		
	}

	public void updateSiteParams() {
		
	}

}
