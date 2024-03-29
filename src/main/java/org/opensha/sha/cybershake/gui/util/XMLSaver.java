package org.opensha.sha.cybershake.gui.util;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.dom4j.Document;
import org.dom4j.Element;
import org.opensha.commons.util.XMLUtils;

/**
 * Common parent class for a JFrame where a gui bean is shown, then saved to XML.
 * 
 * Useful for saving Attenuation Relationships or ERFs to XML.
 * 
 * @author kevin
 *
 */
public abstract class XMLSaver extends JFrame implements ActionListener {
	
	JPanel beanPanel;
	
	JPanel main = new JPanel(new BorderLayout());
	
	JPanel buttonPanel = new JPanel();
	
	JButton saveButton = new JButton("Save To XML");
	
	String fileName = "output.xml";
	
	boolean hideOnSave = false;
	
	JDialog diag = null;

	public XMLSaver() {
		super();
	}
	
	public void init() {
		this.beanPanel = this.getPanel();
		
		saveButton.addActionListener(this);
		
		buttonPanel.add(saveButton);
		
		main.add(this.beanPanel, BorderLayout.CENTER);
		main.add(buttonPanel, BorderLayout.SOUTH);
		
		this.setContentPane(main);
		
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setLocationRelativeTo(null);
		this.setSize(500, 700);
	}
	
	public JDialog getAsDialog() {
		if (diag == null) {
			diag = new JDialog();
			diag.setContentPane(main);
			diag.setSize(500, 700);
		}
		return diag;
	}

	public void actionPerformed(ActionEvent e) {
		if (e.getSource() == saveButton) {
			Document doc = XMLUtils.createDocumentWithRoot();
			Element el = getXML(doc.getRootElement());
			
			try {
				XMLUtils.writeDocumentToFile(new File(fileName), doc);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			if (hideOnSave) {
				this.setVisible(false);
				if (diag != null)
					diag.setVisible(false);
			}
		}
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public void setHideOnSave(boolean hide) {
		this.hideOnSave = hide;
	}
	
	public abstract Element getXML(Element root);
	
	public abstract JPanel getPanel();
}
