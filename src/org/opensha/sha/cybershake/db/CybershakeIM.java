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

package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.opensha.commons.util.ClassUtils;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.OtherParams.ComponentParam;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class CybershakeIM implements Comparable<CybershakeIM> {
	
	public enum IMType implements DBField {
		SA("spectral acceleration", "SA");
		
		private String dbName, shortName;
		private IMType(String dbName, String shortName) {
			this.dbName = dbName;
			this.shortName = shortName;
		}
		@Override
		public String getDBName() {
			return dbName;
		}
		@Override
		public String getShortName() {
			return shortName;
		}
		@Override
		public String toString() {
			return getShortName();
		}
	}
	
	public enum CyberShakeComponent implements DBField {
		GEOM_MEAN("geometric mean", "GEOM", Component.AVE_HORZ, Component.GMRotI50),
		X("X", "X", Component.RANDOM_HORZ),
		Y("Y", "Y", Component.RANDOM_HORZ),
		RotD100("RotD100", "RotD100", Component.RotD100),
		RotD50("RotD50", "RotD50", Component.RotD50);

		private String dbName, shortName;
		// supported GMPE components
		private Component[] gmpeComponents;
		private CyberShakeComponent(String dbName, String shortName, Component... gmpeComponents) {
			this.dbName = dbName;
			this.shortName = shortName;
			this.gmpeComponents = gmpeComponents;
			Preconditions.checkNotNull(gmpeComponents);
		}
		@Override
		public String getDBName() {
			return dbName;
		}
		@Override
		public String getShortName() {
			return shortName;
		}
		@Override
		public String toString() {
			return getShortName();
		}
		public Component[] getGMPESupportedComponents() {
			return gmpeComponents;
		}
		public Component getSupportedComponent(ComponentParam compParam) {
			return getSupportedComponent(compParam.getConstraint().getAllowedValues());
		}
		public Component getSupportedComponent(List<Component> components) {
			for (Component gmpeComponent : gmpeComponents)
				if (components.contains(gmpeComponent))
					return gmpeComponent;
			// no supported
			return null;
		}
		public boolean isComponentSupported(Component component) {
			for (Component supported : gmpeComponents)
				if (supported == component)
					return true;
			return false;
		}
	}
	
	private interface DBField {
		public String getDBName();
		public String getShortName();
	}
	
	public static <E extends Enum<E>> E fromDBField(String dbName, Class<E> clazz) {
		for (E e : clazz.getEnumConstants()) {
			Preconditions.checkState(e instanceof DBField);
			DBField db = (DBField)e;
			if (db.getDBName().equals(dbName.trim()))
				return e;
		}
		throw new IllegalStateException("DB field for type "
				+ClassUtils.getClassNameWithoutPackage(clazz)+" not found: "+dbName);
	}
	
	public static <E extends Enum<E>> E fromShortName(String shortName, Class<E> clazz) {
		for (E e : clazz.getEnumConstants()) {
			Preconditions.checkState(e instanceof DBField);
			DBField db = (DBField)e;
			if (db.getShortName().toLowerCase().equals(shortName.trim().toLowerCase()))
				return e;
		}
		throw new IllegalArgumentException("Short name for type "
				+ClassUtils.getClassNameWithoutPackage(clazz)+" not found: "+shortName);
	}
	
	public static <E extends Enum<E>> List<String> getShortNames(Class<E> clazz) {
		List<String> names = Lists.newArrayList();
		for (E e : clazz.getEnumConstants()) {
			Preconditions.checkState(e instanceof DBField);
			DBField db = (DBField)e;
			names.add(db.getShortName());
		}
		return names;
	}
	
	private int id;
	private IMType measure;
	private double val;
	private String units;
	private CyberShakeComponent component;
	
	public CybershakeIM(int id, IMType measure, double val, String units, CyberShakeComponent component) {
		this.id = id;
		this.measure = measure;
		this.val = val;
		this.units = units;
		this.component = component;
	}

	public int getID() {
		return id;
	}

	public IMType getMeasure() {
		return measure;
	}
	
	public CyberShakeComponent getComponent() {
		return component;
	}

	public double getVal() {
		return val;
	}

	public String getUnits() {
		return units;
	}
	
	public String toString() {
		return this.measure+" ("+component+"): "+this.val+" ("+this.units+")";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CybershakeIM other = (CybershakeIM) obj;
		if (id != other.id)
			return false;
		return true;
	}

	public int compareTo(CybershakeIM im) {
		int ret = measure.compareTo(im.measure);
		if (ret != 0)
			return ret;
		ret = component.compareTo(im.component);
		if (ret != 0)
			return ret;
		return Double.compare(val, im.val);
	}
	
	public static CybershakeIM fromResultSet(ResultSet rs) throws SQLException {
		int id = rs.getInt("IM_Type_ID");
		String measureStr = rs.getString("IM_Type_Measure");
		IMType measure = fromDBField(measureStr, IMType.class);
		Double value = rs.getDouble("IM_Type_Value");
		String units = rs.getString("Units");
		String componentStr = rs.getString("IM_Type_Component");
		CyberShakeComponent component = fromDBField(componentStr, CyberShakeComponent.class);
		return new CybershakeIM(id, measure, value, units, component);
	}
}
