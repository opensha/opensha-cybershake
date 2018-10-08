package org.opensha.sha.cybershake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.siteData.CachedSiteDataWrapper;
import org.opensha.commons.data.siteData.OrderedSiteDataProviderList;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.SiteDataValue;
import org.opensha.commons.data.siteData.SiteDataValueList;
import org.opensha.commons.data.siteData.impl.CVM4BasinDepth;
import org.opensha.commons.data.siteData.impl.CVM4i26BasinDepth;
import org.opensha.commons.data.siteData.impl.CVMHBasinDepth;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.data.siteData.impl.ConstantValueDataProvider;
import org.opensha.commons.data.siteData.impl.WaldAllenGlobalVs30;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.param.Parameter;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.imr.param.SiteParams.DepthTo1pt0kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.DepthTo2pt5kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;
import org.opensha.sha.imr.param.SiteParams.Vs30_TypeParam;

import com.google.common.base.Preconditions;

public class CyberShakeSiteBuilder {
	
	private static WillsMap2015 wills2015;
	private static WaldAllenGlobalVs30 wald2008;
	
	private Vs30_Source vs30Source;
	private int velModelID;
	
	private Double forceVs30 = null;
	
	private OrderedSiteDataProviderList mapBasinProvs = null;
	
	public static enum Vs30_Source {
		Wills2015("Wills 2015"),
		WaldAllen2008("Wald & Allen 2008"),
		Simulation("Simulation Value");
		
		private String name;
		
		private Vs30_Source(String name) {
			this.name = name;
		}
		
		public synchronized double getVs30(CybershakeRun run, Location siteLoc) {
			if (this == Wills2015) {
				try {
					if (wills2015 == null)
						wills2015 = new WillsMap2015();
					Double val = wills2015.getValue(siteLoc);
					if (!wills2015.isValueValid(val)) {
						System.out.println("Wills 2015 Vs30 is "+val+" for run "+run.getRunID()+" at "
								+siteLoc.getLatitude()+", "+siteLoc.getLongitude()+". Defaulting to Wald Allen 2008");
						val = WaldAllen2008.getVs30(run, siteLoc);
						if (!wald2008.isValueValid(val)) {
							System.out.println("No value for Wills or Wald, reverting to default of 760");
							val = 760d;
						}
					}
					return val;
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			} else if (this == WaldAllen2008) {
				try {
					if (wald2008 == null)
						wald2008 = new WaldAllenGlobalVs30();
					return wald2008.getValue(siteLoc);
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			} else if (this == Vs30_Source.Simulation) {
//				Double vs30 = run.getMeshVs30();
//				if (vs30 == null) {
//					System.err.println("Warning, mesh Vs30 not defined, using model Vs30");
//					vs30 = run.getModelVs30();
//					Preconditions.checkNotNull(vs30, "Neither mesh nor model Vs30 defined!");
//				}
				// TODO reverting to model Vs30 until Mesh Vs30 is fixed
				Double vs30 = run.getModelVs30();
				Preconditions.checkNotNull(vs30, "No model Vs30 defined!");
				Preconditions.checkState(Double.isFinite(vs30), "Vs30 is null for run %s", run.getRunID());
				return vs30;
			} else {
				throw new IllegalStateException("Unknown Vs30_Source: "+this);
			}
		}
		
		@Override
		public String toString() {
			return name;
		}
	}
	
	public CyberShakeSiteBuilder(Vs30_Source vs30Source, int velModelID) {
		Preconditions.checkNotNull(vs30Source);
		this.vs30Source = vs30Source;
		Preconditions.checkState(velModelID > 0);
		this.velModelID = velModelID;
	}
	
	public void setForceVs30(Double forceVs30) {
		this.forceVs30 = forceVs30;
	}
	
	public Site buildSite(CybershakeRun run, CybershakeSite csSite) {
		List<CybershakeRun> runs = new ArrayList<>();
		runs.add(run);
		List<CybershakeSite> csSites = new ArrayList<>();
		csSites.add(csSite);
		return buildSites(runs, csSites).get(0);
	}
	
	public List<Site> buildSites(List<CybershakeRun> runs, List<CybershakeSite> csSites) {
		Preconditions.checkState(runs.size() == csSites.size());
		
		LocationList locs = new LocationList();
		for (CybershakeSite csSite : csSites)
			locs.add(csSite.createLocation());
		
		// will only fetch if needed
		List<SiteDataValueList<?>> mapBasinVals = null;
		
		List<Site> sites = new ArrayList<>();
		
		for (int i=0; i<runs.size(); i++) {
			CybershakeRun run = runs.get(i);
			CybershakeSite csSite = csSites.get(i);
			Location siteLoc = locs.get(i);
			
			// build site
			Site site = new Site(siteLoc);
			site.setName(csSite.short_name);
			Vs30_Param vs30Param = new Vs30_Param();
			site.addParameter(vs30Param);
			site.addParameter(new Vs30_TypeParam());
			DepthTo1pt0kmPerSecParam z10Param = new DepthTo1pt0kmPerSecParam(null, true);
			site.addParameter(z10Param);
			DepthTo2pt5kmPerSecParam z25Param = new DepthTo2pt5kmPerSecParam(null, true);
			site.addParameter(z25Param);
			for (Parameter<?> param : site)
				param.setValueAsDefault();
			
			// set site parameters
			double vs30;
			if (forceVs30 != null)
				vs30 = forceVs30;
			else
				vs30 = vs30Source.getVs30(run, siteLoc);
			Preconditions.checkState(Double.isFinite(vs30),
					"Bad Vs30=%s for site %s at %s, %s", vs30, csSite.short_name, csSite.lat, csSite.lon);
			vs30Param.setValue(vs30);
			
			Double z10 = run.getZ10(); 
			Double z25 = run.getZ25();
			if ((z10 == null || z25 == null) && mapBasinVals == null) {
				System.out.println("Fetching basin depth data from web services for "+locs.size()+" sites");
				try {
					mapBasinVals = getMapBasinProviders().getAllAvailableData(locs);
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			}
			
			if (z10 == null) {
				z10 = getMapValueFromList(mapBasinVals, SiteData.TYPE_DEPTH_TO_1_0, i);
				// data providers have it in km, we want it in m
				z10 *= 1000d;
			}
			z10Param.setValue(z10);
			
			if (z25 == null) {
				z25 = getMapValueFromList(mapBasinVals, SiteData.TYPE_DEPTH_TO_2_5, i);
			} else {
				// CS Z2.5 values in m, we need them in km
				z25 /= 1000d;
			}
			z25Param.setValue(z25);
			
			sites.add(site);
		}
		
		return sites;
	}
	
	private static Double getMapValueFromList(List<SiteDataValueList<?>> mapBasinVals, String type, int index) {
		for (SiteDataValueList<?> list : mapBasinVals) {
			if (list.getType().equals(type)) {
				SiteDataValue<?> val = list.getValue(index);
				if (val.getValue() != null)
					return (Double)val.getValue();
			}
		}
		throw new IllegalStateException("No values found for type "+type+" at index "+index);
	}
	
	public synchronized OrderedSiteDataProviderList getMapBasinProviders() {
		if (mapBasinProvs == null)
			mapBasinProvs = getMapBasinProviders(velModelID);
		return mapBasinProvs;
	}
	
	public static OrderedSiteDataProviderList getMapBasinProviders(int velModelID) {
		ArrayList<SiteData<?>> providers = new ArrayList<>();

		if (velModelID == CybershakeVelocityModel.Models.CVM_S_1D.getID()) {
			// Hadley-Kanamori 1D model. Set to 0KM (as per e-mail from David Gill 1/17/14)
			providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_DEPTH_TO_2_5,
					SiteData.TYPE_FLAG_INFERRED, 0d, "Hadley-Kanamori 1D model", "HK-1D"));
			providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_DEPTH_TO_1_0,
					SiteData.TYPE_FLAG_INFERRED, 0d, "Hadley-Kanamori 1D model", "HK-1D"));
			providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_VS30,
					SiteData.TYPE_FLAG_INFERRED, 2886d, "Hadley-Kanamori 1D model Vs30", "HK-1D"));
		} else if (velModelID == CybershakeVelocityModel.Models.BBP_1D.getID()) {
			providers.addAll(getBBP_1D_Providers());
		} else if (velModelID == CybershakeVelocityModel.Models.CCA_1D.getID()) {
			providers.addAll(getCCA_1D_Providers());
		} else {
			if (velModelID == CybershakeVelocityModel.Models.CVM_S4.getID()) {
				/*		CVM4 Depth to 2.5					 */
				try {
					providers.add(new CachedSiteDataWrapper<Double>(new CVM4BasinDepth(SiteData.TYPE_DEPTH_TO_2_5)));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}

				/*		CVM4 Depth to 1.0					 */
				try {
					providers.add(new CachedSiteDataWrapper<Double>(new CVM4BasinDepth(SiteData.TYPE_DEPTH_TO_1_0)));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
			} else if (velModelID == CybershakeVelocityModel.Models.CVM_H_11_2.getID()
					|| velModelID == CybershakeVelocityModel.Models.CVM_H_11_9.getID()
					|| velModelID == CybershakeVelocityModel.Models.CVM_H_11_9_NO_GTL.getID()) {
				/*		CVMH Depth to 2.5					 */
				boolean includeGTL = velModelID != CybershakeVelocityModel.Models.CVM_H_11_9_NO_GTL.getID();
				try {
					CVMHBasinDepth cvmh = new CVMHBasinDepth(SiteData.TYPE_DEPTH_TO_2_5);
					cvmh.getAdjustableParameterList().setValue(CVMHBasinDepth.GTL_PARAM_NAME, includeGTL);
					providers.add(new CachedSiteDataWrapper<Double>(cvmh));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}

				/*		CVMH Depth to 1.0					 */
				try {
					CVMHBasinDepth cvmh = new CVMHBasinDepth(SiteData.TYPE_DEPTH_TO_1_0);
					cvmh.getAdjustableParameterList().setValue(CVMHBasinDepth.GTL_PARAM_NAME, includeGTL);
					providers.add(new CachedSiteDataWrapper<Double>(cvmh));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
			} else if (velModelID == CybershakeVelocityModel.Models.CVM_S4_26.getID()) {
				/*		CVM4i26 Depth to 2.5					 */
				try {
					providers.add(new CachedSiteDataWrapper<Double>(new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_2_5)));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}

				/*		CVM4i26 Depth to 1.0					 */
				try {
					providers.add(new CachedSiteDataWrapper<Double>(new CVM4i26BasinDepth(SiteData.TYPE_DEPTH_TO_1_0)));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
			} else if (velModelID == CybershakeVelocityModel.Models.CCA_06.getID()) {
				/*		CVM4i26 Depth to 2.5					 */
				try {
					providers.add(new CachedSiteDataWrapper<Double>(new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_2_5)));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}

				/*		CVM4i26 Depth to 1.0					 */
				try {
					providers.add(new CachedSiteDataWrapper<Double>(new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_1_0)));
				} catch (IOException e) {
					ExceptionUtils.throwAsRuntimeException(e);
				}
			} else {
				System.err.println("Unknown Velocity Model ID: "+velModelID);
				System.exit(1);
			}
		}
		return new OrderedSiteDataProviderList(providers);
	}
	
	public static List<SiteData<?>> getBBP_1D_Providers() {
		ArrayList<SiteData<?>> providers = new ArrayList<SiteData<?>>();
		
		// BBP LA 1D model.
		providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_DEPTH_TO_2_5,
				SiteData.TYPE_FLAG_INFERRED, 0.0225d, "BBP 1-D LA model", "BBP-1D"));
		providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_DEPTH_TO_1_0,
				SiteData.TYPE_FLAG_INFERRED, 3.500d, "BBP 1-D LA model", "BBP-1D"));
		providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_VS30,
				SiteData.TYPE_FLAG_INFERRED, 843.189d, "BBP 1-D LA model Vs30", "BBP-1D"));
		
		return providers;
	}
	
	public static List<SiteData<?>> getCCA_1D_Providers() {
		ArrayList<SiteData<?>> providers = new ArrayList<SiteData<?>>();
		
		// CCA LA 1D model, via e-mail from David Gill 9/22/16
		providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_DEPTH_TO_1_0,
				SiteData.TYPE_FLAG_INFERRED, 0.0d, "CCA 1-D model", "CCA-1D"));
		providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_DEPTH_TO_2_5,
				SiteData.TYPE_FLAG_INFERRED, 5.5001d, "CCA 1-D model", "CCA-1D"));
		providers.add(new ConstantValueDataProvider<Double>(SiteData.TYPE_VS30,
				SiteData.TYPE_FLAG_INFERRED, 2101d, "CCA 1-D model Vs30", "CCA-1D"));
		
		return providers;
	}

}
