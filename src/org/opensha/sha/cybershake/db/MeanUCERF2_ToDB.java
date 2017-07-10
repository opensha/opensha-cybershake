/**
 * 
 */
package org.opensha.sha.cybershake.db;

import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.TimeSpan;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.param.ParameterList;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.InterpolatedEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This Class creates an instances of Mean UCERF2 ERF to insert into the database
 * 
 * @author vipingupta
 *
 */
public class MeanUCERF2_ToDB extends ERF2DB {
	
	public MeanUCERF2_ToDB(DBAccess db){
		this(db, false);
	}
	
	public MeanUCERF2_ToDB(DBAccess db, boolean hiRes) {
		this(db, hiRes, true);
	}

	public MeanUCERF2_ToDB(DBAccess db, boolean hiRes, boolean ddwAdjust) {
		super(db);
		if (hiRes)
			eqkRupForecast = createUCERF2_200mERF(ddwAdjust);
		else
			eqkRupForecast = createUCERF2ERF(ddwAdjust);
	}

	public static AbstractERF createUCERF2ERF() {
		return createUCERF2ERF(true);
	}
	
	public static AbstractERF createUCERF2ERF(boolean ddwAdjust) {


		AbstractERF eqkRupForecast = new MeanUCERF2();

		eqkRupForecast = setMeanUCERF_CyberShake_Settings(eqkRupForecast, ddwAdjust);

		return eqkRupForecast;
	}
	
	public static AbstractERF setMeanUCERF_CyberShake_Settings(AbstractERF eqkRupForecast) {
		return setMeanUCERF_CyberShake_Settings(eqkRupForecast, true);
	}

	public static AbstractERF setMeanUCERF_CyberShake_Settings(AbstractERF eqkRupForecast, boolean ddwAdjust) {
		// exclude Background seismicity
		eqkRupForecast.getAdjustableParameterList().getParameter(
				UCERF2.BACK_SEIS_NAME).setValue(UCERF2.BACK_SEIS_EXCLUDE);

		// Rup offset
		eqkRupForecast.getAdjustableParameterList().getParameter(
				MeanUCERF2.RUP_OFFSET_PARAM_NAME).setValue(
						new Double(5.0));

		// Cybershake DDW(down dip correction) correction
		eqkRupForecast.getAdjustableParameterList().getParameter(
				MeanUCERF2.CYBERSHAKE_DDW_CORR_PARAM_NAME).setValue(
						new Boolean(ddwAdjust));

		// Set Poisson Probability model
		eqkRupForecast.getAdjustableParameterList().getParameter(
				UCERF2.PROB_MODEL_PARAM_NAME).setValue(
						UCERF2.PROB_MODEL_POISSON);

		// duration
		eqkRupForecast.getTimeSpan().setDuration(1.0);

		System.out.println("Updating Forecast...");
		eqkRupForecast.updateForecast();

		return eqkRupForecast;
	}
	
	public static AbstractERF createUCERF2_200mERF(boolean ddwAdjust) {
		double hiResSpacing = 0.2;
		final int discrPnts = (int)(1.0/hiResSpacing);
		
		// first get regular ERF
		AbstractERF regERF = createUCERF2ERF(ddwAdjust);
		// set high res: 200m
		UCERF2.GRID_SPACING = hiResSpacing;
		AbstractERF hiResERF = createUCERF2ERF(ddwAdjust);
		UCERF2.GRID_SPACING = 1.0;
		
		Preconditions.checkState(regERF.getNumSources() == hiResERF.getNumSources());
		
		// now go through each source and find differences
		final List<ProbEqkSource> combSourceList = Lists.newArrayList();
		
		int interpolatedCnt = 0;
		
		for (int sourceID=0; sourceID<regERF.getNumSources(); sourceID++) {
			System.out.print("Source "+sourceID+"...");
			final ProbEqkSource regSource = regERF.getSource(sourceID);
			final ProbEqkSource hiResSource = hiResERF.getSource(sourceID);
			
			EvenlyGriddedSurface s = (EvenlyGriddedSurface)hiResSource.getRupture(0).getRuptureSurface();
//			s = (EvenlyGriddedSurface)hiResSource.getRupture(0).getRuptureSurface();
//			System.out.println(LocationUtils.horzDistance(s.get(0, 0), s.get(0, 1)));
//			System.out.flush();
			Preconditions.checkState(s.getAveGridSpacing() == hiResSpacing);
			
			if (identical(regSource, hiResSource)) {
				// use hi res
				System.out.println("Identical!");
				combSourceList.add(hiResSource);
			} else {
				System.out.println("Different, building!");
				interpolatedCnt++;
				// use lo res, with hi res interpolated surfaces
				final List<ProbEqkRupture> rups = Lists.newArrayList();
				for (int i=0; i<regSource.getNumRuptures(); i++) {
					ProbEqkRupture loResRup = regSource.getRupture(i);
					final EvenlyGriddedSurface loResSurf = (EvenlyGriddedSurface)loResRup.getRuptureSurface();
					EvenlyGriddedSurface interpSurf = new InterpolatedEvenlyGriddedSurface(loResSurf, hiResSpacing);
//					// now set all points
//					for (int origRow=0; origRow<origRows-1; origRow++) {
//						for (int origCol=0; origCol<origCols-1; origCol++) {
//							Location topLeftLoc = loResSurf.get(origRow, origCol);
//							Location topRightLoc = loResSurf.get(origRow, origCol+1);
//							Location botLeftLoc = loResSurf.get(origRow+1, origCol);
//							Location botRightLoc = loResSurf.get(origRow+1, origCol+1);
//							
//							double horzDist = LocationUtils.horzDistance(topLeftLoc, topRightLoc);
//							double vertDist = LocationUtils.horzDistance(topLeftLoc, botLeftLoc);
//							
//							double horzAz = LocationUtils.azimuthRad(topLeftLoc, topRightLoc);
//							double vertAz = LocationUtils.azimuthRad(topLeftLoc, botLeftLoc);
//							
//							double depthDelta = botLeftLoc.getDepth()-topLeftLoc.getDepth();
//							
////							Preconditions.checkState(Math.abs(horzAzTop-horzAzBot) < 0.1, horzAzTop+" diff "+horzAzBot);
////							if ((float)vertDistLeft > 0f || (float)vertDistRight > 0f)
////								Preconditions.checkState(Math.abs(vertAzLeft-vertAzRight) < 0.1, vertAzLeft+" diff "+vertAzRight+" ("+0.5*(vertDistLeft+vertDistRight)+")");
//							
//							for (int rowI=0; rowI<discrPnts; rowI++) {
//								double relativeVertPos = (double)rowI/(double)discrPnts;
//								for (int colI=0; colI<discrPnts; colI++) {
//									double relativeHorzPos = (double)colI/(double)discrPnts;
//									
////									double horzDist = relativeHorzPos*(horzDistTop*(1d-relativeVertPos)+horzDistBot*relativeVertPos);
////									double vertDist = relativeVertPos*(vertDistLeft*(1d-relativeHorzPos)+vertDistRight*relativeHorzPos);
////									double horzAz = horzAzTop*(1d-relativeVertPos)+horzAzBot*relativeVertPos;
////									double vertAz = vertAzLeft*(1d-relativeHorzPos)+vertAzRight*relativeHorzPos;
////									
////									double depthDelta = relativeVertPos*(depthDeltaLeft*(1d-relativeHorzPos)+depthDeltaRight*relativeHorzPos);
//									
//									// start top left
//									Location loc = topLeftLoc;
//									// move to the right
//									loc = LocationUtils.location(loc, horzAz, horzDist*relativeHorzPos);
//									// move down dip
//									if ((float)vertDist > 0f)
//										loc = LocationUtils.location(loc, vertAz, vertDist*relativeVertPos);
//									// now actually move down
//									loc = new Location(loc.getLatitude(), loc.getLongitude(), loc.getDepth()+depthDelta*relativeVertPos);
//									interpSurf.set(origRow*discrPnts+rowI, origCol*discrPnts+colI, loc);
//								}
//							}
//						}
//					}
					ProbEqkRupture hiResRup = new ProbEqkRupture(loResRup.getMag(), loResRup.getAveRake(),
							loResRup.getProbability(), interpSurf, loResRup.getHypocenterLocation());
					rups.add(hiResRup);
				}
				ProbEqkSource combSource = new ProbEqkSource() {
					
					@Override
					public String getName() {
						return regSource.getName();
					}

					@Override
					public String getInfo() {
						return regSource.getInfo();
					}

					@Override
					public RuptureSurface getSourceSurface() {
						return hiResSource.getSourceSurface();
					}
					
					@Override
					public LocationList getAllSourceLocs() {
						return hiResSource.getAllSourceLocs();
					}
					
					@Override
					public ProbEqkRupture getRupture(int nRupture) {
						return rups.get(nRupture);
					}
					
					@Override
					public int getNumRuptures() {
						return regSource.getNumRuptures();
					}
					
					@Override
					public double getMinDistance(Site site) {
						return regSource.getMinDistance(site);
					}
				};
				combSourceList.add(combSource);
			}
		}
		System.out.println("Done building interpolated ERF, interpolated "
				+interpolatedCnt+"/"+regERF.getNumSources()+" sources");
		Preconditions.checkState(combSourceList.size() == regERF.getNumSources());
		String ddwSt = "";
		if (!ddwAdjust)
			ddwSt = ", No DDW";
		final String name = regERF.getName()+" "+(int)(hiResSpacing*1000d)+"m"+ddwSt;
		final ParameterList adjustParams = regERF.getAdjustableParameterList();
		final TimeSpan regTimeSpan = regERF.getTimeSpan();
		AbstractERF meanERF = new AbstractERF() {
			
			@Override
			public String getName() {
				// kludgy way to do it
				this.adjustableParams = adjustParams;
				this.timeSpan = regTimeSpan;
				return name;
			}
			
			@Override
			public void updateForecast() {}
			
			@Override
			public ProbEqkSource getSource(int idx) {
				return combSourceList.get(idx);
			}
			
			@Override
			public int getNumSources() {
				return combSourceList.size();
			}
		};
		// this does our init stuff above
		meanERF.getName();
		
		return meanERF;
	}
	
	private static boolean identical(ProbEqkSource src1, ProbEqkSource src2) {
		if (src1.getNumRuptures() != src2.getNumRuptures())
			return false;
		
		for (int rupID=0; rupID<src1.getNumRuptures(); rupID++) {
			if ((float)src1.getRupture(rupID).getProbability() != (float)src2.getRupture(rupID).getProbability())
				return false;
			if ((float)src1.getRupture(rupID).getMag() != (float)src2.getRupture(rupID).getMag())
				return false;
		}
		
		return true;
	}
}
