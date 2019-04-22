package scratch.kevin.cybershake;

import java.util.ArrayList;
import java.util.List;

import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;

import com.google.common.base.Joiner;

public class ERF_MagCheck {

	public static void main(String[] args) {
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		for (ProbEqkSource source : erf) {
			List<Double> weirdMags = new ArrayList<>();
			for (ProbEqkRupture rup : source) {
				double mag = rup.getMag();
				if ((float)(mag*20d) != (float)Math.round(mag*20d))
					weirdMags.add(mag);
			}
			if (!weirdMags.isEmpty()) {
				System.out.println("Source "+source.getName()+" has unexpected magnitudes");
				System.out.println("\t"+Joiner.on(",").join(weirdMags));
			}
		}
	}

}
