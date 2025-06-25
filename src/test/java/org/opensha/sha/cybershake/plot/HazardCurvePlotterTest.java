package org.opensha.sha.cybershake.plot;

import static org.junit.Assert.*;

import java.io.IOException;
//import java.io.File;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

// Completing all tests should take approximately 25 minutes
// Each computation with CSV probabilities isn't cached and takes around 5 mins.
public class HazardCurvePlotterTest {
	
	final String RUN_ID = "9306";
	final String MOD_CSV = "src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter/USC_mod_probs_Mojave_Coachella.csv";
	final String REF_CSV = "src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter/USC_reference_probs.csv";
	final String OUTPUT_DIR = "src/test/resources/org/opensha/sha/cybershake/plot/HazardCurvePlotter";
	final String PLOT_CHARS_FILE = "src/main/resources/org/opensha/sha/cybershake/conf/robPlot.xml";
	final String COMPONENT = "RotD50";
	final String PERIODS = "3"; 
	final String OUTPUT_TYPE = "TXT";
	
	LocalDate today = LocalDate.now();
    String todayFmt = today.format(DateTimeFormatter.ofPattern("yyyy_MM_dd"));

//	HazardCurvePlotter plotter;
	
	final String[] FETCH_ARGS = {
//			"--output-dir", OUTPUT_DIR + "/fetchdb",
			"--run-id", RUN_ID,
			"--component", COMPONENT,
			"--plot-chars-file", PLOT_CHARS_FILE,
			"--period", PERIODS,
			"--type", OUTPUT_TYPE
	};

	final String[] REF_ARGS = {
//			"--output-dir", OUTPUT_DIR + "/refprob",
			"--run-id", RUN_ID,
			"--component", COMPONENT,
			"--plot-chars-file", PLOT_CHARS_FILE,
			"--period", PERIODS,
			"--rv-probs-csv", REF_CSV,
			"--type", OUTPUT_TYPE
	};

	final String[] MOD_ARGS = {
//			"--output-dir", OUTPUT_DIR + "/modprob",
			"--run-id", RUN_ID,
			"--component", COMPONENT,
			"--plot-chars-file", PLOT_CHARS_FILE,
			"--period", PERIODS,
			"--rv-probs-csv", MOD_CSV,
			"--type", OUTPUT_TYPE
	};
	
	// Prepends the provided outputDir parameter and its value to the set of
	// arguments to pass to the HazardCurvePlotter
	String[] getArgsWithOutputDir(String[] argsTemplate, Path outputDir) {
		String[] args = new String[argsTemplate.length + 2];
		args[0] = "--output-dir";
		args[1] = outputDir.toString();
		System.arraycopy(argsTemplate, 0, args, 2, argsTemplate.length);
		return args;
	}
	
	final String EXPECTED_FETCH =
		    "# Curves: 1\n" +
		    "# Name: CS Run 9306\n" +
		    "1.0E-4\t0.0972944\n" +
		    "1.3E-4\t0.0972944\n" +
		    "1.6E-4\t0.0972944\n" +
		    "2.0E-4\t0.0972944\n" +
		    "2.5E-4\t0.0972944\n" +
		    "3.2E-4\t0.0972944\n" +
		    "4.0E-4\t0.0972944\n" +
		    "5.0E-4\t0.0972944\n" +
		    "6.3E-4\t0.0972944\n" +
		    "7.9E-4\t0.0972944\n" +
		    "0.001\t0.0972944\n" +
		    "0.00126\t0.0972885\n" +
		    "0.00158\t0.0972723\n" +
		    "0.002\t0.0972383\n" +
		    "0.00251\t0.0970549\n" +
		    "0.00316\t0.0967158\n" +
		    "0.00398\t0.0958923\n" +
		    "0.00501\t0.0942809\n" +
		    "0.00631\t0.091648\n" +
		    "0.00794\t0.0880894\n" +
		    "0.01\t0.0827783\n" +
		    "0.01259\t0.0762276\n" +
		    "0.01585\t0.0689824\n" +
		    "0.01995\t0.0609752\n" +
		    "0.02512\t0.052526\n" +
		    "0.03162\t0.0442611\n" +
		    "0.03981\t0.0362934\n" +
		    "0.05012\t0.0288697\n" +
		    "0.0631\t0.0218069\n" +
		    "0.07943\t0.0157526\n" +
		    "0.1\t0.0106364\n" +
		    "0.12589\t0.00651907\n" +
		    "0.15849\t0.00363806\n" +
		    "0.19953\t0.00181766\n" +
		    "0.25119\t8.2677E-4\n" +
		    "0.31623\t3.6392E-4\n" +
		    "0.39811\t1.38002E-4\n" +
		    "0.50119\t5.08047E-5\n" +
		    "0.63096\t1.84047E-5\n" +
		    "0.79433\t4.19709E-6\n" +
		    "1.0\t1.07341E-6\n" +
		    "1.25893\t3.12596E-7\n" +
		    "1.58489\t1.7843E-8\n" +
		    "1.99526\t0.0\n" +
		    "2.51189\t0.0\n" +
		    "3.16228\t0.0\n" +
		    "3.98107\t0.0\n" +
		    "5.01187\t0.0\n" +
		    "6.30957\t0.0\n" +
		    "7.94328\t0.0\n" +
		    "10.0\t0.0\n";

	final String EXPECTED_MOD =
		    "# Curves: 1\n" +
		    "# Name: CS Run 9306\n" +
		    "1.0E-4\t0.09729444838250711\n" +
		    "1.3E-4\t0.09729444838250711\n" +
		    "1.6E-4\t0.09729444838250711\n" +
		    "2.0E-4\t0.09729444838250711\n" +
		    "2.5E-4\t0.09729444838250711\n" +
		    "3.2E-4\t0.09729444838250711\n" +
		    "4.0E-4\t0.09729444838250711\n" +
		    "5.0E-4\t0.09729444838250711\n" +
		    "6.3E-4\t0.09729444838250711\n" +
		    "7.9E-4\t0.09729444838250711\n" +
		    "0.001\t0.09729444838250711\n" +
		    "0.00126\t0.09728848912116439\n" +
		    "0.00158\t0.09727225767021863\n" +
		    "0.002\t0.0972372687226647\n" +
		    "0.00251\t0.09705113294710987\n" +
		    "0.00316\t0.09670946740689823\n" +
		    "0.00398\t0.09587581372849319\n" +
		    "0.00501\t0.09425611726185501\n" +
		    "0.00631\t0.09160148919037625\n" +
		    "0.00794\t0.08804584563318618\n" +
		    "0.01\t0.0827395927035216\n" +
		    "0.01259\t0.07619211523493796\n" +
		    "0.01585\t0.06894634315953796\n" +
		    "0.01995\t0.060954944064043004\n" +
		    "0.02512\t0.052513694685939805\n" +
		    "0.03162\t0.04425593038702558\n" +
		    "0.03981\t0.03629200014551226\n" +
		    "0.05012\t0.028869405969049855\n" +
		    "0.0631\t0.021806499857609185\n" +
		    "0.07943\t0.015753211368358055\n" +
		    "0.1\t0.010637277386283839\n" +
		    "0.12589\t0.006519811872611525\n" +
		    "0.15849\t0.003638106445429834\n" +
		    "0.19953\t0.0018177226040551053\n" +
		    "0.25119\t8.267773520218569E-4\n" +
		    "0.31623\t3.6392088967274194E-4\n" +
		    "0.39811\t1.3800225568516566E-4\n" +
		    "0.50119\t5.08047391840627E-5\n" +
		    "0.63096\t1.8404699624219312E-5\n" +
		    "0.79433\t4.197090710089668E-6\n" +
		    "1.0\t1.0734137476653416E-6\n" +
		    "1.25893\t3.125964780359425E-7\n" +
		    "1.58489\t1.7843047372956278E-8\n" +
		    "1.99526\t0.0\n" +
		    "2.51189\t0.0\n" +
		    "3.16228\t0.0\n" +
		    "3.98107\t0.0\n" +
		    "5.01187\t0.0\n" +
		    "6.30957\t0.0\n" +
		    "7.94328\t0.0\n" +
		    "10.0\t0.0\n";
	
	final String EXPECTED_REF =
		    "# Curves: 1\n" +
		    "# Name: CS Run 9306\n" +
		    "1.0E-4\t0.0972944489013603\n" +
		    "1.3E-4\t0.0972944489013603\n" +
		    "1.6E-4\t0.0972944489013603\n" +
		    "2.0E-4\t0.0972944489013603\n" +
		    "2.5E-4\t0.0972944489013603\n" +
		    "3.2E-4\t0.0972944489013603\n" +
		    "4.0E-4\t0.0972944489013603\n" +
		    "5.0E-4\t0.0972944489013603\n" +
		    "6.3E-4\t0.0972944489013603\n" +
		    "7.9E-4\t0.0972944489013603\n" +
		    "0.001\t0.0972944489013603\n" +
		    "0.00126\t0.09728848963842507\n" +
		    "0.00158\t0.09727225818577845\n" +
		    "0.002\t0.09723829109069337\n" +
		    "0.00251\t0.09705493917914187\n" +
		    "0.00316\t0.09671577389585251\n" +
		    "0.00398\t0.09589225499822596\n" +
		    "0.00501\t0.09428089044980126\n" +
		    "0.00631\t0.09164798918668182\n" +
		    "0.00794\t0.08808939034017538\n" +
		    "0.01\t0.08277825779669401\n" +
		    "0.01259\t0.07622763675464028\n" +
		    "0.01585\t0.06898243741112975\n" +
		    "0.01995\t0.060975172288955326\n" +
		    "0.02512\t0.052526028428850124\n" +
		    "0.03162\t0.04426112627194456\n" +
		    "0.03981\t0.03629335674769674\n" +
		    "0.05012\t0.028869680855119872\n" +
		    "0.0631\t0.021806884821773398\n" +
		    "0.07943\t0.01575262692443402\n" +
		    "0.1\t0.010636406574563328\n" +
		    "0.12589\t0.006519069858863968\n" +
		    "0.15849\t0.0036380569227504322\n" +
		    "0.19953\t0.0018176640175343595\n" +
		    "0.25119\t8.26769698482499E-4\n" +
		    "0.31623\t3.6391966160309597E-4\n" +
		    "0.39811\t1.38002250942737E-4\n" +
		    "0.50119\t5.0804737371956676E-5\n" +
		    "0.63096\t1.8404698884255666E-5\n" +
		    "0.79433\t4.197090279545179E-6\n" +
		    "1.0\t1.073413572361126E-6\n" +
		    "1.25893\t3.1259642152559053E-7\n" +
		    "1.58489\t1.7843042821041877E-8\n" +
		    "1.99526\t0.0\n" +
		    "2.51189\t0.0\n" +
		    "3.16228\t0.0\n" +
		    "3.98107\t0.0\n" +
		    "5.01187\t0.0\n" +
		    "6.30957\t0.0\n" +
		    "7.94328\t0.0\n" +
		    "10.0\t0.0\n";

	@Before
	public void setup() {
		// Ensure no cached results
		Path fetchDir1 = Paths.get(OUTPUT_DIR + "/fetchdb/1");
		Path fetchDir2 = Paths.get(OUTPUT_DIR + "/fetchdb/2");
		Path refDir1 = Paths.get(OUTPUT_DIR + "/refprob/1");
		Path refDir2 = Paths.get(OUTPUT_DIR + "/refprob/2");
		Path modDir1 = Paths.get(OUTPUT_DIR + "/modprob/1");
		Path modDir2 = Paths.get(OUTPUT_DIR + "/modprob/2");
		try {
			FileUtils.deleteDirectory(fetchDir1.toFile());
			FileUtils.deleteDirectory(fetchDir2.toFile());
			Files.createDirectories(fetchDir1);
			Files.createDirectories(fetchDir2);
			FileUtils.deleteDirectory(refDir1.toFile());
			FileUtils.deleteDirectory(refDir2.toFile());
			Files.createDirectories(refDir1);
			Files.createDirectories(refDir2);
			FileUtils.deleteDirectory(modDir1.toFile());
			FileUtils.deleteDirectory(modDir2.toFile());
			Files.createDirectories(modDir1);
			Files.createDirectories(modDir2);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Hazard Curve x,y points for the plots are consistent when fetching from DB
	@Test
	public void fetchConsistentCurves() {
		Path fetchDir1 = Paths.get(OUTPUT_DIR + "/fetchdb/1");
		assertTrue(Files.exists(fetchDir1));
		Path fetchDir2 = Paths.get(OUTPUT_DIR + "/fetchdb/2");
		assertTrue(Files.exists(fetchDir2));

		String[] args = getArgsWithOutputDir(FETCH_ARGS, fetchDir1);
		try {
			HazardCurvePlotter.run(args);
			args = getArgsWithOutputDir(FETCH_ARGS, fetchDir2);
			HazardCurvePlotter.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		};

		Path res1 = fetchDir1.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_2022_12_21.txt");
		Path res2 = fetchDir2.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_2022_12_21.txt");
		assertTrue(Files.exists(res1));
		assertTrue(Files.exists(res2));
		try {
			assertEquals(Files.readString(res1), EXPECTED_FETCH);
			assertEquals(Files.readString(res2), EXPECTED_FETCH);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Hazard Curve x,y points are equal when building
	@Test
	public void calculateConsistentCurves() {
		Path refDir1 = Paths.get(OUTPUT_DIR + "/refprob/1");
		assertTrue(Files.exists(refDir1));
		Path refDir2 = Paths.get(OUTPUT_DIR + "/refprob/2");
		assertTrue(Files.exists(refDir2));

		String[] args = getArgsWithOutputDir(REF_ARGS, refDir1);
		try {
			HazardCurvePlotter.run(args);
			args = getArgsWithOutputDir(REF_ARGS, refDir2);
			HazardCurvePlotter.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		};

		Path res1 = refDir1.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_" + todayFmt + ".txt");
		Path res2 = refDir2.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_" + todayFmt + ".txt");
		assertTrue(Files.exists(res1));
		assertTrue(Files.exists(res2));
		try {
			assertEquals(Files.readString(res1), EXPECTED_REF);
			assertEquals(Files.readString(res2), EXPECTED_REF);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Equal when building with modified rupture variation probabilities
	@Test
	public void calculateConsistentCurvesModifiedRupVarProb() {
		Path modDir1 = Paths.get(OUTPUT_DIR + "/modprob/1");
		assertTrue(Files.exists(modDir1));
		Path modDir2 = Paths.get(OUTPUT_DIR + "/modprob/2");
		assertTrue(Files.exists(modDir2));

		String[] args = getArgsWithOutputDir(MOD_ARGS, modDir1);
		try {
			HazardCurvePlotter.run(args);
			args = getArgsWithOutputDir(MOD_ARGS, modDir2);
			HazardCurvePlotter.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		};

		Path res1 = modDir1.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_" + todayFmt + ".txt");
		Path res2 = modDir2.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_" + todayFmt + ".txt");
		assertTrue(Files.exists(res1));
		assertTrue(Files.exists(res2));
		try {
			assertEquals(Files.readString(res1), EXPECTED_MOD);
			assertEquals(Files.readString(res2), EXPECTED_MOD);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Different when comparing modified rupvar probs to fetched curve.
	@Test
	public void differentCurvesWithModifiedRupVarProb() {
		Path modDir = Paths.get(OUTPUT_DIR + "/modprob/1");
		assertTrue(Files.exists(modDir));
		Path fetchDir = Paths.get(OUTPUT_DIR + "/fetchdb/1");
		assertTrue(Files.exists(fetchDir));

		String[] args = getArgsWithOutputDir(MOD_ARGS, modDir);
		try {
			HazardCurvePlotter.run(args);
			args = getArgsWithOutputDir(FETCH_ARGS, fetchDir);
			HazardCurvePlotter.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		};

		Path res1 = modDir.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_" + todayFmt + ".txt");
		Path res2 = fetchDir.resolve(
				"USC_ERF36_Run9306_SA_3sec_RotD50_2022_12_21.txt");
		assertTrue(Files.exists(res1));
		assertTrue(Files.exists(res2));
		try {
			assertNotEquals(Files.readString(res1), Files.readString(res2));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
