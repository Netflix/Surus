package org.surus.math;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class AugmentedDickeyFuller_Test {

	@Test
	public void testLinearTrend() {
		Random rand = new Random();
		double[] x = new double[100];
		for (int i = 0; i < x.length; i ++) {
			x[i] = (i+1) + 5*rand.nextDouble();
		}
		AugmentedDickeyFuller adf = new AugmentedDickeyFuller(x);
		assertTrue(adf.isNeedsDiff() == true);
	}
	
	@Test
	public void testLinearTrendWithOutlier() {
		Random rand = new Random();
		double[] x = new double[100];
		for (int i = 0; i < x.length; i ++) {
			x[i] = (i+1) + 5*rand.nextDouble();
		}
		x[50] = 100;
		AugmentedDickeyFuller adf = new AugmentedDickeyFuller(x);
		assertTrue(adf.isNeedsDiff() == true);
	}

}
