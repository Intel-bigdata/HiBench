package com.intel.hammer.log.util;

import java.util.Random;

public class EnhancedRandom extends Random {

	private static final long serialVersionUID = -6077814966843574151L;

	public EnhancedRandom() {
		super();
	}

	// public double nextGaussian(double mean,double scale){
	// return super.nextGaussian() * scale + mean;
	// }

	// public double nextGaussian(double mean){
	// return super.nextGaussian() + mean;
	// }

	public double nextGaussian(double miu, double sigma2) {
		double N = 12;
		double x = 0, temp = N;
		do {
			x = 0;
			for (int i = 0; i < N; i++)
				x = x + (Math.random());
			x = (x - temp / 2) / (Math.sqrt(temp / 12));
			x = miu + x * Math.sqrt(sigma2);
		} while (x <= 0);
		return x;
	}
	
	public double nextGaussian(double mean){
		return nextGaussian(mean,1);
	}
}
