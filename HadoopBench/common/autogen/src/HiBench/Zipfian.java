package HiBench;

/***
 * Zipfian distribution: Y(x) = f / pow(x, exponent) where x = 1, 2, ..., n
 * 
 * Simulation <RULE 0>:
 * 	Define Z(x) = Y'(1) + Y'(2) + ... + Y'(x), where Y'(i) = round(Y(i) * scale).
 * 	For any z randomly selected from [0, Z(n)), choose i if Z(i-1) < x < Z(i),
 * 	assuming Z(0) = 0. We say, x (nearly) follows Zipfian distribution.
 * 
 * 	scale is determined by considering:
 * 	1. Y'(n) should not be less then 1 (to ensure the chance of picking up n)
 * 	2. Z(n) should not be too large (to ensure the times of picking each z)
 * 
 * n may be too large and it's not suitable to store all Z(x)s. Furthermore,
 * directly searching Z(i-1) < x <Z(i) may be time consuming. To speed up the
 * searching, continues Y'(x)s with the same values will be folded into one buck. 
 * knee point xknee is a special x where
 * 		if (x < xknee), then each buck contains only one Y'(x), i.e., B(i) = {Y'(i)}
 * 		if (x >= xknee), then each buck conains one ore more Y'(x)
 * 
 * Each buck is represented as <xstart, zstart, yvalue>, where xstart is the start x
 * of buck, zstart is the start z of buck, and yvalue is the corresponding y value of
 * buck. Therefore the search space becomes buck[0], buck[1], ..., buck[n'].
 * 
 * An index (index[0], index[1], ..., index[n'']) is built to fast reduce search
 * space. For any given z, func(z) is a bitwise function which calculates the index
 * corresponding to z. For j = func(z), zstart[index[j]] < z < zstart[index[j+1]].
 * Therefore the search space of z becomes buck[index[j]] ~ buck[index[j+1]]
 *  
 * @author lyi2
 *
 */

public class Zipfian {

	private long elems, knee;
	private long zelems, zknee;
	
	private double exponent, tail, scale;

	private int gran, divider;
	private long mask, limit, minY;

	private long[] buckIndex;
	private long[] zbuck, xbuck, ybuck;

	Zipfian(long size, double exp) {
		elems = size;
		exponent = exp;
	}
	
	private long calcFactors (double vtail) {
		tail = vtail;
		scale = tail * Math.pow(elems, exponent);
		
		knee = calcKneePoint();
		zknee = calcZKnee();
		zelems = zknee + calcRestZ();

		divider = floorLog2((long) minY) + 1;
		gran = floorLog2(zknee >> divider) + 1;
		mask = (1 << gran) - 1;
		limit = Long.rotateLeft(1, gran + divider);
		
		return zelems;
	}
	
	public void setupZipf(long samples) {

		System.out.println("Here I am");
		double vtail = 0.5;
		long basev = calcFactors(vtail);
		if (basev > samples) {
			System.out.println("ERROR: too small samples to show zipfian distribution!!!");
			System.exit(-1);
		}
/*		
		while (Math.abs(samples - basev) > 100) {
			vtail = samples * vtail / basev;
			basev = calcFactors(vtail);
		}
*/		
		System.out.println(
				"guess tail: " + vtail + 
				", guess velems: " + basev + 
				", samples: " + samples +
				", scale: " + scale);

		createIndex();
		generateBucks();
	}

	public void setupZipf(long samples, double zoom) {

		long expectVElems = (long) Math.floor(samples * zoom);
		long baseVElems = calcFactors(0.5);

		// if expectVElems is too small
		if (expectVElems < baseVElems) {
			System.out.println("ERROR: too small samples to show zipfian distribution!!!");
			System.out.println("Please increase samples or zoom value!!!");
			System.exit(-1);
		}
		
		// find the (almost) largest tail regarding the expectVElems
		double vtail = Math.round(expectVElems * 0.5 / baseVElems) - 0.5;
		if (vtail > 10) {
			vtail = 10;
		}
		while (calcFactors (vtail) > expectVElems) {
			vtail = vtail - 1;
		}

		createIndex();
		generateBucks();
	}
	
	public ZipfCore createZipfCore() {
		
		ZipfCore kernel = new ZipfCore();
		
		kernel.elems = elems;
		kernel.exponent = exponent;
		kernel.scale = scale;
		kernel.zelems = zelems;

		kernel.zbuck = zbuck;
		kernel.xbuck = xbuck;
		kernel.ybuck = ybuck;
		
		kernel.gran = gran;
		kernel.divider = divider;
		kernel.mask = mask;
		kernel.limit = limit;

		kernel.buckIndex = new int[buckIndex.length];
		for (int i=0; i<kernel.buckIndex.length; i++) {
			kernel.buckIndex[i] = (int) buckIndex[i];
		}

		return kernel;
	}
	
	private double getXfromDerivative(double derivative) {
		return Math.pow((scale * exponent) / derivative, 1.0 / (1 + exponent));
	}
	
	private double getXfromY(double Y) {
		return Math.pow(scale / Y, 1.0 / exponent);
	}
	
	private double getYfromX(double X) {
		return scale / Math.pow(X, exponent);
	}
	
	/***
	 * calculate to get the knee point which separates velems into
	 * two parts:
	 * 		left  (x <  knee): one buck for each x
	 * 		right (x >= knee): one buck for 1 or more x
	 * @return knee point x
	 */
	private long calcKneePoint() {

		long x = (long) Math.ceil(getXfromDerivative(1.0));
		if ((getYfromX(x - 1) - getYfromX(x)) < 1.0) {
			x--;
		}
		return x;
	}

	private long calcZKnee() {

		long sumY = 0;
		for (long i=1; i<knee; i++) {
			sumY = sumY + Math.round(getYfromX(i));
		}
		return sumY;
	}

	private long calcRestZ() {

		minY = Long.MAX_VALUE;
		
		long sumY = 0;
		long x0 = knee - 1, x1 = knee;
		for (long curZ = Math.round(getYfromX(x1)); curZ >= Math.round(tail); curZ--) {
			x1 = (long) Math.floor(getXfromY(curZ-0.5));
			if (x1 > elems) {
				x1 = elems;
			}
			sumY = sumY + (x1 - x0) * curZ;
			if (minY > (x1 - x0) * curZ) {
				minY = (x1 - x0) * curZ;
			}
			x0 = x1;
		}
		return sumY;
	}

	private int calcBuckIndex (long x) {

		/**
		 * ipart: floor of log2(X) + 1
		 * fpart: 
		 */
		long X = (x + limit) >> divider;
		
		int ipart = floorLog2(X >> gran);
		int fpart = (int) (mask & (X >> ipart));

//		System.out.println("-- " + x + " - " + X + "<" + ipart + ", " + fpart + ">");
		return (ipart << gran) + fpart;
	}

	// floor of log2(x), return -1 if x = 0
	private int floorLog2 (long x) {
		return 63 - Long.numberOfLeadingZeros(x);
	}
	
	private void createIndex () {
		int lenIndex = calcBuckIndex(zelems);
		int intLimit = lenIndex >> (gran);
		lenIndex = lenIndex + 2;
		buckIndex = new long[lenIndex];
		
		int curIndex = 0;
		int i = 0, j = 0;
		boolean endloop = false;
		for (i=0; i<=intLimit; i++) {
			for (j=0; j<(mask+1); j++) {
				long ipart = Long.rotateLeft(1, gran + divider + i);
				long fpart = Long.rotateLeft(j, divider + i);
				long x = ipart + fpart - limit;
				if (x > zelems) {
					endloop = true;
					break;
				}
				
				curIndex = (i << gran) + j;
				buckIndex[curIndex] = x;
			}
			if (endloop) break;
		}
		
		curIndex = (i << gran) + j; 
		System.out.println("curIndex: " + curIndex + ", total: " + lenIndex);
		buckIndex[curIndex] = buckIndex[curIndex - 1];
	}

	private int writeBackIndex(int index, int buck, long sum) {
//		int count = 0;
		while ((index < buckIndex.length) && (buckIndex[index] < sum)) {
			buckIndex[index++] = buck;
//			count++;
		}
//		System.out.println("count: " + count);
		return index;
	}

	private void generateBucks() {
		
		int bucks = (int) ((knee - 1) + (Math.round(getYfromX(knee)) - Math.round(tail) + 1));

		zbuck = new long[bucks+1];
		xbuck = new long[bucks+1];
		ybuck = new long[bucks+1];

		long sumZ = 0;
		int index = 0;
		int i;
		for (i=1; i<knee; i++) {
			xbuck[i-1] = i - 1;
			zbuck[i-1] = sumZ;

			ybuck[i-1] = Math.round(getYfromX(i));
			sumZ = sumZ + ybuck[i-1];

			index = writeBackIndex(index, i - 1, sumZ);
		}

		// i == knee now
		long x0 = knee - 1, x1 = knee;
		for (long curZ = Math.round(getYfromX(x1)); curZ >= Math.round(tail); curZ--) {
			xbuck[i-1] = x0;
			zbuck[i-1] = sumZ;

			x1 = (long) Math.floor(getXfromY(curZ-0.5));
			if (x1 > elems) {
				x1 = elems;
			}
			ybuck[i-1] = curZ;
			sumZ = sumZ + (x1 - x0) * curZ;

			index = writeBackIndex(index, i - 1, sumZ);
			x0 = x1;
			i++;
		}

		xbuck[i-1] = x0;
		zbuck[i-1] = sumZ;
	}

	public String debuginfo() {
		
		return "[elems: " + elems
				+ "] [zelems: " + zelems
				+ "] [knee: " + knee
				+ "] [zknee: " + zknee
				+ "] [exponent: " + exponent
				+ "] [tail: " + tail
				+ "] [scale: " + scale
				+ "] [gran: " + gran
				+ "] [divider: " + divider
				+ "] [mask: " + mask
				+ "] [limit: " + limit
				+ "] [minY: " + minY + "]";
	}
	
}
