package HiBench;

import java.io.Serializable;
import java.util.Random;

public class ZipfCore implements Serializable {

	private static final long serialVersionUID = 2483499022100222872L;

	public long elems, zelems;
	public double exponent, scale;

	public int gran, divider;
	public long mask, limit;

	public int[] buckIndex;
	public long[] zbuck, xbuck, ybuck;	// bucks represented by three arrays
	
	public Random rand;

	ZipfCore() {
		rand = new Random();
	}

	public void setRandSeed(long seed) {
		if (null==rand) {
			rand = new Random(seed);
		} else {
			rand.setSeed(seed);
		}
	}

	public long simpleNext() {

		long v = (long) Math.floor(rand.nextDouble() * zelems);

//		count++;
		int start = 0, end = zbuck.length-2, mid;
		while (start != end) {
			mid = (start + end) / 2;
			if (v >= zbuck[mid+1]) {
				start = mid + 1;
			} else {
				end = mid;
			}
//			count++;
		}
		return xbuck[start] + (v - zbuck[start]) / ybuck[start];
	}

	public long next() {

		long v = (long) Math.floor(rand.nextDouble() * zelems);
		
		long X = (v + limit) >> divider;
		int ipart = 63 - Long.numberOfLeadingZeros(X >> gran);
		int i = (int) ((ipart << gran) + (mask & (X >> ipart)));

//		count++;
		int start = buckIndex[i], end = buckIndex[i+1], mid;
		while (start != end) {
			mid = (start + end) / 2;
			if (v >= zbuck[mid+1]) {
				start = mid + 1;
			} else {
				end = mid;
			}
//			count++;
		}
		return xbuck[start] + (v - zbuck[start]) / ybuck[start];
	}
}
