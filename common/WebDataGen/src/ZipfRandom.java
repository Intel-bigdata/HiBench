/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hibench;

import org.apache.hadoop.mapred.JobConf;

public class ZipfRandom extends RandSimulate {

	public int agents;
	public int reds;
	public long agentelems;
	public double exponent;

	ZipfRandom (int numAgents, int numReds, long numElems, double exp) {
		type = RandType.ZIPFIAN;
		agents = numAgents;
		reds = numReds;
		elems = numElems;
		exponent = exp;

		agentelems = (long) Math.ceil(elems * 1.0 / agents);
	}

	@Override
	public double massfunc(double var) {
		return 1.0 / Math.pow(var, exponent);
	}

	@Override
	public void setVirtElems(long numVirtElems) {
		velems = numVirtElems;
	}

	public double estimateZipfSum() {
		double p0=1, x0=1, x1, p1, estSum=0, logstep=1.5;

		/***
		 * Estimate scale, sum, and required space to simulate zipf sampling 
		 */
		do {
			x1 = Math.ceil(x0*logstep);
			p1 = massfunc(x1);
			estSum = estSum + (p0+p1)*(x1-x0)/2.0;

			x0 = x1;
			p0 = p1;
		} while (x0<=elems);

		return estSum;
	}

	@Override
	public void calcScale(long samples, double zoom) {

		samps = samples;
		ratio = zoom;

		double estSum = estimateZipfSum();

		double estScale = Math.ceil(1.0 / massfunc(elems));
		double estSpace = estScale * estSum;

		double limit = ratio * samps;
		if (MAX_SPACE < limit) {
			limit = MAX_SPACE;
		}

		if (limit < estSpace) {
			estSpace = limit;
		}

		scale = estSpace / estSum;
	}

	public void setJobConf (JobConf job) {
		job.setInt("agents", agents);
		job.setLong("elems", elems);
		job.setLong("agentelems", agentelems);
		job.setFloat("exponent", (float)exponent);
		job.setFloat("scale", (float)scale);
	}

	public String debugInfo() {

		return "[elems: " + elems
				+ "] [velems: " + velems
				+ "] [samps: " + samps
				+ "] [ratio: " + ratio
				+ "] [agentelems: " + agentelems
				+ "] [exponent: " + exponent
				+ "] [scale: " + scale + "]";
	}
}
