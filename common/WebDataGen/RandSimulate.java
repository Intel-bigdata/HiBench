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

public abstract class RandSimulate {
	public static final long MAX_SPACE = Long.parseLong("1000000000000");

	public static enum RandType {
		UNIFORM, GAUSSIAN, ZIPFIAN, POISSON  
	}
	
	public RandType type;
	
	public long elems;	// original number of random values (e.g., url_1, url_2, ..., url_m)
	public long velems;	// number of virtual random values after flattened simulation (e.g., vurl_1,vurl_2, ..., vurl_n) 
	public long samps;	// expected number of samples (e.g., totally M links to be created)
	public double ratio;	// usually, ratio = velems/samps should be less than 0.1/0.01 
	public double scale;	// basically, scale * sum(P(url_i)) = velems
							// scale * massfunc(url_i) is the number of velems in [0, range) that refers to url_i

	public abstract double massfunc (double var);	// probability mass function, i.e., return P(url_i)
													// but not limited to be <1, it can be r*P(url_i) where
													// r is a constant value
	public abstract void setVirtElems (long numVirtElems);
	public abstract void calcScale (long samples, double factor);
}