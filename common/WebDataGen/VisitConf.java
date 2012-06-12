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

public class VisitConf {
	public int agents;
	public long visits;
	public long agentvisits;
	public long pages;
	
	VisitConf (DataOptions options) {
		agents = options.maps;
		visits = options.visits;
		pages = options.pages;	// range of pages to be visited

		agentvisits = (long) Math.ceil(visits * 1.0 / agents);
	}

/*
	VisitConf (int numAgents, long numVisits) {
		agents = numAgents;
		visits = numVisits;

		agentvisits = (long) Math.ceil(visits * 1.0 / agents);
	}
*/
	
	public void setJobConf(JobConf job) {
		job.setInt("agents", agents);
		job.setLong("visits", visits);
		job.setLong("agentvisits", agentvisits);
		job.setLong("pages", pages);
	}

	public String debugInfo() {

		String msg = "[agents: " + agents
				+ "] [visits: " + visits
				+ "] [agentvisits: " + agentvisits
				+ "] [pages: " + pages + "]";
		return msg;
	}
}
