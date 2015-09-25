/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.zkHelper;

import org.I0Itec.zkclient.ZkClient;

public class ZKUtil {

	ZkClient zkClient;
	
	public ZKUtil(String zkHost){
		zkClient=new ZkClient(zkHost,30*1000, 30*1000, new ZKStringSerializer());
	}
	
	public void resetOffsetToBegin(String path,int partitionCount,String newOffset){
		OffsetResetUpdater<String> update=new OffsetResetUpdater<String>(newOffset);
		for(int i=0;i<partitionCount;i++){
			String pathi=path+i;
			zkClient.updateDataSerialized(pathi, update);
		}
	}
	
	public void lsOffsets(String path,int partitionCount){
		for(int i=0;i<partitionCount;i++){
			String pathi=path+"/"+i;
			System.out.println("Path:"+pathi);
			Object res=zkClient.readData(pathi);
			System.out.println("Partition:"+i+"  offset:"+res);
		}
	}
	
	public static void main(String[] args){
		if(args.length<4){
			System.err.println("Usage: <OP> <ZKHost> <PATH> <PARTITION_COUNT>");
			return;
		}
		String op=args[0];	
		String zkHost=args[1];
		String path=args[2];
		int partitionCount=Integer.parseInt(args[3]);
		String offset="0";
		if(args.length>4){
			offset=args[4];
		}
		
		ZKUtil zk=new ZKUtil(zkHost);
		if(op.equals("ls")){
			zk.lsOffsets(path, partitionCount);
		}else if(op.equals("update")){
			zk.resetOffsetToBegin(path, partitionCount, offset);
		}else{
			System.err.println("Unsupported operation!");
		}
	}
}
