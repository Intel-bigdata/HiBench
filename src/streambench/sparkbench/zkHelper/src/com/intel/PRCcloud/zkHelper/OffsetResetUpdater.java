package com.intel.PRCcloud.zkHelper;

import org.I0Itec.zkclient.DataUpdater;

public class OffsetResetUpdater<T> implements DataUpdater<T> {

	T newOffset;
	public OffsetResetUpdater(T offset){
		newOffset=offset;
	}
	
	@Override
	public T update(T l) {
		return newOffset;
	}

}
