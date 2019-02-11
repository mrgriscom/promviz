package com.mrgris.prominence.util;

import java.util.HashMap;

// THIS CLASS IS NOT THREAD-SAFE

public abstract class DefaultMap<K, V> extends HashMap<K, V> {
	long threadId = -1;
	
	@Override
	public V get(Object key) {
		threadSafetyCheck();
		
		if (!this.containsKey(key)) {
			this.put((K)key, this.defaultValue((K)key));
		}
		return super.get(key);
	}
	
	void threadSafetyCheck() {
		if (threadId == -1) {
			threadId = Thread.currentThread().getId();
		} else if (Thread.currentThread().getId() != threadId) {
			throw new RuntimeException("DefaultMap is not thread-safe!");
		}		
	}
	
	@Override
	public Object clone() {
		threadId = -1;
		return super.clone();
	}
	
	public abstract V defaultValue(K key);
}
