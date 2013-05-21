package promviz.util;

import java.util.HashMap;

public abstract class DefaultMap<K, V> extends HashMap<K, V> {
	@Override
	public V get(Object key) {
		if (!this.containsKey(key)) {
			this.put((K)key, this.defaultValue());
		}
		return super.get(key);
	}
	
	public abstract V defaultValue();
}
