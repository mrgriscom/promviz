package promviz.util;

import java.util.Comparator;

public class ReverseComparator<K> implements Comparator<K> {

	Comparator<K> cmp;
	
	public ReverseComparator(Comparator<K> cmp) {
		this.cmp = cmp;
	}
	
	@Override
	public int compare(K a, K b) {
		return cmp.compare(b, a);
	}

}
