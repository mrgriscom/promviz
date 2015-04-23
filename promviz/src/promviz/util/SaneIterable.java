package promviz.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class SaneIterable<K> implements Iterator<K>, Iterable<K> {

	K nextItem;
	boolean dispensed = true;
	boolean complete = false;
	
	public abstract K genNext();
	
	@Override
	public boolean hasNext() {
		if (dispensed) {
			try {
				nextItem = genNext();
				dispensed = false;
			} catch (NoSuchElementException nsee) {
				complete = true;
			}
		}
		return !complete;
	}

	@Override
	public K next() {
		if (dispensed) {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
		}
		dispensed = true;
		return nextItem;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<K> iterator() {
		return this;
	}

}
