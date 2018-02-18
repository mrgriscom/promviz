package com.mrgris.prominence.util;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

public class MutablePriorityQueue <E> extends PriorityQueue<E> {

	Set<E> set;
	
	public MutablePriorityQueue(Comparator<? super E> c) {
		super(10, c);
		set = new HashSet<E>();
	}
	
	public boolean add(E e) {
		boolean newItem = set.add(e);
		assert newItem;
		super.add(e);
		return true;
	}
	
	public E poll() {
		E e = super.poll();
		if (e != null) {
			set.remove(e);
			ensureHeadIsValid();
		}
		return e;
	}
	
	public boolean remove(Object o) {
		boolean removed = set.remove(o);
		ensureHeadIsValid();
		return removed;
	}

	public boolean contains(Object o) {
		return set.contains(o);
	}
	
	public int size() {
		return set.size();
	}

	public Iterator<E> iterator() {
		return set.iterator();
	}
	
	// removed items remain in the heap. thus, when the head of the heap changes
	// purge any items until a valid one is at the head again
	void ensureHeadIsValid() {
		while (true) {
			E e = peek();
			if (e == null || contains(e)) {
				break;
			}
			super.poll(); // remove and discard (note: SUPER.poll(), not this.)
		}
	}
}
