package org.pocs.keyvalue.memory;

import org.pocs.keyvalue.KVStore;

public class InMemoryDiskBacked<K, V> implements KVStore<K, V> {

	int concurrencyLevel = 16;
	DataMap<K, V>[] m = null;
	String name = null;
	
	public InMemoryDiskBacked(String name) {
		m = new DataMap[this.concurrencyLevel];
		this.name = name;
		for (int i = 0; i < this.concurrencyLevel; i++)
			m[i] = new DataMap<>(i, name);

	}

	private int getBucket(K key) {
		int bucket = key.hashCode() % concurrencyLevel;
		return bucket;
	}

	public V get(K key) {

		return m[getBucket(key)].get(key);
	}

	public void put(K key, V value) {
		m[getBucket(key)].put(key, value);
	}

	public void delete(K key) {
		m[getBucket(key)].delete(key);
	}

	public void clear() {
		for (int i = 0; i < concurrencyLevel; i++)
			m[i].clear();

	}

	public long size() {
		int total = 0;
		for (int i = 0; i < concurrencyLevel; i++)
			total += m[i].size();

		return total;
	}

}
