package org.pocs.keyvalue.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Test;
import org.pocs.keyvalue.KVStore;
import org.pocs.keyvalue.memory.InMemoryDiskBacked;

import junit.framework.TestCase;

public class KVStoreTest extends TestCase {

	@Test
	public final void testPuts() {
		KVStore<Integer, Integer> store = new InMemoryDiskBacked<Integer, Integer>("A");
		for (int i = 1000; i < 2000; i++)
			store.put(i, i);
		//creating a seperate copy and verifying ensures no data sharing
		KVStore<Integer, Integer> store1 = new InMemoryDiskBacked<Integer, Integer>("A");
		boolean outputMatched = true;
		int count = 0;
		for (int i = 1000; i < 2000; i++) {
			outputMatched &= (i == store1.get(i));
			count++;
		}
		assertTrue(outputMatched && count == 1000);
	}
	 
	@Test
	public final void testGets() {
		KVStore<Integer, Integer> store = new InMemoryDiskBacked<Integer, Integer>("AB");
		for (int i = 1000; i < 2000; i++)
			store.put(i, i);
		
		KVStore<Integer, Integer> store1 = new InMemoryDiskBacked<Integer, Integer>("AB");
		boolean outputMatched = true;
		int count = 0;
		for (int i = 1000; i < 2000; i++) {
			outputMatched &= (i == store1.get(i));
			count++;
		}
		assertTrue(outputMatched && count == 1000);

	}

	@Test
	public final void testGetsAbsentVals() {
		KVStore<Integer, Integer> store = new InMemoryDiskBacked<Integer, Integer>("AB");
		assertNull(store.get(100));
	}

	@Test
	public final void testConcurrentPuts() {
		KVStore<Integer, Integer> store = new InMemoryDiskBacked<Integer, Integer>("B");
		ExecutorService service = Executors.newFixedThreadPool(16);
		IntStream.range(1, 5).forEach(value -> service.execute(() -> {
			int startVal = value * 1000;

			for (int i = startVal; i < startVal + 1000; i++)
				store.put(i, i);
		}));

		service.shutdown();
		try {
			service.awaitTermination(10, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		KVStore<Integer, Integer> store1 = new InMemoryDiskBacked<Integer, Integer>("B");
		boolean outputMatched = true;
		int count = 0;
		for (int i = 1; i < 5; i++) {
			int startVal = i * 1000;
			for (int j =startVal; j < startVal + 1000; j++) {
				outputMatched &= (j == store1.get(j));
				count++;
			}
		}
		assertTrue(outputMatched && count == 4000);
	}
	@Test
	public final void testRemove() {
		KVStore<Integer, Integer> store = new InMemoryDiskBacked<Integer, Integer>("C");
		store.put(1000, 1000);
		store.delete(1000);
		
		KVStore<Integer, Integer> store1 = new InMemoryDiskBacked<Integer, Integer>("C");
		assertNull(store1.get(1000));
	}
	
	@Test
	public final void testClear() {
		KVStore<Integer, Integer> store = new InMemoryDiskBacked<Integer, Integer>("J");
		IntStream.range(1, 5).forEach(i-> store.put(i, i)); 
		store.clear();
		store.put(1, 1);
		
		KVStore<Integer, Integer> store1 = new InMemoryDiskBacked<Integer, Integer>("J");
		assertEquals(1, store1.size());
	}
}
