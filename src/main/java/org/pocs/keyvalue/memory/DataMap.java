package org.pocs.keyvalue.memory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.log4j.Logger;

class DataReader<K, V> {
	final static Logger logger = Logger.getLogger(DataReader.class);
	
	FileInputStream fileReader = null;

	public DataReader(String fileName) {
		try {
			fileReader = new FileInputStream(fileName);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					fileReader.close();
				} catch (Exception e) {
					logger.error("Error closing file handle.",e);
				}
			}));
		} catch (FileNotFoundException e) {
			logger.error("Error: file not available.",e);
		}
	}

	@SuppressWarnings("all")
	public V readAll(BiConsumer c) {
		try (BufferedInputStream in = new BufferedInputStream(fileReader)) {

			while (in.available() != 0) {
				int keyLen;
				byte[] keyArr = getDataArr(in);
				K key = null;
				// Zero length key is to delete all keys
				if (keyArr.length != 0)
					key = (K) IoUtils.bytesToObj(keyArr);

				byte[] valArr = getDataArr(in);
				V val = null;
				// Zero length is for empty string
				if (valArr.length != 0)
					val = (V) IoUtils.bytesToObj(valArr);

				c.accept(key, val);
			}

		} catch (IOException e) {
			logger.error("Error: file reading file data.",e);
			throw new RuntimeException(e);
		}

		return null;
	}

	private byte[] getDataArr(BufferedInputStream in) throws IOException {
		byte[] keyLenArr = new byte[Integer.BYTES];
		in.read(keyLenArr);
		int keyLen = ByteBuffer.wrap(keyLenArr).getInt();

		byte[] keyArr = new byte[keyLen];
		in.read(keyArr);
		return keyArr;
	}
}

class DataWriter<K, V> {
	final static Logger logger = Logger.getLogger(DataWriter.class);
	FileOutputStream fileWriter = null;

	public DataWriter(String fileName) {
		try {
			fileWriter = new FileOutputStream(fileName, true);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					fileWriter.close();
				} catch (Exception e) {
					logger.error("Error closing file handle.",e);
				}
			}));
		} catch (FileNotFoundException e) {
			logger.error("Error: file not available.",e);
		}
	}

	public void write(K k, V v) {

		byte[] key = k == Constants.EMPTY ? Constants.EMPTY : IoUtils.objToBytes(k);
		byte[] value = v == Constants.EMPTY ? Constants.EMPTY : IoUtils.objToBytes(v);

		int totalLen = key.length + value.length;

		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + totalLen);
		buffer.putInt(key.length).put(key).putInt(value.length).put(value);

		try {
			fileWriter.write(buffer.array());
		} catch (IOException e) {
			logger.error("Error: file writing file data.",e);
			throw new RuntimeException(e);
		}
	}
}

@SuppressWarnings("all")
public class DataMap<K, V> {
	final static Logger logger = Logger.getLogger(DataWriter.class);
	Map<K, V> m = null;
	
	DataWriter writer = null;
	DataReader reader = null;
	int partition = 0;

	public DataMap(int i, String name) {
		partition = i;
		String fileName = name + i + "";
		writer = new DataWriter(fileName);
		reader = new DataReader(fileName);

		m = Collections.synchronizedMap(new HashMap<K, V>());
		File f = new File(fileName);
		if (!f.exists()) {
			try {
				logger.info("Creating new data back up file : " + fileName);
				f.createNewFile();
			} catch (IOException e) {
				logger.error("Error creating file "+ fileName +" , please check permissions/disk space");
				System.exit(0);
			}
		} else {
			reader.readAll(new BiConsumer<K, V>() {

				@Override
				public void accept(K k, V v) {
					if(k==null) 
						m.clear();
					else if (v == null)
						m.remove(k);
					else
						m.put(k, v);
				}
			});
			logger.info("Data Restoration Completed for partition "+ partition);
		}
	}

	public V get(K key) {
		return m.get(key);
	}

	public void put(K key, V value) {
		m.put(key, value);
		writer.write(key, value);
	}

	public void delete(K key) {
		// on removal of the key, just write the data as blank, on restore just remove
		// the key with blank data.
		m.remove(key);
		writer.write(key, Constants.EMPTY);
	}

	public void clear() {
		m.clear();
		writer.write(Constants.EMPTY, Constants.EMPTY);
	}

	public long size() {
		return m.size();
	}
}
