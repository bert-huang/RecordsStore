package cepw.rs.store;

import cepw.rs.store.exception.RecordsFileException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link BaseRecordsStore} with in-memory index handling.
 */
public class MemIndexedRecordsStore extends BaseRecordsStore {

  /**
   * Hash table which holds the in-memory index. For efficiency, the entire
   * index is cached in memory. The hash table maps a key of type String to a
   * RecordHeader.
   */
  private Map<String, RecordHeader> memIndex;

  /**
   * Creates a new database file, initializing the appropriate headers.
   * Enough space is allocated in the index for the specified initial capacity.
   *
   * @param storePath       the path of the store
   * @param initialCapacity the initial capacity of the store
   * @throws IOException          if store cannot be created
   * @throws RecordsFileException if store already exists
   */
  public MemIndexedRecordsStore(String storePath, int initialCapacity) throws IOException,
          RecordsFileException {
    super(storePath, initialCapacity);
    memIndex = Collections.synchronizedMap(new HashMap<String, RecordHeader>(initialCapacity));
  }

  /**
   * Opens an existing database and initializes the in-memory index.
   *
   * @param storePath  the path of the file store
   * @param accessMode can be "r", "rw", "rws" or "rwd" as defined in RandomAccessFile.
   * @throws IOException          if store cannot be opened
   * @throws RecordsFileException if store does not exist
   */
  public MemIndexedRecordsStore(String storePath, String accessMode) throws IOException,
          RecordsFileException {
    super(storePath, accessMode);
    int numRecords = readNumRecordsHeader();
    memIndex = Collections.synchronizedMap(new HashMap<String, RecordHeader>());
    for (int i = 0; i < numRecords; i++) {
      String key = readKeyFromIndex(i);
      RecordHeader header = readRecordHeaderFromIndex(i);
      header.setIndexPosition(i);
      memIndex.put(key, header);
    }
  }

  /**
   * Get all the record keys in the store.
   *
   * @return the key set on the in-memory index.
   */
  @Override
  public Iterable<String> keys() {
    return memIndex.keySet();
  }

  /**
   * Get the number of record in the store.
   *
   * @return the size of the in-memory index.
   */
  @Override
  public int size() {
    return memIndex.size();
  }

  /**
   * Check the in-memory index if the given key exists.
   *
   * @param key the key of the record
   * @return {@link Boolean#TRUE} if the key exists in the store, {@link Boolean#FALSE} otherwise
   */
  @Override
  public boolean exists(String key) {
    return memIndex.containsKey(key);
  }

  /**
   * Get the {@link RecordHeader} with the given key from the in-memory index.
   *
   * @param key the key of the record.
   * @return the {@link RecordHeader} of the record.
   * @throws RecordsFileException if the key does not exist or points to a null record.
   */
  @Override
  protected RecordHeader getRecordHeader(String key)
          throws RecordsFileException {
    RecordHeader h = memIndex.get(key);
    if (h == null) {
      throw new RecordsFileException("Key not found: " + key);
    }
    return h;
  }

  /**
   * Get the {@link RecordHeader} to which the target file pointer belongs - meaning the
   * specified location in the file is part of the record data of the returned {@link RecordHeader}
   * (O(n) mem accesses)
   *
   * @param filePointer the target {@link RecordHeader} file pointer location
   * @return the corresponding {@link RecordHeader}. Null if the location is not part of a record
   */
  @Override
  protected RecordHeader getRecordHeader(long filePointer) {
    for (RecordHeader next : this.memIndex.values()) {
      if (filePointer >= next.getDataPointer()
              && filePointer < next.getDataPointer() + (long) next.getDataCapacity()) {
        return next;
      }
    }
    return null;
  }

  /**
   * Searches the file for free space, increase file length if necessary,
   * and returns a {@link RecordHeader} which uses the allocated space.
   * <p>
   * O(n) memory accesses
   *
   * @param key        the key of the record
   * @param dataLength the length of the record data
   * @return the {@link RecordHeader} for the record
   * @throws IOException if file space allocation failed
   */
  @Override
  protected RecordHeader allocateRecord(String key, int dataLength) throws IOException {
    /* Search for empty space within existing records */
    RecordHeader newRecord = null;
    for (RecordHeader next : this.memIndex.values()) {
      int free = next.getFreeSpace();
      if (dataLength <= free) {
        newRecord = next.split();
        writeRecordHeaderToIndex(next);
        break;
      }
    }

    /* If no suitable gap is found within the existing records,
       append to EOF. Grow file size if necessary */
    if (newRecord == null) {
      long fp = getFileLength();
      setFileLength(fp + dataLength);
      newRecord = new RecordHeader(fp, dataLength);
    }
    return newRecord;
  }

  /**
   * Adds the new record's {@link RecordHeader} to the in-memory index and calls the super class to add
   * the index entry to the file.
   *
   * @param key               the key of the record
   * @param newRecord         the {@link RecordHeader} of the new record
   * @param currentNumRecords a reference of the current number of records, used when inserting new record to file
   * @throws IOException          if error occurred when saving record index to file
   * @throws RecordsFileException if the key size is bigger than the {@link RecordConstants#MAX_KEY_LENGTH}
   */
  @Override
  protected void addEntryToIndex(String key, RecordHeader newRecord, int currentNumRecords)
          throws IOException, RecordsFileException {
    super.addEntryToIndex(key, newRecord, currentNumRecords);
    memIndex.put(key, newRecord);
  }

  /**
   * Removes the record from the index. Replaces the target with the entry at
   * the end of the index.
   *
   * @param key               the key of the record to remove
   * @param header            the record's {@link RecordHeader} to remove
   * @param currentNumRecords a reference of the current number of records, used when removing record from file
   * @throws IOException          if error occurred when removing record index to file
   * @throws RecordsFileException if key does not exist
   * @throws AssertionError       if the deleted {@link RecordHeader} reference is different to the intended {@link RecordHeader}
   */
  @Override
  protected void deleteEntryFromIndex(String key, RecordHeader header, int currentNumRecords)
          throws IOException, RecordsFileException {
    super.deleteEntryFromIndex(key, header, currentNumRecords);
    RecordHeader deleted = memIndex.remove(key);
    assert header == deleted;
  }

  /**
   * Closes the file
   *
   * @throws IOException if error occurred during the closing of the file
   */
  @Override
  public synchronized void close() throws IOException {
    try {
      super.close();
    } finally {
      memIndex.clear();
      memIndex = null;
    }
  }
}
