package cepw.rs.store;

import cepw.rs.io.ByteArrayDataOutputStream;
import cepw.rs.store.exception.RecordsFileException;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * The base implementation of a file store with {@link RandomAccessFile} performing the
 * underlying reading and writing operations.
 * <p>
 * The file store supports basic CRUD operation and handles concurrent by mutually exclusion
 * of CRUD methods via synchronization.
 */
/*
 * The file store will have the following structure:
 *
 *  File Header Region   =======================
 *                       | num of records      |
 *                       +---------------------+
 *                       | data start ptr      | ---+
 *  Index Region         ======================+    |
 *                       | key(0) + header(0)  |    |
 *                       +---------------------+    |
 *                       | key(1) + header(1)  |    |
 *                       +---------------------+    |
 *                       | ...                 |    |
 *                       +---------------------+    |
 *                       | key(i) + header(i)  | ---|---+
 *                       +---------------------+    |   |
 *                       | extra index space   |    |   |
 *                       |                     |    |   |
 *                       | ...                 |    |   |
 *  Record Data Region   ======================= <<-+   |
 *                       | record(0)           |        |
 *                       +---------------------+        |
 *                       | record(1)           |        |
 *                       +---------------------+        |
 *                       | ...                 |        |
 *                       +---------------------+        |
 *                       | record(i)           | <<-----+
 *  EOF                  =======================
 *
 *
 */
public abstract class BaseRecordsStore {

  /**
   * Current file pointer to the start of the record data.
   */
  private long dataStartPtr;

  /**
   * The {@link RandomAccessFile}
   */
  private RandomAccessFile file;

  /**
   * Creates a new database file, initializing the appropriate headers.
   * Enough space is allocated in the index for the specified initial capacity.
   *
   * @param storePath       the path of the store
   * @param initialCapacity the initial capacity of the store
   * @throws IOException          if store cannot be created
   * @throws RecordsFileException if store already exists
   */
  public BaseRecordsStore(String storePath, int initialCapacity) throws IOException, RecordsFileException {
    File f = new File(storePath);
    if (f.exists()) {
      throw new RecordsFileException("Store already exists: " + storePath);
    }
    file = new RandomAccessFile(f, "rw");

    /* Record Data Region starts were the (i+1)th index entry would start */
    dataStartPtr = getKeyPointerAtPosition(initialCapacity);
    setFileLength(dataStartPtr);
    writeNumRecordsHeader(0);
    writeDataStartPointerHeader(dataStartPtr);
  }

  /**
   * Opens an existing database file and initializes the dataStartPtr
   *
   * @param storePath  the path of the file store
   * @param accessMode can be "r", "rw", "rws" or "rwd" as defined in RandomAccessFile.
   * @throws IOException          if store cannot be opened
   * @throws RecordsFileException if store does not exist
   */
  public BaseRecordsStore(String storePath, String accessMode) throws IOException, RecordsFileException {
    File f = new File(storePath);
    if (!f.exists()) {
      throw new RecordsFileException("Store not found: " + storePath);
    }
    file = new RandomAccessFile(f, accessMode);
    dataStartPtr = readDataStartPointerHeader();
  }

  /**
   * @return an {@link Iterable} of the keys of all records in the store.
   */
  public abstract Iterable<String> keys();

  /**
   * @return the number or records in the store.
   */
  public abstract int size();

  /**
   * Checks if there is a record with the given key.
   *
   * @return {@link Boolean#TRUE} if the key exists in the store, {@link Boolean#FALSE} otherwise.
   */
  public abstract boolean exists(String key);

  /**
   * Adds the given record to the database.
   */
  public synchronized void insert(RecordWriter rw) throws RecordsFileException, IOException {
    String key = rw.getKey();
    if (exists(key)) {
      throw new RecordsFileException("Key exists: " + key);
    }
    ensureIndexSpace(size() + 1);
    RecordHeader newRecord = allocateRecord(key, rw.getDataSize());
    writeRecordData(newRecord, rw);
    addEntryToIndex(key, newRecord, size());
  }

  /**
   * Updates an existing record. If the new contents do not fit in the original record,
   * then the update is handled by deleting the old record and adding the new.
   */
  public synchronized void update(RecordWriter rw) throws RecordsFileException, IOException {
    RecordHeader header = getRecordHeader(rw.getKey());
    if (rw.getDataSize() > header.getDataCapacity()) {
      delete(rw.getKey());
      insert(rw);
    } else {
      writeRecordData(header, rw);
      writeRecordHeaderToIndex(header);
    }
  }

  /**
   * Reads a record.
   */
  public synchronized RecordReader read(String key) throws RecordsFileException, IOException {
    RecordHeader header = getRecordHeader(key);
    byte[] data = readRecordData(header);
    return new RecordReader(key, data);
  }

  /**
   * Deletes a record.
   */
  public synchronized void delete(String key) throws RecordsFileException, IOException {
    RecordHeader delRec = getRecordHeader(key);
    int currentNumRecords = size();

    if (getFileLength() == delRec.getDataPointer() + delRec.getDataCapacity()) {

      /* shrink file since this is the last record in the file */
      setFileLength(delRec.getDataPointer());
    } else {

      RecordHeader previous = getRecordHeader(delRec.getDataPointer() - 1);
      if (previous != null) {

        /* append space of deleted record onto previous record */
        previous.setDataCapacity(previous.getDataCapacity() + delRec.getDataCapacity());
        writeRecordHeaderToIndex(previous);
      } else {

        /* target record is first in the file and is deleted by adding its space to the second record */
        RecordHeader secondRecord = getRecordHeader(delRec.getDataPointer() + (long) delRec.getDataCapacity());
        byte[] data = readRecordData(secondRecord);
        secondRecord.setDataPointer(delRec.getDataPointer());
        secondRecord.setDataCapacity(secondRecord.getDataCapacity() + delRec.getDataCapacity());
        writeRecordData(secondRecord, data);
        writeRecordHeaderToIndex(secondRecord);
      }
    }
    deleteEntryFromIndex(key, delRec, currentNumRecords);
  }

  /**
   * Closes the file
   *
   * @throws IOException if error occurred during the close of the file
   */
  public synchronized void close() throws IOException {
    try {
      file.close();
    } finally {
      file = null;
    }
  }

  /**
   * Get the {@link RecordHeader} with the given key
   *
   * @return the {@link RecordHeader} of the record.
   * @throws RecordsFileException if the record does not meet the read/write criteria.
   */
  protected abstract RecordHeader getRecordHeader(String key) throws RecordsFileException;

  /**
   * Get the {@link RecordHeader} to which the target file pointer belongs - meaning the
   * specified location in the file is part of the record data of the returned {@link RecordHeader}
   * (O(n) mem accesses)
   *
   * @param filePointer the target {@link RecordHeader} file pointer location
   * @return the corresponding {@link RecordHeader}. Null if the location is not part of a record
   */
  protected abstract RecordHeader getRecordHeader(long filePointer);

  /**
   * Locates space for a new record of dataLength size and initializes a {@link RecordHeader}.
   *
   * @param key        the key of the record
   * @param dataLength the length of the record data
   * @return the {@link RecordHeader} for the record
   * @throws IOException if file space allocation failed
   */
  protected abstract RecordHeader allocateRecord(String key, int dataLength) throws RecordsFileException, IOException;

  /**
   * @return the allocated file length of the {@link RandomAccessFile}
   * @throws IOException if an I/O error occurs.
   */
  protected long getFileLength() throws IOException {
    return file.length();
  }

  /**
   * @param length the file length to allocate for the {@link RandomAccessFile}
   * @throws IOException if an I/O error occurs.
   */
  protected void setFileLength(long length) throws IOException {
    file.setLength(length);
  }

  /**
   * Reads the number of {@link RecordHeader}s from the file.
   *
   * @return the number of record headers from the file
   * @throws IOException if an I/O error occurs.
   */
  protected int readNumRecordsHeader() throws IOException {
    file.seek(RecordConstants.NUM_RECORDS_HEADER_LOCATION);
    return file.readInt();
  }

  /**
   * Writes the number of {@link RecordHeader}s to the file.
   *
   * @param numRecords the number of records
   * @throws IOException if an I/O error occurs.
   */
  protected void writeNumRecordsHeader(int numRecords) throws IOException {
    file.seek(RecordConstants.NUM_RECORDS_HEADER_LOCATION);
    file.writeInt(numRecords);
  }

  /**
   * Reads the data start pointer header from the file.
   *
   * @return the data start pointer header
   * @throws IOException if an I/O error occurs.
   */
  protected long readDataStartPointerHeader() throws IOException {
    file.seek(RecordConstants.DATA_START_HEADER_LOCATION);
    return file.readLong();
  }

  /**
   * Writes the data start pointer header to the file.
   *
   * @param dataStartPtr the data start pointer header
   * @throws IOException if an I/O error occurs.
   */
  protected void writeDataStartPointerHeader(long dataStartPtr) throws IOException {
    file.seek(RecordConstants.DATA_START_HEADER_LOCATION);
    file.writeLong(dataStartPtr);
  }

  /**
   * Returns a file pointer in the index region pointing to the first byte
   * in the key located at the given position.
   *
   * @param position the position of the key in the index region.
   * @return the file pointer of the record key.
   */
  protected long getKeyPointerAtPosition(int position) {
    return RecordConstants.FILE_HEADERS_REGION_LENGTH + (RecordConstants.INDEX_ENTRY_LENGTH * position);
  }

  /**
   * Returns a file pointer in the index region pointing to the first byte
   * in the {@link RecordHeader} located at the given position.
   *
   * @param position the position of the {@link RecordHeader} in the index region.
   * @return the file pointer of the {@link RecordHeader}.
   */
  protected long getRecordHeaderPointerAtPosition(int position) {
    return RecordConstants.FILE_HEADERS_REGION_LENGTH +
            (RecordConstants.INDEX_ENTRY_LENGTH * position) + RecordConstants.MAX_KEY_LENGTH;
  }

  /**
   * Reads the ith key from the index.
   *
   * @param position the position of the key in the index region.
   * @return the key of the record
   */
  protected String readKeyFromIndex(int position) throws IOException {
    file.seek(getKeyPointerAtPosition(position));
    return file.readUTF();
  }

  /**
   * Reads the ith record header from the index.
   *
   * @param position the position of the {@link RecordHeader} in the index region.
   * @return the {@link RecordHeader} of the record
   */
  protected RecordHeader readRecordHeaderFromIndex(int position) throws IOException {
    file.seek(getRecordHeaderPointerAtPosition(position));
    return RecordHeader.readHeader(file);
  }

  /**
   * Writes the ith record header to the index.
   *
   * @param header the {@link RecordHeader} to write to the index
   */
  protected void writeRecordHeaderToIndex(RecordHeader header) throws IOException {
    file.seek(getRecordHeaderPointerAtPosition(header.getIndexPosition()));
    header.write(file);
  }

  /**
   * Appends an entry to end of index. Assumes that ensureIndexSpace() has already been called.
   *
   * @param key               the key of the record
   * @param newRecord         the {@link RecordHeader} of the new record
   * @param currentNumRecords a reference of the current number of records, used when inserting new record to file
   * @throws IOException          if error occurred when saving record index to file
   * @throws RecordsFileException if the key size is bigger than the {@link RecordConstants#MAX_KEY_LENGTH}
   */
  protected void addEntryToIndex(String key, RecordHeader newRecord, int currentNumRecords)
          throws IOException, RecordsFileException {
    ByteArrayDataOutputStream temp = new ByteArrayDataOutputStream(RecordConstants.MAX_KEY_LENGTH);
    (new DataOutputStream(temp)).writeUTF(key);
    if (temp.size() > RecordConstants.MAX_KEY_LENGTH) {
      throw new RecordsFileException("Key is larger than permitted size of " + RecordConstants.MAX_KEY_LENGTH + " bytes");
    }
    file.seek(getKeyPointerAtPosition(currentNumRecords));
    temp.writeTo(file);
    file.seek(getRecordHeaderPointerAtPosition(currentNumRecords));
    newRecord.write(file);
    newRecord.setIndexPosition(currentNumRecords);
    writeNumRecordsHeader(currentNumRecords + 1);
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
  protected void deleteEntryFromIndex(String key, RecordHeader header, int currentNumRecords) throws IOException, RecordsFileException {
    if (header.getIndexPosition() != currentNumRecords - 1) {
      String lastKey = readKeyFromIndex(currentNumRecords - 1);
      RecordHeader last = getRecordHeader(lastKey);
      last.setIndexPosition(header.getIndexPosition());
      file.seek(getKeyPointerAtPosition(last.getIndexPosition()));
      file.writeUTF(lastKey);
      file.seek(getRecordHeaderPointerAtPosition(last.getIndexPosition()));
      last.write(file);
    }
    writeNumRecordsHeader(currentNumRecords - 1);
  }

  /**
   * Reads the record data for the given record header.
   *
   * @param header the {@link RecordHeader}
   * @return the data of the record in bytes
   * @throws IOException if an I/O error occurs.
   */
  protected byte[] readRecordData(RecordHeader header) throws IOException {
    byte[] data = new byte[header.getDataSize()];
    file.seek(header.getDataPointer());
    file.readFully(data);
    return data;
  }

  /**
   * Updates the contents of the given record. The header's data size is updated, but not
   * written to the file.
   *
   * @param header the {@link RecordHeader}
   * @param rw   the {@link RecordWriter} to write the record data
   * @throws IOException          if an I/O error occurs.
   * @throws RecordsFileException if the new data does not fit in the space allocated to the record.
   */
  protected void writeRecordData(RecordHeader header, RecordWriter rw) throws IOException, RecordsFileException {
    if (rw.getDataSize() > header.getDataCapacity()) {
      throw new RecordsFileException("Record data does not fit");
    }
    header.setDataSize(rw.getDataSize());
    file.seek(header.getDataPointer());
    rw.writeTo(file);
  }

  /**
   * Updates the contents of the given record. The header's data size is updated, but not
   * written to the file.
   *
   * @param header the {@link RecordHeader}
   * @param data   the data of the record in bytes
   * @throws IOException          if an I/O error occurs.
   * @throws RecordsFileException if the new data does not fit in the space allocated to the record.
   */
  protected void writeRecordData(RecordHeader header, byte[] data) throws IOException, RecordsFileException {
    if (data.length > header.getDataCapacity()) {
      throw new RecordsFileException("Record data does not fit");
    }
    header.setDataSize(data.length);
    file.seek(header.getDataPointer());
    file.write(data, 0, data.length);
  }

  /**
   * Checks to see if there is space for and additional index entry. If
   * not, space is created by moving records to the end of the file.
   *
   * @param requiredNumRecords the required record number
   * @throws RecordsFileException if the record data does not fit into the allocated data length
   * @throws IOException          if an I/O error occurs.
   */
  protected void ensureIndexSpace(int requiredNumRecords)
          throws RecordsFileException, IOException {

    int originalFirstDataCapacity;
    int currentNumRecords = size();
    long endIndexPtr = getKeyPointerAtPosition(requiredNumRecords);
    if (endIndexPtr > getFileLength() && currentNumRecords == 0) {
      setFileLength(endIndexPtr);
      dataStartPtr = endIndexPtr;
      writeDataStartPointerHeader(dataStartPtr);
      return;
    }
    while (endIndexPtr > dataStartPtr) {
      RecordHeader first = getRecordHeader(dataStartPtr);

      /* no entries exists */
      if (first == null) {
        return;
      }

      byte[] data = readRecordData(first);
      first.setDataPointer(getFileLength());

      /* If first.dataCapacity is set to the actual data count BEFORE resetting dataStartPtr,
         and there is free space in 'first', then dataStartPtr will not be reset to the start
         of the second record. Capture the capacity first and use it to perform the reset. */
      originalFirstDataCapacity = first.getDataCapacity();
      first.setDataCapacity(data.length);
      setFileLength(first.getDataPointer() + data.length);
      writeRecordData(first, data);
      writeRecordHeaderToIndex(first);
      dataStartPtr += originalFirstDataCapacity;
      writeDataStartPointerHeader(dataStartPtr);
    }
  }
}
