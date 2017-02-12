package cepw.rs.store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Internal Header object to hold record metadata that will be used for CRUD operations.
 */
public class RecordHeader {

  /**
   * File pointer to the first byte of record data (8 bytes).
   */
  private long dataPointer;

  /**
   * Actual number of bytes of data held in this record (4 bytes).
   */
  private int dataSize;

  /**
   * Number of bytes of data that this record can hold (4 bytes).
   */
  private int dataCapacity;

  /**
   * Indicates this header's position in the file index.
   */
  private int indexPosition;

  /**
   * Default constructor
   */
  private RecordHeader() {}

  /**
   * Constructor
   *
   * @param dataPointer  the data pointer for this {@link RecordHeader}.
   * @param dataCapacity the data capacity for this {@link RecordHeader}.
   * @throws IllegalArgumentException if data capacity is invalid (< 1).
   */
  public RecordHeader(long dataPointer, int dataCapacity) {
    if (dataCapacity < 1) {
      throw new IllegalArgumentException("Bad record size: " + dataCapacity);
    }
    this.dataPointer = dataPointer;
    this.dataCapacity = dataCapacity;
    this.dataSize = 0;
  }

  /**
   * Read the {@link RecordHeader} from a {@link DataInput} stream.
   *
   * @param in the {@link DataInput} stream.
   * @return the {@link RecordHeader}.
   * @throws IOException if an I/O error occurs.
   */
  public static RecordHeader readHeader(DataInput in) throws IOException {
    RecordHeader recordHeader = new RecordHeader();
    recordHeader.read(in);
    return recordHeader;
  }

  /**
   * @return the free space of this {@link RecordHeader} that may be allocated for others
   */
  public int getFreeSpace() {
    return dataCapacity - dataSize;
  }

  /**
   * Read as a single operation to avoid corruption.
   *
   * @param in the {@link DataInput}.
   * @throws IOException if an I/O error occurs.
   */
  private void read(DataInput in) throws IOException {
    byte[] header = new byte[RecordConstants.RECORD_HEADER_LENGTH];
    in.readFully(header);
    ByteBuffer buffer = ByteBuffer.allocate(RecordConstants.RECORD_HEADER_LENGTH);
    buffer.put(header);
    buffer.flip();

    dataPointer = buffer.getLong();
    dataCapacity = buffer.getInt();
    dataSize = buffer.getInt();
  }

  /**
   * In order to improve the likelihood of not corrupting the header, write as
   * a single operation.
   *
   * @param out the {@link DataOutput} stream.
   * @throws IOException if an I/O error occurs.
   */
  public void write(DataOutput out) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(RecordConstants.RECORD_HEADER_LENGTH);
    buffer.putLong(dataPointer);
    buffer.putInt(dataCapacity);
    buffer.putInt(dataSize);
    out.write(buffer.array(), 0, RecordConstants.RECORD_HEADER_LENGTH);

  }

  /**
   * Returns a new record header which occupies the free space of this record.
   * Shrinks this record size by the size of its free space.
   *
   * @return the new {@link RecordHeader} occupying the free space of this record.
   */
  public RecordHeader split() {
    long newFp = dataPointer + (long) dataSize;
    RecordHeader newRecord = new RecordHeader(newFp, getFreeSpace());
    dataCapacity = dataSize;
    return newRecord;
  }

  /**
   * @return the {@link #indexPosition}.
   */
  public int getIndexPosition() {
    return indexPosition;
  }

  /**
   * @param indexPosition the value to be set to {@link #indexPosition}.
   */
  public void setIndexPosition(int indexPosition) {
    this.indexPosition = indexPosition;
  }

  /**
   * @return the {@link #dataCapacity}.
   */
  public int getDataCapacity() {
    return dataCapacity;
  }

  /**
   * @param dataCapacity the value to be set to {@link #dataCapacity}.
   */
  public void setDataCapacity(int dataCapacity) {
    this.dataCapacity = dataCapacity;
  }

  /**
   * @return the {@link #dataPointer}.
   */
  public long getDataPointer() {
    return dataPointer;
  }

  /**
   * @param dataPointer the value to be set to {@link #dataPointer}.
   */
  public void setDataPointer(long dataPointer) {
    this.dataPointer = dataPointer;
  }

  /**
   * @return the {@link #dataSize}.
   */
  public int getDataSize() {
    return dataSize;
  }

  /**
   * @param dataSize the value to be set to {@link #dataSize}.
   */
  public void setDataSize(int dataSize) {
    this.dataSize = dataSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RecordHeader that = (RecordHeader) o;

    if (dataPointer != that.dataPointer) return false;
    if (dataSize != that.dataSize) return false;
    if (dataCapacity != that.dataCapacity) return false;
    return indexPosition == that.indexPosition;
  }

  @Override
  public int hashCode() {
    int prime = 31;
    int result = (int) (dataPointer ^ (dataPointer >>> 32));
    result = prime * result + dataSize;
    result = prime * result + dataCapacity;
    result = prime * result + indexPosition;
    return result;
  }

  @Override
  public String toString() {
    return "RecordHeader [" +
            "dataPointer=" + dataPointer + ", " +
            "dataSize=" + dataSize + ", " +
            "dataCapacity=" + dataCapacity + ", " +
            "indexPosition=" + indexPosition +
            "]";
  }
}