package cepw.rs.store;

import cepw.rs.io.ByteArrayDataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Helper wrapper to provide operations when writing a record to file
 */
public class RecordWriter {

  /**
   * The key of the record
   */
  private String key;

  /**
   * The {@link ByteArrayOutputStream} for writing to file
   */
  private ByteArrayDataOutputStream byteArrayDataOutputStream;

  /**
   * The {@link ByteArrayDataOutputStream} for writing to file
   */
  private ObjectOutputStream objectOutputStream;

  /**
   * Constructor
   *
   * @param key the key of the record
   */
  public RecordWriter(String key) {
    this.key = key;
    byteArrayDataOutputStream = new ByteArrayDataOutputStream();
  }

  /**
   * Returns the key of the record
   *
   * @return the key of the record
   */
  public String getKey() {
    return key;
  }

  /**
   * Writes an {@link Object}
   *
   * @param obj the object to write.
   * @throws IOException if an I/O error occurs or if the given object is not serializable
   */
  public void writeObject(Object obj) throws IOException {

    if (objectOutputStream == null) {
      objectOutputStream = new ObjectOutputStream(byteArrayDataOutputStream);
    }
    objectOutputStream.writeObject(obj);
    objectOutputStream.flush();

  }

  /**
   * @return the number of bytes in the data.
   */
  public int getDataSize() {
    return byteArrayDataOutputStream.size();
  }

  /**
   * Writes the data byteArrayDataOutputStream to the stream without re-allocating the buffer.
   */
  public void writeTo(DataOutput stream) throws IOException {
    byteArrayDataOutputStream.writeTo(stream);
  }

  /**
   * Clear the buffer of the {@link RecordWriter}, allowing it to be reused
   *
   * @throws IOException if error occurred when resetting the output streams
   */
  public void clear() throws IOException {
    byteArrayDataOutputStream.reset();
    objectOutputStream = new ObjectOutputStream(byteArrayDataOutputStream);
  }
}






