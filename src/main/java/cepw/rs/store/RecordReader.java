package cepw.rs.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Helper wrapper to provide operations when reading a record from file
 */
public class RecordReader {

  /**
   * The key of the record
   */
  private String key;

  /**
   * The {@link ByteArrayInputStream} with the data of the record
   */
  private ByteArrayInputStream byteArrayInputStream;

  /**
   * The {@link ObjectInputStream} to convert the bytes read by the {@link #byteArrayInputStream}
   * into {@link Object}
   */
  private ObjectInputStream objectInputStream;

  /**
   * Constructor
   *
   * @param key  the key of the record
   * @param data the data of record
   */
  public RecordReader(String key, byte[] data) {
    this.key = key;
    byteArrayInputStream = new ByteArrayInputStream(data);
  }

  /**
   * Reads the next object byteArrayInputStream the record using an ObjectInputStream.
   */
  public Object readObject() throws IOException, ClassNotFoundException {
    if (objectInputStream == null) {
      objectInputStream = new ObjectInputStream(byteArrayInputStream);
    }
    return objectInputStream.readObject();
  }

  /**
   * @return the key of the record
   */
  public String getKey() {
    return key;
  }
}






