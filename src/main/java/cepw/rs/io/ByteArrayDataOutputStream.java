package cepw.rs.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Extends ByteArrayOutputStream to provide a way of writing the buffer to
 * a DataOutput without re-allocating it.
 */
public class ByteArrayDataOutputStream extends ByteArrayOutputStream {

  /**
   * Default constructor
   */
  public ByteArrayDataOutputStream() {
    super();
  }

  /**
   * Constructor with buffer size initialisation
   *
   * @param bufferSize the buffer size of the {@link ByteArrayOutputStream}
   */
  public ByteArrayDataOutputStream(int bufferSize) {
    super(bufferSize);
  }

  /**
   * Writes the full contents of the buffer to a  {@link DataOutput} stream.
   *
   * @param outputStream the {@link DataOutput} stream.
   * @throws IOException if an I/O error occurs.
   */
  public synchronized void writeTo(DataOutput outputStream) throws IOException {
    byte[] data = super.buf;
    int bufferSize = super.size();
    outputStream.write(data, 0, bufferSize);
  }

}
