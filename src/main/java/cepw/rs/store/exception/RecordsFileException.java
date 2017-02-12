package cepw.rs.store.exception;

/**
 * Exception for errors when record does not meet the read/write criteria
 */
public class RecordsFileException extends Exception {

  /**
   * The {@code serialVersionUID}
   */
  private static final long serialVersionUID = 8737992852532243510L;

  /**
   * Constructor
   *
   * @param msg the message for this {@link Exception}
   */
  public RecordsFileException(String msg) {
    super(msg);
  }
}
