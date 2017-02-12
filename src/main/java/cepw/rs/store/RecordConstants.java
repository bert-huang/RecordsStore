package cepw.rs.store;

public class RecordConstants {

  /**
   * Total length in bytes of the global store headers
   */
  static final int FILE_HEADERS_REGION_LENGTH = 16;
  /**
   * Number of bytes in the record header
   */
  static final int RECORD_HEADER_LENGTH = 16;

  /**
   * The length of a key in the index
   */
  static final int MAX_KEY_LENGTH = 64;

  /**
   * The total length of one index entry = the key length + the record header length
   */
  static final int INDEX_ENTRY_LENGTH = MAX_KEY_LENGTH + RECORD_HEADER_LENGTH;

  /**
   * File pointer to the num records header
   */
  static final long NUM_RECORDS_HEADER_LOCATION = 0;

  /**
   * File pointer to the data start pointer header
   */
  static final long DATA_START_HEADER_LOCATION = 4;
}
