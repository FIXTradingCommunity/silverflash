package org.fixtrading.silverflash.fixp.messages;

/**
 * Reasons for terminating a session
 * 
 * @author Don Mendelson
 *
 */
public enum TerminationCode {

  /**
   * Finished sending
   */
  FINISHED((byte) 0),
  /**
   * Unknown error
   */
  UNSPECIFIED_ERROR((byte) 1),
  /**
   * Ranged of messages not available for retransmisstion
   */
  RE_REQUEST_OUT_OF_BOUNDS((byte) 2),
  /**
   * Concurrent RetransmitRequest received
   */
  RE_REQUEST_IN_PROGRESS((byte) 3);

  private final byte code;

  private TerminationCode(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static TerminationCode getTerminateCode(byte code) {
    switch (code) {
      case 0:
        return FINISHED;
      case 1:
        return UNSPECIFIED_ERROR;
      case 2:
        return RE_REQUEST_OUT_OF_BOUNDS;
      case 3:
        return RE_REQUEST_IN_PROGRESS;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
