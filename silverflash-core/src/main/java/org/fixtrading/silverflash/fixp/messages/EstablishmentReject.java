package org.fixtrading.silverflash.fixp.messages;

/**
 * Enumeration of rejection reasons for session establishment
 * 
 * @author Don Mendelson
 *
 */
public enum EstablishmentReject {

  /**
   * Establish request was not preceded by a Negotiation or session was finalized, requiring
   * renegotiation.
   */
  UNNEGOTIATED((byte) 0),
  /**
   * EstablishmentAck was already sent; Establish was redundant.
   */
  ALREADY_ESTABLISHED((byte) 1),
  /**
   * User is not authorized.
   */
  SESSION_BLOCKED((byte) 2),
  /**
   * Value is out of accepted range.
   */
  KEEPALIVE_INTERVAL((byte) 3),
  /**
   * Failed authentication because identity is not recognized, keys are invalid, or the user is not
   * authorized to use a particular service.
   */
  CREDENTIALS((byte) 4),
  /**
   * Any other reason that the server cannot establish a session
   */
  UNSPECIFIED((byte) 5);

  private final byte code;

  private EstablishmentReject(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static EstablishmentReject getReject(byte code) {
    switch (code) {
      case 0:
        return UNNEGOTIATED;
      case 1:
        return ALREADY_ESTABLISHED;
      case 2:
        return SESSION_BLOCKED;
      case 3:
        return KEEPALIVE_INTERVAL;
      case 4:
        return CREDENTIALS;
      case 5:
        return UNSPECIFIED;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
