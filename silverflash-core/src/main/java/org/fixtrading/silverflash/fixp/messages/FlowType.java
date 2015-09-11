package org.fixtrading.silverflash.fixp.messages;

/**
 * Type of message flow
 * 
 * @author Don Mendelson
 *
 */
public enum FlowType {

  /**
   * Unsequenced flow
   */
  UNSEQUENCED((byte) 0),
  /**
   * Idempotent flow
   */
  IDEMPOTENT((byte) 1),
  /**
   * Recoverable flow
   */
  RECOVERABLE((byte) 2),
  /**
   * No application messages flow in this direction. Part of a one-way session.
   */
  NONE((byte) 3);

  private final byte code;

  private FlowType(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static FlowType getFlowType(byte code) {
    switch (code) {
      case 0:
        return UNSEQUENCED;
      case 1:
        return IDEMPOTENT;
      case 2:
        return RECOVERABLE;
      case 3:
        return NONE;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
