/*  File EID.java   */

package global;

import java.io.*;

/**
 * class EID
 */

public class EID {

  /**
   * public int slotNo
   */
  public int slotNo;

  /**
   * public PageId pageNo
   */
  public PageId pageNo = new PageId();

  /**
   * default constructor of class
   */
  public EID() {
  }

  /**
   * constructor of class
   */
  public EID(LID lid) {
    pageNo = lid.pageNo;
    slotNo = lid.slotNo;
  }

  /**
   * make a copy of the given EID
   */
  public void copyEID(EID eid) {
    pageNo = eid.pageNo;
    slotNo = eid.slotNo;
  }

  /**
   * Write the EID into a byte array at offset
   * 
   * @param ary    the specified byte array
   * @param offset the offset of byte array to write
   * @exception java.io.IOException I/O errors
   */
  public void writeToByteArray(byte[] ary, int offset)
      throws java.io.IOException {
    Convert.setIntValue(slotNo, offset, ary);
    Convert.setIntValue(pageNo.pid, offset + 4, ary);
  }

  /**
   * Compares two EID object, i.e, this to the EID
   * 
   * @param EID EID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(EID eid) {

    if ((this.pageNo.pid == eid.pageNo.pid)
        && (this.slotNo == eid.slotNo))
      return true;
    else
      return false;
  }

  //returns the LID for entity
  public LID returnLID() {
    LID lid = new LID(pageNo, slotNo);
    return lid;
  }

}
