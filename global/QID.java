/*  File QID.java   */

package global;

import java.io.*;

/**
 * class QID
 */

public class QID {

  /**
   * public int slotNo
   */
  public int slotNo;

  /**
   * public PageId pageNo
   */
  public PageId pageNo = new PageId();

  // public int pid;

  /**
   * default constructor of class
   */
  public QID() {
  }

  /**
   * constructor of class
   */
  public QID(PageId pageno, int slotno) {
    pageNo = pageno;
    slotNo = slotno;
  }

  /**
   * make a copy of the given QID
   */
  public void copyQID(QID rid) {
    pageNo = rid.pageNo;
    slotNo = rid.slotNo;
  }

  /**
   * Write the QID into a byte array at offset
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
   * Compares two QID object, i.e, this to the QID
   * 
   * @param QID QID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(QID qid) {

    if ((this.pageNo.pid == qid.pageNo.pid)
        && (this.slotNo == qid.slotNo))
      return true;
    else
      return false;
  }

}
