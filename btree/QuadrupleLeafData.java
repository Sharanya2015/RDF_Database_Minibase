package btree;

import global.*;
//import btree.*;

/**
 * IndexData: It extends the DataClass.
 * It defines the data "qid" for leaf node in B++ tree.
 */
public class QuadrupleLeafData extends DataClass {
  private QID myQid;

  public String toString() {
    String s;
    s = "[ " + (new Integer(myQid.pageNo.pid)).toString() + " "
        + (new Integer(myQid.slotNo)).toString() + " ]";
    return s;
  }

  /**
   * Class constructor
   * 
   * @param qid the data qid
   */
  public QuadrupleLeafData(QID qid) {
    myQid = new QID(qid.pageNo, qid.slotNo);
  };

  /**
   * get a copy of the qid
   * 
   * @return the reference of the copy
   */
  public QID getData() {
    return new QID(myQid.pageNo, myQid.slotNo);
  };

  /**
   * set the qid
   */
  public void setData(QID qid) {
    myQid = new QID(qid.pageNo, qid.slotNo);
  };
}