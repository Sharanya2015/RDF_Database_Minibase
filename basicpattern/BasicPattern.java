/* File BasicPattern.java */
package basicpattern;

import java.io.*;
import global.*;
import heap.*;
import labelheap.*;

public class BasicPattern implements GlobalConst {

  /**
   * Maximum size of the basic pattern
   */
  public static final int max_size = MINIBASE_PAGESIZE;

  /**
   * a data byte array
   */
  private byte[] data;

  /**
   * start position of data[]
   */
  private int basicPattern_offset;

  /**
   * Basic Pattern Length
   */
  private int bp_length;

  /**
   * Basic pattern number of fields
   */
  private short fldCnt;

  /**
   * Array of offsets of the fields
   */

  private short[] fldOffset;

  public static SystemDefs sysdef = null;

  /**
   * Create a new basic pattern with length = max_size, offset = 0.
   */

  public BasicPattern() {
    // Creat a new basic pattern
    data = new byte[max_size];
    bp_length = max_size;
    basicPattern_offset = 0;
  }

  /**
   * @param abasicpattern a byte array containing the basic pattern
   * @param offset   the offset of the basic pattern
   * @param length   the length of the pattern
   */

  public BasicPattern(byte[] abasicpattern, int offset, int length) {
    data = abasicpattern;
    basicPattern_offset = offset;
    bp_length = length;
  }

  /**
   * Constructor(used as basic pattern copy)
   * @param fromBP a byte array containing the basic pattern
   */
  public BasicPattern(BasicPattern fromBP) {
    data = fromBP.getBasicPatternByteArray();
    bp_length = fromBP.getLength();
    basicPattern_offset = 0;
    fldCnt = fromBP.noOfFlds();
    fldOffset = fromBP.copyFldOffset();
  }

  /**
   * Class constructor
   * Creat a new basic pattern with length = size,basic pattern offset = 0.
   */

  public void SetBasicPatternArray(byte[] recordarray) {
    data = recordarray;
  }

  public BasicPattern(int size) {
    data = new byte[size];
    basicPattern_offset = 0;
    bp_length = size;
  }

  public BasicPattern(Tuple tuple) {

    data = new byte[max_size];
    basicPattern_offset = 0;
    bp_length = max_size;
    initBasicPatternForTuple(tuple);   
  }

  /**
   * Copy a basic pattern to the current basic pattern position
   * you must make sure the basic pattern lengths must be equal
   *
   * @param fromBP the basic pattern being copied
   */
  public void basicPatternCopy(BasicPattern fromBP) {
    byte[] temparray = fromBP.getBasicPatternByteArray();
    System.arraycopy(temparray, 0, data, basicPattern_offset, bp_length);

  }

  /**
   * This method initializes the basic pattern for the given tuple
   * @param tuple
   */
  public void initBasicPatternForTuple(Tuple tuple)
  {
    try {
      int no_tuple_flds = tuple.noOfFlds();
      setHdr((short) ((no_tuple_flds - 1) / 2 + 1));
      int j = 1;
      for (int i = 1; i < fldCnt; i++) {
        int slotno = tuple.getIntFld(j++);
        int pageno = tuple.getIntFld(j++);
        PageId page = new PageId(pageno);
        LID lid = new LID(page, slotno);
        EID eid = lid.returnEID();
        setEIDForFld(i, eid);
      }
      setDoubleFld(fldCnt, (double) tuple.getDoubleFld(j));
    } catch (Exception e) {
      System.out.println("Error initialising basic pattern from tuple" + e);
    }
  }



  /**
   * This is used when you don't want to use the constructor
   * 
   * @param abasicpattern a byte array which contains the pattern
   * @param offset   the offset of the pattern in the byte array
   * @param length   the length of the pattern
   */

  public void basicPatternInit(byte[] abasicpattern, int offset, int length) {
    data = abasicpattern;
    basicPattern_offset = offset;
    bp_length = length;
  }

  /**
   * Set a basic pattern with the given length and offset
   * 
   * @param basicPatternData a byte array contains the basic pattern
   * @param offset the offset of the basic pattern ( =0 by default)
   * @param length the length of the basic pattern
   */
  public void basicPatternSet(byte[] basicPatternData, int offset, int length) {
    System.arraycopy(basicPatternData, offset, data, 0, length);
    basicPattern_offset = 0;
    bp_length = length;
  }

  /**
   * get the length of a basic pattern, call this method if you did not
   * call setHdr () before
   * 
   * @return length of this basic pattern in bytes
   */
  public int getLength() {
    return bp_length;
  }

  /**
   * get the length of a basic pattern, call this method if you did
   * call setHdr () before
   * 
   * @return size of this pattern in bytes
   */
  public short size() {
    return ((short) (fldOffset[fldCnt] - basicPattern_offset));
  }

  /**
   * get the offset of a basic pattern
   * 
   * @return offset of the basic pattern in byte array
   */
  public int getOffset() {
    return basicPattern_offset;
  }

  /**
   * Copy the basic pattern byte array out
   * 
   * @return byte[], a byte array contains the basic pattern
   *         the length of byte[] = length of the basic pattern
   */

  public byte[] getBasicPatternByteArray() {
    byte[] basicPatternCopy = new byte[bp_length];
    System.arraycopy(data, basicPattern_offset, basicPatternCopy, 0, bp_length);
    return basicPatternCopy;
  }


  /**
   * return the data byte array for basic pattern
   * 
   * @return data byte array
   */

  public byte[] returnBasicPatternByteArray() {
    return data;
  }

  /**
   * get the EID field after coverting
   * 
   * @param fldNo the field number
   * @return the converted EID if success
   * 
   * @exception IOException                    I/O errors
   * @exception BasicPatternFieldNumberOutOfBoundException Basic pattern field number out of bound
   */

  public EID getEIDFld(int fldNo)
      throws IOException, BasicPatternFieldNumberOutOfBoundException {
    int pageNo, slotNo;
    if ((fldNo > 0) && (fldNo <= fldCnt)) {
      pageNo = Convert.getIntValue(fldOffset[fldNo - 1], data);
      slotNo = Convert.getIntValue(fldOffset[fldNo - 1] + 4, data);
      PageId page = new PageId();
      page.pid = pageNo;
      LID lid = new LID(page, slotNo);
      EID eid = new EID(lid);
      return eid;
    } else
      throw new BasicPatternFieldNumberOutOfBoundException(null, "ERROR: Basic Pattern OutofBound");
  }

  public Tuple getBasicPatternTuple() {
    Tuple new_tuple = new Tuple();
    int length = (fldCnt);
    AttrType[] attrTypes = new AttrType[(length - 1) * 2 + 1];
    int j = 0;
    for (j = 0; j < (length - 1) * 2; j++) {
      attrTypes[j] = new AttrType(AttrType.attrInteger);
    }
    attrTypes[j] = new AttrType(AttrType.attrDouble);
    short[] strSizes = new short[1];
    strSizes[0] = (short) ((length - 1) * 8 + 8);
    try {
      new_tuple.setHdr((short) ((length - 1) * 2 + 1), attrTypes, strSizes);
    } catch (Exception e) {
      e.printStackTrace();
    }
    int i = 0;
    j = 1;
    for (i = 0; i < fldCnt - 1; i++) {
      try {
        EID eid = getEIDFld(i + 1);
        new_tuple.setIntFld(j++, eid.slotNo);
        new_tuple.setIntFld(j++, eid.pageNo.pid);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    try {
      new_tuple.setDoubleFld(j, getDoubleFld(fldCnt));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new_tuple;
  }


  /**
   * Convert this field in to double
   *
   * @param fldNo the field number
   * @return the converted double number if success
   * 
   * @exception IOException                    I/O errors
   * @exception BasicPatternFieldNumberOutOfBoundException basic pattern field number out of bound
   */

  public double getDoubleFld(int fldNo)
      throws IOException, BasicPatternFieldNumberOutOfBoundException {
    double val;
    if ((fldNo > 0) && (fldNo <= fldCnt)) {
      val = Convert.getDoubleValue(fldOffset[fldNo - 1], data);
      return val;
    } else
      throw new BasicPatternFieldNumberOutOfBoundException(null, "ERROR : BP Field Out of Bound");
  }

  /**
   * setHdr will set the header of this basic pattern.
   *
   * @param numFlds number of nodes + confidence
   *
   * @exception IOException               I/O errors
   * @exception InvalidTypeException      Invalid tupe type
   * @exception InvalidTupleSizeException Tuple size too big
   *
   */

  public void setHdr(short numFlds) throws InvalidBasicPatternSizeException, IOException {
    if ((numFlds + 2) * 2 > max_size)
      throw new InvalidBasicPatternSizeException(null, "ERROR: Invalid Basic Pattern");

    fldCnt = numFlds;
    Convert.setShortValue(numFlds, basicPattern_offset, data);
    fldOffset = new short[numFlds + 1];
    int pos = basicPattern_offset + 2;

    fldOffset[0] = (short) ((numFlds + 2) * 2 + basicPattern_offset);

    Convert.setShortValue(fldOffset[0], pos, data);
    pos += 2;
    short confidence_size;
    int i;

    for (i = 1; i < numFlds; i++) {
      confidence_size = 8;
      fldOffset[i] = (short) (fldOffset[i - 1] + confidence_size);
      Convert.setShortValue(fldOffset[i], pos, data);
      pos += 2;

    }

    // For confidence
    confidence_size = 8;

    fldOffset[numFlds] = (short) (fldOffset[i - 1] + confidence_size);
    Convert.setShortValue(fldOffset[numFlds], pos, data);

    bp_length = fldOffset[numFlds] - basicPattern_offset;

    if (bp_length > max_size)
      throw new InvalidBasicPatternSizeException(null, "ERROR : Basic Pattern Invalid");
  }





  /**
   * Set this field to double value
   *
   * @param fldNo the field number
   * @param val   the double value
   * @exception IOException                    I/O errors
   * @exception BasicPatternFieldNumberOutOfBoundException Basic pattern field number out of bound
   */

  public BasicPattern setDoubleFld(int fldNo, double val)
      throws IOException, BasicPatternFieldNumberOutOfBoundException {
    if ((fldNo > 0) && (fldNo <= fldCnt)) {
      Convert.setDoubleValue(val, fldOffset[fldNo - 1], data);
      return this;
    } else
      throw new BasicPatternFieldNumberOutOfBoundException(null, "ERROR : Field out of bound");

  }


  /**
   * Returns number of fields in this basic pattern
   *
   * @return the number of fields in this basic pattern
   *
   */

  public short noOfFlds() {
    return fldCnt;
  }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   *
   */

  public short[] copyFldOffset() {
    short[] newFldOffset = new short[fldCnt + 1];
    for (int i = 0; i <= fldCnt; i++) {
      newFldOffset[i] = fldOffset[i];
    }

    return newFldOffset;
  }

  /**
   * Set this field to EID value
   *
   * @param fldNo the field number
   * @param val   the EID value
   * @exception IOException                    I/O errors
   * @exception BasicPatternFieldNumberOutOfBoundException Basic Pattern field number out of
   *                                           bound
   */

  public BasicPattern setEIDForFld(int fldNo, EID val)
          throws IOException, BasicPatternFieldNumberOutOfBoundException {
    if ((fldNo > 0) && (fldNo <= fldCnt)) {
      Convert.setIntValue(val.pageNo.pid, fldOffset[fldNo - 1], data);
      Convert.setIntValue(val.slotNo, fldOffset[fldNo - 1] + 4, data);
      return this;
    } else
      throw new BasicPatternFieldNumberOutOfBoundException(null, "ERROR : Field Out of Bound");
  }

  /**
   * Print out the basic pattern
   * 
   * @Exception IOException I/O exception
   */
  public void print()
      throws IOException {
    LabelHeapFile Entity_HF = sysdef.JavabaseDB.getEntityHandle();
    System.out.print("<<");
    try {
      for (int i = 1; i <= fldCnt - 1; i++) {
        String subject = Entity_HF.getLabel(this.getEIDFld(i).returnLID());
        System.out.printf("%20s  ", subject);
      }
      System.out.printf("%20s  ", getDoubleFld(fldCnt));
      System.out.println(">>");

    } catch (Exception e) {
      e.printStackTrace();
    } 

  }

  public void printIDs() {
    System.out.print("[");
    try {
      for (int i = 1; i <= fldCnt - 1; i++) {
        System.out.print("(" + this.getEIDFld(i).pageNo.pid + "," + this.getEIDFld(i).slotNo + ")");
      }
      System.out.print("Confidence:: " + getDoubleFld(fldCnt));
      System.out.println("]");

    } catch (Exception e) {
      System.out.println("Error printing BP" + e);
    }
  }

  public boolean findEID(EID eid) {
    boolean found = false;
    try {
      EID e = null;
      for (int i = 1; i <= fldCnt - 1; i++) {
        if (eid.equals(getEIDFld(i))) {
          found = true;
          break;
        }

      }
    } catch (Exception e) {
      System.out.print(e);

    }
    return found;
  }

}
