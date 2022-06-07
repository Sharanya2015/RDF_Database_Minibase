package bpiterator;

import heap.*;
import global.*;
import java.io.*;
import labelheap.*;

/**
 * some useful method when processing Basic Pattern
 */
public class BPUtils {



    /**
   * set up a basic pattern - tuple in specified field from a tuple
   * 
   * @param value   the tuple to be set
   * @param tuple   the given tuple
   * @param fld_no  the field number
   * @param fldType the tuple attr type
   * @exception UnknowAttrType      don't know the attribute type
   * @exception IOException         some I/O fault
   * @exception TupleUtilsException exception from this class
   */
  public static void SetValue(Tuple value, Tuple tuple, int fld_no, AttrType fldType)
      throws IOException,
      UnknownAttrType,
      BPUtilsException {

    switch (fldType.attrType) {
      case AttrType.attrInteger: // integer
        try {
          value.setIntFld(fld_no, tuple.getIntFld(fld_no));
          value.setIntFld(fld_no + 1, tuple.getIntFld(fld_no + 1));
        } catch (FieldNumberOutOfBoundException e) {
          throw new BPUtilsException(e, "FieldNumberOutOfBoundException is caught by BPUtils.java");
        }
        break;

      case AttrType.attrDouble: //double
        try {
          value.setDoubleFld(fld_no, tuple.getDoubleFld(fld_no));
        } catch (FieldNumberOutOfBoundException e) {
          throw new BPUtilsException(e, "FieldNumberOutOfBoundException is caught by BPUtils.java");
        }
        break;

      default:
        throw new UnknownAttrType(null, "Can't attrSymbol, attrNull");
    }
    return;
  }


  /**
   * This function compares a tuple with another tuple in respective field, and
   * returns:
   *
   * 0 if the two are equal,
   * 1 if the tuple is greater,
   * -1 if the tuple is smaller,
   *
   * @param fldType   the type of the field being compared.
   * @param t1        one tuple.
   * @param t2        another tuple.
   * @param t1_fld_no the field numbers in the tuples to be compared.
   * @param t2_fld_no the field numbers in the tuples to be compared.
   * @exception TupleUtilsException exception from this class
   * @return 0 if the two are equal,
   *         1 if the tuple is greater,
   *         -1 if the tuple is smaller,
   * @throws Exception
   * @throws LHFBufMgrException
   * @throws LHFDiskMgrException
   * @throws LHFException
   * @throws InvalidLabelSizeException
   * @throws InvalidSlotNumberException
   */
  public static int CompareTupleWithTuple(AttrType fldType,
      Tuple t1, int t1_fld_no,
      Tuple t2, int t2_fld_no)
      throws LabelInvalidSlotNumberException, 
      InvalidLabelSizeException,
      LHFException, 
      LHFDiskMgrException,
      LHFBufMgrException, 
      Exception {
    int t1_is, t1_ip;
    int t2_is, t2_ip;
    double t1_r, t2_r;
    String t1_s, t2_s;
    LabelHeapFile labelHF = SystemDefs.JavabaseDB.getEntityHandle();

    char[] cArr1 = new char[1]; // charecter arrays for string generation
    char[] cArr2 = new char[1];

    cArr1[0] = Character.MIN_VALUE;
    cArr2[0] = Character.MAX_VALUE;

    String s1 = new String(cArr1);
    String s2 = new String(cArr2);
    switch (fldType.attrType) {
      case AttrType.attrInteger: // Compare two integers.
        try {
          t1_is = t1.getIntFld(t1_fld_no);
          t1_ip = t1.getIntFld(t1_fld_no + 1);

          t2_is = t2.getIntFld(t2_fld_no);
          t2_ip = t2.getIntFld(t2_fld_no + 1);

          PageId t1PageId = new PageId(t1_ip);
          PageId t2PageId = new PageId(t2_ip);
          LID t1LabelId = new LID(t1PageId, t1_is);
          LID t2LabelId = new LID(t2PageId, t2_is);
          
          if (t1LabelId.pageNo.pid < 0)
          {
            t1_s = new String(s1);
          } else if (t1LabelId.pageNo.pid == Integer.MAX_VALUE)
          {
            t1_s = new String(s2);
          } else {
            t1_s = labelHF.getLabel(t1LabelId);
          }

          if (t2LabelId.pageNo.pid < 0)
          {
            t2_s = new String(s1);
          } else if (t2LabelId.pageNo.pid == Integer.MAX_VALUE)
          {
            t2_s = new String(s2);
          } else {
            t2_s = labelHF.getLabel(t2LabelId);
          }
          
        } catch (FieldNumberOutOfBoundException e) {
          throw new BPUtilsException(e, "Basic Pattern FieldNumberOutOfBoundException is caught by BPUtils.java");
        }

        if (t1_s.compareTo(t2_s) > 0)
        {
          return 1;
        } else if (t1_s.compareTo(t2_s) < 0)
        {
          return -1;
        }
        return 0;

      case AttrType.attrDouble: // Compare two Double attributes
        try {
          t1_r = t1.getDoubleFld(t1_fld_no);
          t2_r = t2.getDoubleFld(t2_fld_no);
        } catch (FieldNumberOutOfBoundException e) {
          throw new BPUtilsException(e, "Basic Pattern FieldNumberOutOfBoundException is caught by BPUtils.java");
        }
        if (t1_r == t2_r)
        {
          return 0;
        } else if (t1_r < t2_r)
        {
          return -1;
        } else if (t1_r > t2_r)
        {
          return 1;
        }

      default:
        throw new UnknownAttrType(null, "Don't know how to handle attrSymbol, attrNull");

    }
  }

  /**
   * This function compares tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   * @param fldType   the type of the field being compared.
   * @param t1        one tuple
   * @param value     another tuple.
   * @param t1_fld_no the field numbers in the tuples to be compared.
   * @return 0 if the two are equal,
   *         1 if the tuple is greater,
   *         -1 if the tuple is smaller,
   * @throws Exception
   * @throws LHFBufMgrException
   * @throws LHFDiskMgrException
   * @throws LHFException
   * @throws InvalidLabelSizeException
   * @throws InvalidSlotNumberException
   * @exception TupleUtilsException exception from this class
   */
  public static int CompareTupleWithValue(AttrType fldType,
      Tuple t1, int t1_fld_no,
      Tuple value)
      throws InvalidSlotNumberException, InvalidLabelSizeException, LHFException, LHFDiskMgrException,
      LHFBufMgrException, Exception {
    return CompareTupleWithTuple(fldType, t1, t1_fld_no, value, t1_fld_no);
  }


}
