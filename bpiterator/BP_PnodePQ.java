
package bpiterator;

import global.*;
import heap.*;
import java.io.*;

import labelheap.*;

/**
 * Implements a sorted binary tree.
 * abstract methods <code>enq</code> and <code>deq</code> are used to add
 * or remove elements from the tree.
 */
public abstract class BP_PnodePQ {
  /** number of elements in the tree */
  protected int count;

  /** the field number of the sorting field */
  protected int fld_no;

  /** the attribute type of the sorting field */
  protected AttrType fld_type;

  /** the sorting order (Ascending or Descending) */
  protected BPOrder sort_order;

  /**
   * class constructor, set <code>count</code> to <code>0</code>.
   */
  public BP_PnodePQ() {
    count = 0;
  }

  /**
   * insert an element in the tree in the correct order.
   * 
   * @param item the element to be inserted
   * @exception IOException    from lower layers
   * @exception UnknowAttrType <code>attrSymbol</code> or
   *                           <code>attrNull</code> encountered
   * @throws Exception
   * @throws LHFBufMgrException
   * @throws LHFDiskMgrException
   * @throws LHFException
   * @throws InvalidLabelSizeException
   * @throws InvalidSlotNumberException
   * @exception TupleUtilsException error in tuple compare routines
   */
  abstract public void enq(BP_Pnode item)
      throws IOException, 
      UnknownAttrType, 
      BPUtilsException, 
      LHFException, 
      LHFDiskMgrException, 
      InvalidSlotNumberException, 
      InvalidLabelSizeException,
      LHFBufMgrException, 
      Exception;

  /**
   * removes the minimum (Ascending) or maximum (Descending) element
   * from the tree.
   * 
   * @return the element removed, null if the tree is empty
   */
  abstract public BP_Pnode deq();

  /**
   * returns the number of elements in the tree.
   * 
   * @return number of elements in the tree.
   */
  public int length() {
    return count;
  }

  /**
   * tests whether the tree is empty
   * 
   * @return true if tree is empty, false otherwise
   */
  public boolean empty() {
    return count == 0;
  }

  /**
   * tests whether the two elements are equal.
   * 
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return <code>true</code> if <code>a == b</code>,
   *         <code>false</code> otherwise
   * @throws Exception
   * @throws LHFBufMgrException
   * @throws LHFDiskMgrException
   * @throws LHFException
   * @throws InvalidLabelSizeException
   * @throws InvalidSlotNumberException
   * @exception TupleUtilsException error in tuple compare routines
   */
  public boolean BP_PnodeEqual(BP_Pnode a, BP_Pnode b) throws InvalidSlotNumberException,
      InvalidLabelSizeException,
      LHFException,
      LHFDiskMgrException,
      LHFBufMgrException,
      Exception {
    return BP_PnodeCompare(a, b) == 0;
  }

  /**
   * compares two elements.
   * 
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return <code>0</code> if the two are equal,
   *         <code>1</code> if <code>a</code> is greater,
   *         <code>-1</code> if <code>b</code> is greater
   * @throws InvalidSlotNumberException
   * @throws LHFBufMgrException
   * @throws Exception
   * @throws LHFDiskMgrException
   * @throws LHFException
   * @throws InvalidLabelSizeException
   * @exception TupleUtilsException error in tuple compare routines
   */
  public int BP_PnodeCompare(BP_Pnode a, BP_Pnode b)
      throws InvalidSlotNumberException,
      LHFBufMgrException,
      LHFDiskMgrException,
      InvalidLabelSizeException,
      LHFException,
      Exception {
    int ans = BPUtils.CompareTupleWithTuple(fld_type, a.tuple, fld_no, b.tuple, fld_no);
    return ans;
  }

}
