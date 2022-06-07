
package bpiterator;
import global.*;
import java.io.*;

import labelheap.InvalidLabelSizeException;
import labelheap.LabelInvalidSlotNumberException;
import labelheap.LHFBufMgrException;
import labelheap.LHFDiskMgrException;
import labelheap.LHFException;

/**
 * Implements a sorted binary tree (extends class pnodePQ).
 * Implements the <code>enq</code> and the <code>deq</code> functions.
 */
public class BPpnodeSplayPQ extends BP_PnodePQ
{

  /** the root of the tree */
  protected BP_PnodeSplayNode   root;
  /*
  pnodeSplayNode*   leftmost();
  pnodeSplayNode*   rightmost();
  pnodeSplayNode*   pred(pnodeSplayNode* t);
  pnodeSplayNode*   succ(pnodeSplayNode* t);
  void            _kill(pnodeSplayNode* t);
  pnodeSplayNode*   _copy(pnodeSplayNode* t);
  */

  /**
   * class constructor, sets default values.
   */
  public BPpnodeSplayPQ() 
  {
    root = null;
    count = 0;
    fld_no = 0;
    fld_type = new AttrType(AttrType.attrInteger);
    sort_order = new BPOrder(BPOrder.Ascending);
  }

  /**
   * class constructor.
   * @param fldNo   the field number for sorting
   * @param fldType the type of the field for sorting
   * @param order   the order of sorting (Ascending or Descending)
   */  
  public BPpnodeSplayPQ(int fldNo, AttrType fldType, BPOrder order)
  {
    root = null;
    count = 0;
    fld_no   = fldNo;
    fld_type = fldType;
    sort_order = order;
  }

  /**
   * Inserts an element into the binary tree.
   * @param item the element to be inserted
 * @throws Exception 
 * @throws LHFBufMgrException 
 * @throws LHFDiskMgrException 
 * @throws LHFException 
 * @throws InvalidLabelSizeException 
 * @throws InvalidSlotNumberException 
 * @exception TupleUtilsException error in tuple compare routines
   */
  public void enq(BP_Pnode item) throws LabelInvalidSlotNumberException, InvalidLabelSizeException, LHFException, LHFDiskMgrException, LHFBufMgrException, Exception 
  {
    count ++;
    BP_PnodeSplayNode newnode = new BP_PnodeSplayNode(item);
    BP_PnodeSplayNode t = root;

    if (t == null) {
      root = newnode;
      return;
    }
    
    int comp = BP_PnodeCompare(item, t.item);
    
    BP_PnodeSplayNode l = BP_PnodeSplayNode.dummy;
    BP_PnodeSplayNode r = BP_PnodeSplayNode.dummy;
     
    boolean done = false;

    while (!done) {
      if ((sort_order.bpOrder == BPOrder.Ascending && comp >= 0) || (sort_order.bpOrder == BPOrder.Descending && comp <= 0)) {
	BP_PnodeSplayNode tr = t.r_ptr;
	if (tr == null) {
	  tr = newnode;
	  comp = 0;
	  done = true;
	}
	else comp = BP_PnodeCompare(item, tr.item);
	
	if ((sort_order.bpOrder == BPOrder.Ascending && comp <= 0) ||(sort_order.bpOrder == BPOrder.Descending && comp >= 0))  {
	  l.r_ptr = t; t.p_ptr = l;
	  l = t;
	  t = tr;
	}
	else {
	  BP_PnodeSplayNode trr = tr.r_ptr;
	  if (trr == null) {
	    trr = newnode;
	    comp = 0;
	    done = true;
	  }
	  else comp = BP_PnodeCompare(item, trr.item);
	  
	  if ((t.r_ptr = tr.l_ptr) != null) t.r_ptr.p_ptr = t;
	  tr.l_ptr = t; t.p_ptr = tr;
	  l.r_ptr = tr; tr.p_ptr = l;
	  l = tr;
	  t = trr;
	}
      } // end of if(comp >= 0)
      else {
	BP_PnodeSplayNode tl = t.l_ptr;
	if (tl == null) {
	  tl = newnode;
	  comp = 0;
	  done = true;
	}
	else comp = BP_PnodeCompare(item, tl.item);
	
	if ((sort_order.bpOrder == BPOrder.Ascending && comp >= 0) || (sort_order.bpOrder == BPOrder.Descending && comp <= 0)) {
	  r.l_ptr = t; t.p_ptr = r;
	  r = t;
	  t = tl;
	}
	else {
	  BP_PnodeSplayNode tll = tl.l_ptr;
	  if (tll == null) {
	    tll = newnode;
	    comp = 0;
	    done = true;
	  }
	  else comp = BP_PnodeCompare(item, tll.item);
	  
	  if ((t.l_ptr = tl.r_ptr) != null) t.l_ptr.p_ptr = t;
	  tl.r_ptr = t; t.p_ptr = tl;
	  r.l_ptr = tl; tl.p_ptr = r;
	  r = tl;
	  t = tll;
	}
      } // end of else
    } // end of while(!done)
    
    if ((r.l_ptr = t.r_ptr) != null) r.l_ptr.p_ptr = r;
    if ((l.r_ptr = t.l_ptr) != null) l.r_ptr.p_ptr = l;
    if ((t.l_ptr = BP_PnodeSplayNode.dummy.r_ptr) != null) t.l_ptr.p_ptr = t;
    if ((t.r_ptr = BP_PnodeSplayNode.dummy.l_ptr) != null) t.r_ptr.p_ptr = t;
    t.p_ptr = null;
    root = t;
	    
    return; 
  }
  
  /**
   * Removes the minimum (Ascending) or maximum (Descending) element.
   * @return the element removed
   */
  public BP_Pnode deq() 
  {
    if (root == null) return null;
    
    count --;
    BP_PnodeSplayNode t = root;
    BP_PnodeSplayNode l = root.l_ptr;
    if (l == null) {
      if ((root = t.r_ptr) != null) root.p_ptr = null;
      return t.item;
    }
    else {
      while (true) {
	BP_PnodeSplayNode ll = l.l_ptr;
	if (ll == null) {
	  if ((t.l_ptr = l.r_ptr) != null) t.l_ptr.p_ptr = t;
	  return l.item;
	}
	else {
	  BP_PnodeSplayNode lll = ll.l_ptr;
	  if (lll == null) {
	    if((l.l_ptr = ll.r_ptr) != null) l.l_ptr.p_ptr = l;
	    return ll.item;
	  }
	  else {
	    t.l_ptr = ll; ll.p_ptr = t;
	    if ((l.l_ptr = ll.r_ptr) != null) l.l_ptr.p_ptr = l;
	    ll.r_ptr = l; l.p_ptr = ll;
	    t = ll;
	    l = lll;
	  }
	}
      } // end of while(true)
    } 
  }
  
  /*  
                  pnodeSplayPQ(pnodeSplayPQ& a);
  virtual       ~pnodeSplayPQ();

  Pix           enq(pnode  item);
  pnode           deq(); 

  pnode&          front();
  void          del_front();

  int           contains(pnode  item);

  void          clear(); 

  Pix           first(); 
  Pix           last(); 
  void          next(Pix& i);
  void          prev(Pix& i);
  pnode&          operator () (Pix i);
  void          del(Pix i);
  Pix           seek(pnode  item);

  int           OK();                    // rep invariant
  */
}
