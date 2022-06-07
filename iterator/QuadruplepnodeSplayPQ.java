
package iterator;
import global.*;
import java.io.*;

/**
 * Implements a sorted binary tree (extends class pnodePQ).
 * Implements the <code>enq</code> and the <code>deq</code> functions.
 */
public class QuadruplepnodeSplayPQ extends QuadruplepnodePQ
{

  /** the root of the tree */
  protected QuadruplepnodeSplayNode   root;

  /**
   * class constructor, sets default values.
   */
  public QuadruplepnodeSplayPQ()
  {
    root = null;
    count = 0;
    sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectPredicateObjectConfidence);
  }

  /**
   * class constructor.
   * @param order   the order of sorting (Ascending or Descending)
   */  
  public QuadruplepnodeSplayPQ(QuadrupleOrder order)
  {
    root = null;
    count = 0;
    sort_order = order;
  }

  /**
   * Inserts an element into the binary tree.
   * @param item the element to be inserted
   * @exception IOException from lower layers
   * @exception UnknowAttrType <code>attrSymbol</code> or 
   *                           <code>attrNull</code> encountered
   * @exception QuadrupleUtilsException error in quadruple compare routines
   */
  public void quadrupleenq(Quadruplepnode item) throws IOException, UnknowAttrType, QuadrupleUtilsException
  {
    count ++;
    QuadruplepnodeSplayNode newnode = new QuadruplepnodeSplayNode(item);
    QuadruplepnodeSplayNode t = root;

    if (t == null) {
      root = newnode;
      return;
    }
    
    int comp = QuadruplepnodeCMP(item, t.item);
    
    QuadruplepnodeSplayNode l = QuadruplepnodeSplayNode.dummy;
    QuadruplepnodeSplayNode r = QuadruplepnodeSplayNode.dummy;
     
    boolean done = false;

    while (!done) {
      if (comp >= 0) {
	      QuadruplepnodeSplayNode tr = t.rt;
	      if (tr == null) {
	        tr = newnode;
	        comp = 0;
	        done = true;
      	}
	      else comp = QuadruplepnodeCMP(item, tr.item);
	
	      if (comp <= 0) {
	        l.rt = t; t.par = l;
	        l = t;
	        t = tr;
      	}
      	else {
	        QuadruplepnodeSplayNode trr = tr.rt;
	        if (trr == null) {
	        trr = newnode;
	        comp = 0;
	        done = true;
	      }
	    else comp = QuadruplepnodeCMP(item, trr.item);
	  
	    if ((t.rt = tr.lt) != null) t.rt.par = t;
	      tr.lt = t; t.par = tr;
	      l.rt = tr; tr.par = l;
	      l = tr;
	      t = trr;
	    }
      } // end of if(comp >= 0)
      else {
	      QuadruplepnodeSplayNode tl = t.lt;
	      if (tl == null) {
	        tl = newnode;
	        comp = 0;
	        done = true;
	    }
	    else comp = QuadruplepnodeCMP(item, tl.item);
	
	    if (comp >= 0){
	      r.lt = t; t.par = r;
	      r = t;
	      t = tl;
	    }
	    else {
	      QuadruplepnodeSplayNode tll = tl.lt;
	      if (tll == null) {
	        tll = newnode;
	        comp = 0;
	        done = true;
	      }
	      else comp = QuadruplepnodeCMP(item, tll.item);
	  
	    if ((t.lt = tl.rt) != null) t.lt.par = t;
	    tl.rt = t; t.par = tl;
	    r.lt = tl; tl.par = r;
	    r = tl;
	    t = tll;
	    }
      } // end of else
    } // end of while(!done)
    
    if ((r.lt = t.rt) != null) r.lt.par = r;
    if ((l.rt = t.lt) != null) l.rt.par = l;
    if ((t.lt = QuadruplepnodeSplayNode.dummy.rt) != null) t.lt.par = t;
    if ((t.rt = QuadruplepnodeSplayNode.dummy.lt) != null) t.rt.par = t;
    t.par = null;
    root = t;
	    
    return; 
  }
  
  /**
   * Removes the minimum (Ascending) or maximum (Descending) element.
   * @return the element removed
   */
  public Quadruplepnode quadrupledeq()
  {
    if (root == null) return null;
    
    count --;
    QuadruplepnodeSplayNode t = root;
    QuadruplepnodeSplayNode l = root.lt;
    if (l == null) {
      if ((root = t.rt) != null) root.par = null;
      return t.item;
    }
    else {
      while (true) {
	      QuadruplepnodeSplayNode ll = l.lt;
	      if (ll == null) {
	      if ((t.lt = l.rt) != null) t.lt.par = t;
	      return l.item;
	      }
  	    else {
	        QuadruplepnodeSplayNode lll = ll.lt;
	      if (lll == null) {
	      if((l.lt = ll.rt) != null) l.lt.par = l;
	      return ll.item;
	  }
	  else {
	    t.lt = ll; ll.par = t;
	    if ((l.lt = ll.rt) != null) l.lt.par = l;
	    ll.rt = l; l.par = ll;
	    t = ll;
	    l = lll;
	  }
	}
      } // end of while(true)
    } 
  }

}
