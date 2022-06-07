
package bpiterator;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class BP_PnodeSplayNode {
  /** a reference to the element in the node */
  public BP_Pnode item;

  /** the left child pointer */
  public BP_PnodeSplayNode l_ptr;

  /** the right child pointer */
  public BP_PnodeSplayNode r_ptr;

  /** the parent pointer */
  public BP_PnodeSplayNode p_ptr;

    /**
   * class constructor, sets all pointers.
   * 
   * @param h the element in this node
   * @param l left child pointer
   * @param r right child pointer
   */
  public BP_PnodeSplayNode(BP_Pnode h, BP_PnodeSplayNode l, BP_PnodeSplayNode r) {
    item = h;
    l_ptr = l;
    r_ptr = r;
    p_ptr = null;
  }
  
  /**
   * class constructor, sets all pointers to <code>null</code>.
   * 
   * @param h the element in this node
   */
  public BP_PnodeSplayNode(BP_Pnode h) {
    item = h;
    l_ptr = null;
    r_ptr = null;
    p_ptr = null;
  }

  /** a static dummy node for use in some methods */
  public static BP_PnodeSplayNode dummy = new BP_PnodeSplayNode(null);

}
