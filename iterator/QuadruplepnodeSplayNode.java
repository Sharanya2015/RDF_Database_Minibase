package iterator;

public class QuadruplepnodeSplayNode {

    /** a reference to the element in the node */
    public Quadruplepnode       item;

    /** the left child pointer */
    public QuadruplepnodeSplayNode    lt;

    /** the right child pointer */
    public QuadruplepnodeSplayNode    rt;

    /** the parent pointer */
    public QuadruplepnodeSplayNode    par;

    /**
     * class constructor, sets all pointers to <code>null</code>.
     * @param h the element in this node
     */
    public QuadruplepnodeSplayNode(Quadruplepnode h)
    {
        item = h;
        lt = null;
        rt = null;
        par = null;
    }

    /**
     * class constructor, sets all pointers.
     * @param h the element in this node
     * @param l left child pointer
     * @param r right child pointer
     */
    public QuadruplepnodeSplayNode(Quadruplepnode h, QuadruplepnodeSplayNode l, QuadruplepnodeSplayNode r)
    {
        item = h;
        lt = l;
        rt = r;
        par = null;
    }

    /** a static dummy node for use in some methods */
    public static QuadruplepnodeSplayNode dummy = new QuadruplepnodeSplayNode(null);

}
