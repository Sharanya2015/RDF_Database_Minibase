package bpiterator;

/**
 * BPOrder class
 */
public class BPOrder {

  public static final int Ascending = 0;
  public static final int Descending = 1;
  public static final int Random = 2;

  public int bpOrder;

  /**
   * BPOrder Constructor
   * 
   * @param _bpOrder The possible ordering of the basic patterns
   */

  public BPOrder(int _bpOrder) {
    bpOrder = _bpOrder;
  }

  public String toString() {

    switch (bpOrder) {
      case Ascending:
        return "Ascending";
      case Descending:
        return "Descending";
      case Random:
        return "Random";
    }
    return ("Unexpected BasicPatternOrder " + bpOrder);
  }

}