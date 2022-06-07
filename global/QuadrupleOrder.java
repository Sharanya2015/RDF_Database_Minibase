package global;

/** 
 * Enumeration class for TupleOrder
 * 
 */

public class QuadrupleOrder {

  /*public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;*/

  public static final int SubjectPredicateObjectConfidence = 1;
	public static final int PredicateSubjectObjectConfidence = 2;
	public static final int SubjectConfidence = 3;
	public static final int PredicateConfidence = 4;
	public static final int ObjectConfidence = 5;
	public static final int Confidence = 6;

  public int quadrupleOrder;

  /** 
   * QuadrupleOrder Constructor
   * <br>
   * A quadruple ordering can be defined as 
   * <ul>
   * <li>   QuadrupleOrder quadrupleOrder = new QuadrupleOrder(QuadrupleOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (quadrupleOrder.quadrupleOrder == QuadrupleOrder.Random) ....
   * </ul>
   *
   * @param _quadrupleOrder The possible ordering of the quadruples 
   */

  public QuadrupleOrder (int _quadrupleOrder) {
    quadrupleOrder = _quadrupleOrder;
  }

  public String toString() {
    
    switch (quadrupleOrder) {
      case SubjectPredicateObjectConfidence:
				return "SubjectPredicateObjectConfidence";
			case PredicateSubjectObjectConfidence:
				return "PredicateSubjectObjectConfidence";
			case SubjectConfidence:
				return "SubjectConfidence";
			case PredicateConfidence:
				return "PredicateConfidence";
			case ObjectConfidence:
				return "ObjectConfidence";
			case Confidence:
				return "Confidence";
    }
    return ("Unexpected QuadrupleOrder " + quadrupleOrder);
  }
}
