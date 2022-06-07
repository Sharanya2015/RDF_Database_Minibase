package iterator;


import global.LID;
import global.QuadrupleOrder;
import global.SystemDefs;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;

import java.io.IOException;

/**
 * some useful method when processing Quadruple
 */
public class QuadrupleUtils {

	private static int compareSubject(Quadruple q1, Quadruple q2, LabelHeapFile Elhf) {
		try {
			Label S1, S2;
			String q1_s, q2_s;
			LID lid1, lid2;
			char[] c = new char[1];
			c[0] = Character.MIN_VALUE;
			lid1 = q1.getSubjectID().returnLID();
			lid2 = q2.getSubjectID().returnLID();
			if (lid1.pageNo.pid < 0) q1_s = new String(c);
			else {
				q1_s = Elhf.getLabel(lid1);
			}
			if (lid2.pageNo.pid < 0) q2_s = new String(c);
			else {
				q2_s = Elhf.getLabel(lid2);
			}
			if (q1_s.compareTo(q2_s) > 0)
				return 1;
			if (q1_s.compareTo(q2_s) < 0)
				return -1;
			return 0;
		} catch (Exception e) {
			System.out.println("Exception" + e);
			return -2;
		}
	}

	private static int comparePredicate(Quadruple q1, Quadruple q2, LabelHeapFile Plhf) {
		try {
			Label P1, P2;
			String q1_s, q2_s;
			LID lid1, lid2;
			char[] c = new char[1];
			c[0] = Character.MIN_VALUE;
			lid1 = q1.getPredicateID().returnLID();
			lid2 = q2.getPredicateID().returnLID();
			if (lid1.pageNo.pid < 0) q1_s = new String(c);
			else {
				q1_s = Plhf.getLabel(lid1);
			}
			if (lid2.pageNo.pid < 0) q2_s = new String(c);
			else {
				q2_s = Plhf.getLabel(lid2);
			}
			if (q1_s.compareTo(q2_s) > 0)
				return 1;
			if (q1_s.compareTo(q2_s) < 0)
				return -1;
			return 0;
		} catch (Exception e) {
			System.out.println("Exception" + e);
			return -2;
		}
	}

	private static int compareObject(Quadruple q1, Quadruple q2, LabelHeapFile Elhf) {
		try {
			Label O1, O2;
			String q1_s, q2_s;
			LID lid1, lid2;
			char[] c = new char[1];
			c[0] = Character.MIN_VALUE;
			lid1 = q1.getObjectID().returnLID();
			lid2 = q2.getObjectID().returnLID();
			if (lid1.pageNo.pid < 0) q1_s = new String(c);
			else {
				q1_s = Elhf.getLabel(lid1);
			}
			if (lid2.pageNo.pid < 0) q2_s = new String(c);
			else {
				q2_s = Elhf.getLabel(lid2);
			}
			if (q1_s.compareTo(q2_s) > 0)
				return 1;
			if (q1_s.compareTo(q2_s) < 0)
				return -1;
			return 0;
		} catch (Exception e) {
			System.out.println("Exception" + e);
			return -2;
		}
	}

	private static int compareConfidence(Quadruple q1, Quadruple q2) {
		try {
			double q1_f, q2_f;
			q1_f = q1.getConfidence();
			q2_f = q2.getConfidence();
			if (q1_f < q2_f) return -1;
			if (q1_f > q2_f) return 1;
			return 0;
		} catch (Exception e) {
			System.out.println("Exception" + e);
			return -2;
		}
	}

	/**
	 * Compare one quadruple with another and return :
	 * <p>
	 * 0        if the two are equal,
	 * 1        if the quadruple is greater,
	 * -1        if the quadruple is smaller,
	 *
	 * @param q1 one quadruple.
	 * @param q2 another quadruple.
	 * @return 0        if the two are equal,
	 * 1        if the quadruple is greater,
	 * -1        if the quadruple is smaller,
	 * @throws UnknowAttrType          don't know the attribute type
	 * @throws IOException             some I/O fault
	 * @throws QuadrupleUtilsException exception from this class
	 */
	public static int CompareQuadrupleWithQuadruple(Quadruple q1, Quadruple q2, int quadruple_fld_no)
			throws IOException,
			UnknowAttrType,
			QuadrupleUtilsException {
		LabelHeapFile Elhf = SystemDefs.JavabaseDB.getEntityHandle();
		LabelHeapFile Plhf = SystemDefs.JavabaseDB.getPredicateHandle();

		switch (quadruple_fld_no) {
			case 1:
				return compareSubject(q1, q2, Elhf);

			case 2:
				return comparePredicate(q1, q1, Plhf);

			case 3:
				return compareObject(q1, q2, Elhf);

			case 4:
				return compareConfidence(q1, q2);


			default:
				throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

		}
	}


	public static int CompareQuadrupleWithQuadruple(QuadrupleOrder quadrupleOrder, Quadruple q1, Quadruple q2)
			throws IOException,
			UnknowAttrType,
			QuadrupleUtilsException {

		int val = -2;
		LabelHeapFile Elhf = SystemDefs.JavabaseDB.getEntityHandle();
		LabelHeapFile Plhf = SystemDefs.JavabaseDB.getPredicateHandle();

		switch (quadrupleOrder.quadrupleOrder) {
			case QuadrupleOrder.SubjectPredicateObjectConfidence:
				val = compareSubject(q1, q2, Elhf);
				if (val == 0) {
					val = comparePredicate(q1, q2, Plhf);
					if (val == 0) {
						val = compareObject(q1, q2, Elhf);
						if (val == 0) {
							val = compareConfidence(q1, q2);
						}
					}
				}
				return val;
			case QuadrupleOrder.PredicateSubjectObjectConfidence:
				val = comparePredicate(q1, q2, Plhf);
				if (val == 0) {
					val = compareSubject(q1, q2, Elhf);
					if (val == 0) {
						val = compareObject(q1, q2, Elhf);
						if (val == 0) {
							val = compareConfidence(q1, q2);
						}
					}
				}
				return val;
			case QuadrupleOrder.SubjectConfidence:
				val = compareSubject(q1, q2, Elhf);
				if (val == 0) {
					val = compareConfidence(q1, q2);
				}
				return val;
			case QuadrupleOrder.PredicateConfidence:
				val = comparePredicate(q1, q2, Plhf);
				if (val == 0) {
					val = compareConfidence(q1, q2);
				}
				return val;
			case QuadrupleOrder.ObjectConfidence:
				val = compareObject(q1, q2, Elhf);
				if (val == 0) {
					val = compareConfidence(q1, q2);
				}
				return val;
			case QuadrupleOrder.Confidence:
				val = compareConfidence(q1, q2);
				return val;
			default:
				return val;
		}

	}


	/**
	 * set up a quadruple in specified field from a quadruple
	 *
	 * @param val       the quadruple to be set
	 * @param quadruple the given quadruple
	 */
	public static void SetValue(Quadruple val, Quadruple quadruple)
			throws Exception {
		val.quadrupleCopy(quadruple);
	}

}




