package bpiterator;

import heap.*;
import global.*;
import java.io.*;

import bufmgr.*;
import iterator.*;
import basicpattern.*;

/**
 * open a heapfile and according to the condition expression to get
 * output file, call get_next to get all tuples
 */
public class BPFileScan extends BPIterator {
	public AttrType[] attrTypesArr;
	public short[] s_sizes;
	public int fieldsLength;
	private Heapfile f;
	private Scan scan;
	private Tuple tuple1;

	public BPFileScan(String file_name, int no_fields)
			throws IOException,
			FileScanException,
			TupleUtilsException,
			InvalidRelation {
		tuple1 = new Tuple();

		try {
			f = new Heapfile(file_name);
		} catch (Exception e) {
			throw new FileScanException(e, "Create new heapfile failed");
		}
		fieldsLength = no_fields;
		int attrLen = (fieldsLength - 1) * 2 + 1;
		attrTypesArr = new AttrType[attrLen];
		attrLen = attrLen - 1;
		int j = 0;
		while (j < attrLen) {
			attrTypesArr[j] = new AttrType(AttrType.attrInteger);
			j++;
		}
		attrTypesArr[j] = new AttrType(AttrType.attrDouble); // add double as last attribute

		s_sizes = new short[1];
		s_sizes[0] = (short) (attrLen * 4 + 1 * 8);
		try {
			tuple1.setHdr((short) (attrLen + 1), attrTypesArr, s_sizes);
		} catch (InvalidTypeException e1) {
			e1.printStackTrace();
		} catch (InvalidTupleSizeException e1) {
			e1.printStackTrace();
		}
		try {
			scan = f.openScan();
		} catch (Exception e) {
			throw new FileScanException(e, "openScan() failed in BPFileScan file");
		}
	}

	/**
	 * @return the result basic pattern
	 * @exception JoinsException                 some join exception
	 * @exception IOException                    I/O errors
	 * @exception InvalidTupleSizeException      invalid tuple size
	 * @exception InvalidTypeException           tuple type not valid
	 * @exception PageNotReadException           exception from lower layer
	 * @exception PredEvalException              exception from PredEval class
	 * @exception UnknowAttrType                 attribute type unknown
	 * @exception FieldNumberOutOfBoundException array out of bounds
	 * @exception WrongPermat                    exception for wrong FldSpec
	 *                                           argument
	 * @throws BasicPatternFieldNumberOutOfBoundException
	 */
	public BasicPattern get_next()
			throws JoinsException,
			IOException,
			InvalidTupleSizeException,
			InvalidTypeException,
			PageNotReadException,
			PredEvalException,
			UnknowAttrType,
			FieldNumberOutOfBoundException,
			WrongPermat, BasicPatternFieldNumberOutOfBoundException {
		RID rid = new RID();

		while (true) {
			if ((tuple1 = scan.getNext(rid)) == null) {
				return null;
			}
			int attrLen = (fieldsLength - 1) * 2 + 1;
			tuple1.setHdr((short) attrLen, attrTypesArr, s_sizes);
			short tupleSize = (tuple1.noOfFlds());

			BasicPattern basicPattern = new BasicPattern();
			try {
				basicPattern.setHdr((short) ((tupleSize) / 2 + 1));
			} catch (InvalidBasicPatternSizeException e) {
				e.printStackTrace();
			}

			int i = 0;
			int incrPtr = 0;

			for (i = 0, incrPtr = 1; i < (tupleSize / 2); i++) {
				int slotNo = tuple1.getIntFld(incrPtr++);
				int pageNo = tuple1.getIntFld(incrPtr++);

				LID labelId = new LID(new PageId(pageNo), slotNo);
				EID entityId = labelId.returnEID();
				basicPattern.setEIDForFld(i + 1, entityId);
			}

			basicPattern.setDoubleFld(i + 1, tuple1.getDoubleFld(incrPtr));
			return basicPattern;
		}
	}

	/**
	 * implement the abstract method close() from super class BPIterator
	 * to finish cleaning up
	 */
	public void close() {

		if (!closeFlag) {
			scan.closescan();
			closeFlag = true;
		}
	}

}
