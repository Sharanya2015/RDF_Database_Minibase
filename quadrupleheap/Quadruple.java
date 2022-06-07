package quadrupleheap;

import java.io.*;
import java.lang.*;

import global.*;


public class Quadruple implements GlobalConst {


    /**
     * Maximum size of any quadruple
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte[] data;

    /**
     * start position of this quadruple in data[]
     */
    private int quadruple_offset;

    /**
     * length of this quadruple
     */
    private int quadruple_length;

    /**
     * private field
     * Number of fields in this quadruple
     */
    private short fldCnt;

    /**
     * private field
     * Array of offsets of the fields
     */

    private short[] fldOffset;

    /**
     * Class constructor
     * Creat a new quadruple with length = max_size,quadruple offset = 0.
     */
    private EID Subject;        //8 bytes
    private PID Predicate;      //8 bytes
    private EID Object;         //8 bytes
    private double value;       //8 bytes

    public Quadruple() {
        // Creat a new quadruple
        data = new byte[RDF_QUADRUPLE_SIZE];
        quadruple_offset = 0;
        quadruple_length = RDF_QUADRUPLE_SIZE;
    }

    /**
     * Constructor
     *
     * @param aquadruple a byte array which contains the quadruple
     * @param offset     the offset of the quadruple in the byte array
     * @param length     the length of the quadruple
     */

    public Quadruple(byte[] aquadruple, int offset, int length) {
        data = aquadruple;
        quadruple_offset = offset;
        quadruple_length = length;
        //  fldCnt = getShortValue(offset, data);
    }

    /**
     * Constructor(used as quadruple copy)
     *
     * @param fromQuadruple a byte array which contains the Quadruple
     */
    public Quadruple(Quadruple fromQuadruple) throws IOException {
        data = fromQuadruple.getQuadrupleByteArray();
        quadruple_length = fromQuadruple.getLength();
        quadruple_offset = 0;
        setConfidence(fromQuadruple.getConfidence());
        setSubjectID(fromQuadruple.getSubjectID());
        setPredicateID(fromQuadruple.getPredicateID());
        setObjectID(fromQuadruple.getObjectID());
        Subject.writeToByteArray(data, 0);
        Predicate.writeToByteArray(data, 8);
        Object.writeToByteArray(data, 16);
        Convert.setDoubleValue(fromQuadruple.getConfidence(), 24, data);

        //fldCnt = fromQuadruple.noOfFlds();
        //fldOffset = fromQuadruple.copyFldOffset();
    }

    /**
     * Class constructor
     * Creat a new quadruple with length = size,quadruple offset = 0.
     */

    public Quadruple(int size) {
        // Creat a new quadruple
        data = new byte[size];
        quadruple_offset = 0;
        quadruple_length = size;
    }

    /**
     * Copy a quadruple to the current quadruple position
     * you must make sure the quadruple lengths must be equal
     *
     * @param fromQuadruple the quadruple being copied
     */
    public void quadrupleCopy(Quadruple fromQuadruple) {
        try {
            byte[] temparray = fromQuadruple.getQuadrupleByteArray();
            System.arraycopy(temparray, 0, data, quadruple_offset, quadruple_length);
//          fldCnt = fromQuadruple.noOfFlds(); 
//          fldOffset = fromQuadruple.copyFldOffset(); 
            Subject = fromQuadruple.getSubjectID();
            Predicate = fromQuadruple.getPredicateID();
            Object = fromQuadruple.getObjectID();
            value = fromQuadruple.getConfidence();
        } catch (Exception e) {
            System.out.println("Error in copying");
        }

    }

    /**
     * This is used when you don't want to use the constructor
     *
     * @param aquadruple a byte array which contains the  uadruple
     * @param offset     the offset of the quadruple in the byte array
     * @param length     the length of the quadruple
     */

    public void quadrupleInit(byte[] aquadruple, int offset, int length) {
        data = aquadruple;
        quadruple_offset = offset;
        quadruple_length = length;
    }

    public EID getSubjectID() {
        EID subjectid = new EID();
        try {
            subjectid.slotNo = Convert.getIntValue(0, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            subjectid.pageNo = new PageId(Convert.getIntValue(4, data));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return subjectid;
    }

    public PID getPredicateID() {
        PID predicateid = new PID();
        try {
            predicateid.slotNo = Convert.getIntValue(8, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            predicateid.pageNo = new PageId(Convert.getIntValue(12, data));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return predicateid;
    }

    public EID getObjectID() {
        EID objectid = new EID();
        try {
            objectid.slotNo = Convert.getIntValue(16, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            objectid.pageNo = new PageId(Convert.getIntValue(20, data));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return objectid;
    }

    public double getConfidence() {
        double val = 0.0;
        try {
            val = Convert.getDoubleValue(24, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return val;
    }

    public void setSubjectID(EID subjectID) throws IOException {
        Subject = subjectID;
        Subject.writeToByteArray(data, 0);
    }

    public void setPredicateID(PID predicateID) throws IOException {
        Predicate = predicateID;
        Predicate.writeToByteArray(data, 8);
    }

    public void setObjectID(EID objectID) throws IOException {
        Object = objectID;
        Object.writeToByteArray(data, 16);
    }

    public void setConfidence(double confidence) throws IOException {
        value = confidence;
        Convert.setDoubleValue(confidence, 24, data);
    }

    /**
     * Set a quadruple with the given quadruple length and offset
     *
     * @param quadruple a byte array contains the quadruple
     * @param offset the offset of the quadruple ( =0 by default)
     * @param length the length of the quadruple
     */
    public void quadrupleSet(byte[] quadruple, int offset, int length) {
        System.arraycopy(quadruple, offset, data, 0, RDF_QUADRUPLE_SIZE);
        quadruple_offset = 0;
        quadruple_length = length;
    }

    /**
     * get the length of a quadruple, call this method if you did not
     * call setHdr () before
     *
     * @return length of this quadruple in bytes
     */
    public int getLength() {
        return quadruple_length;
    }

    /**
     * get the length of a quadruple, call this method if you did
     * call setHdr () before
     *
     * @return size of this quadruple in bytes
     */
    public short size() {
        return ((short) (quadruple_length));
    }

    /**
     * get the offset of a quadruple
     *
     * @return offset of the quadruple in byte array
     */
    public int getOffset() {
        return quadruple_offset;
    }

    /**
     * Copy the quadruple byte array out
     *
     * @return byte[], a byte array contains the quadruple
     * the length of byte[] = length of the quadruple
     */

    public byte[] getQuadrupleByteArray() {
        byte[] quadruplecopy = new byte[quadruple_length];
        System.arraycopy(data, quadruple_offset, quadruplecopy, 0, quadruple_length);
        return quadruplecopy;
    }

    /**
     * return the data byte array
     *
     * @return data byte array
     */

    public byte[] returnQuadrupleByteArray() {
        return data;
    }

    /**
     * Convert this field into integer
     *
     * @param fldNo the field number
     * @return the converted integer if success
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException Quadruple field number out of bound
     */

    public int getIntFld(int fldNo)
            throws IOException, QFieldNumberOutOfBoundException {
        int val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getIntValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field in to float
     *
     * @param fldNo the field number
     * @return the converted float number  if success
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException quadruple field number out of bound
     */

    public float getFloFld(int fldNo)
            throws IOException, QFieldNumberOutOfBoundException {
        float val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getFloValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");
    }


    /**
     * Convert this field into String
     *
     * @param fldNo the field number
     * @return the converted string if success
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException Quadruple field number out of bound
     */

    public String getStrFld(int fldNo)
            throws IOException, QFieldNumberOutOfBoundException {
        String val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getStrValue(fldOffset[fldNo - 1], data,
                    fldOffset[fldNo] - fldOffset[fldNo - 1]); //strlen+2
            return val;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field into a character
     *
     * @param fldNo the field number
     * @return the character if success
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException Quadruple field number out of bound
     */

    public char getCharFld(int fldNo)
            throws IOException, QFieldNumberOutOfBoundException {
        char val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getCharValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to integer value
     *
     * @param fldNo the field number
     * @param val   the integer value
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException Quadruple field number out of bound
     */

    public Quadruple setIntFld(int fldNo, int val)
            throws IOException, QFieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setIntValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Set this field to float value
     *
     * @param fldNo the field number
     * @param val   the float value
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException Quadruple field number out of bound
     */

    public Quadruple setFloFld(int fldNo, float val)
            throws IOException, QFieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setFloValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to String value
     *
     * @param fldNo the field number
     * @param val   the string value
     * @throws IOException                    I/O errors
     * @throws QFieldNumberOutOfBoundException Quadruple field number out of bound
     */

    public Quadruple setStrFld(int fldNo, String val)
            throws IOException, QFieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setStrValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new QFieldNumberOutOfBoundException(null, "QUADRUPLE:QUADRUPLE_FLDNO_OUT_OF_BOUND");
    }


    /**
     * setHdr will set the header of this quadruple.
     *
     * @param numFlds  number of fields
     * @param types    contains the types that will be in this quadruple
     * @param strSizes contains the sizes of the string
     * @throws IOException                   I/O errors
     * @throws InvalidQTypeException          Invalid quadruple type
     * @throws InvalidQuadrupleSizeException Quadruple size too big
     */

    public void setHdr(short numFlds, AttrType types[], short strSizes[])
            throws IOException, InvalidQTypeException, InvalidQuadrupleSizeException {
        if ((numFlds + 2) * 2 > max_size)
            throw new InvalidQuadrupleSizeException(null, "QUADRUPLE: QUADRUPLE_TOOBIG_ERROR");

        fldCnt = numFlds;
        Convert.setShortValue(numFlds, quadruple_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = quadruple_offset + 2;  // start position for fldOffset[]

        //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
        //another 1 for fldCnt
        fldOffset[0] = (short) ((numFlds + 2) * 2 + quadruple_offset);

        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2;
        short strCount = 0;
        short incr;
        int i;

        for (i = 1; i < numFlds; i++) {
            switch (types[i - 1].attrType) {

                case AttrType.attrInteger:
                    incr = 4;
                    break;

                case AttrType.attrReal:
                    incr = 4;
                    break;

                case AttrType.attrString:
                    incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                    strCount++;
                    break;

                default:
                    throw new InvalidQTypeException(null, "QUADRUPLE: QUADRUPLE_TYPE_ERROR");
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;

        }
        switch (types[numFlds - 1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr = 4;
                break;

            case AttrType.attrString:
                incr = (short) (strSizes[strCount] + 2);  //strlen in bytes = strlen +2
                break;

            default:
                throw new InvalidQTypeException(null, "QUADRUPLE: QUADRUPLE_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        quadruple_length = fldOffset[numFlds] - quadruple_offset;

        if (quadruple_length > max_size)
            throw new InvalidQuadrupleSizeException(null, "QUADRUPLE: QUADRUPLE_TOOBIG_ERROR");
    }


    /**
     * Returns number of fields in this quadruple
     *
     * @return the number of fields in this quadruple
     */

    public short noOfFlds() {
        return fldCnt;
    }

    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     */

    public short[] copyFldOffset() {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i = 0; i <= fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }

        return newFldOffset;
    }

    /**
     * Print out the quadruple
     *
     * @param type the types in the quadruple
     * @Exception IOException I/O exception
     */
    public void print(AttrType type[])
            throws IOException {
        int i, val;
        float fval;
        String sval;

        System.out.print("[");
        for (i = 0; i < fldCnt - 1; i++) {
            switch (type[i].attrType) {

                case AttrType.attrInteger:
                    val = Convert.getIntValue(fldOffset[i], data);
                    System.out.print(val);
                    break;

                case AttrType.attrReal:
                    fval = Convert.getFloValue(fldOffset[i], data);
                    System.out.print(fval);
                    break;

                case AttrType.attrString:
                    sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                    System.out.print(sval);
                    break;

                case AttrType.attrNull:
                case AttrType.attrSymbol:
                    break;
            }
            System.out.print(", ");
        }

        switch (type[fldCnt - 1].attrType) {

            case AttrType.attrInteger:
                val = Convert.getIntValue(fldOffset[i], data);
                System.out.print(val);
                break;

            case AttrType.attrReal:
                fval = Convert.getFloValue(fldOffset[i], data);
                System.out.print(fval);
                break;

            case AttrType.attrString:
                sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                System.out.print(sval);
                break;

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
        }
        System.out.println("]");

    }

    /**
     * private method
     * Padding must be used when storing different types.
     *
     * @param type   the type of quadruple
     * @param offset
     * @return short quadruple
     */

    private short pad(short offset, AttrType type) {
        return 0;
    }
}