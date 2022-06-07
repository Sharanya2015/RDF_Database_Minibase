package quadrupleheap;

/**
 * File DataPageInfo.java
 */

import global.*;

import java.io.*;

/**
 * QDataPageInfo class : the type of quadruple stored on a directory page.
 * <p>
 * April 9, 1998
 */

class QDataPageInfo implements GlobalConst {

    /**
     * QHFPage returns int for avail space, so we use int here
     */
    int availspace;

    /**
     * for efficient implementation of getRecCnt()
     */
    int quadct;

    /**
     * obvious: id of this particular data page (a QHFPage)
     */
    PageId pageId = new PageId();

    /**
     * auxiliary fields of QDataPageInfo
     */

    public static final int size = 12;// size of QDataPageInfo object in bytes

    private byte[] data; // a data buffer

    private int offset;

    /**
     * We can store roughly pagesize/sizeof(DataPageInfo) quadruple per
     * directory page; for any given QuadrupleHeapFile insertion, it is likely
     * that at least one of those referenced data pages will have
     * enough free space to satisfy the request.
     */

    /**
     * Default constructor
     */
    public QDataPageInfo() {
        data = new byte[12]; // size of Qdatapageinfo
        int availspace = 0;
        quadct = 0;
        pageId.pid = INVALID_PAGE;
        offset = 0;
    }

    /**
     * Constructor
     *
     * @param array a byte array
     */
    public QDataPageInfo(byte[] array) {
        data = array;
        offset = 0;
    }

    public byte[] returnByteArray() {
        return data;
    }

    /**
     * constructor: translate a quadruple to a QDataPageInfo object
     * it will make a copy of the data in the quadruple
     *
     * @param _aquadruple: the input quadruple
     */
    public QDataPageInfo(Quadruple _aquadruple)
            throws InvalidQuadrupleSizeException, IOException {
        // need check _aquadruple size == this.size ?otherwise, throw new exception
        if (_aquadruple.getLength() != 12) {
            throw new InvalidQuadrupleSizeException(null, "QUADRUPLEHEAPFILE: QUADRUPLE SIZE ERROR");
        } else {
            data = _aquadruple.returnQuadrupleByteArray();
            offset = _aquadruple.getOffset();

            availspace = Convert.getIntValue(offset, data);
            quadct = Convert.getIntValue(offset + 4, data);
            pageId = new PageId();
            pageId.pid = Convert.getIntValue(offset + 8, data);

        }
    }

    /**
     * convert this class objcet to a quadruple(like cast a QDataPageInfo to quadruple)
     */
    public Quadruple convertToQuadruple()
            throws IOException {

        // 1) write availspace, recct, pageId into data []
        Convert.setIntValue(availspace, offset, data);
        Convert.setIntValue(quadct, offset + 4, data);
        Convert.setIntValue(pageId.pid, offset + 8, data);

        // 2) create a quadruple object using this array
        Quadruple aquadruple = new Quadruple(data, offset, size);

        // 3) return quadruple object
        return aquadruple;

    }

    /**
     * write this object's useful fields(availspace, recct, pageId)
     * to the data[](may be in buffer pool)
     */
    public void flushToQuadruple() throws IOException {
        // write availspace, recct, pageId into "data[]"
        Convert.setIntValue(availspace, offset, data);
        Convert.setIntValue(quadct, offset + 4, data);
        Convert.setIntValue(pageId.pid, offset + 8, data);

        // here we assume data[] already points to buffer pool

    }

}
