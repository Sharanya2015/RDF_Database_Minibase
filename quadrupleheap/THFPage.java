/* File THFPage.java */

package quadrupleheap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;

/**
 * Define constant values for INVALID_SLOT and EMPTY_SLOT
 */

interface ConstSlot {
    int INVALID_SLOT = -1;
    int EMPTY_SLOT = -1;
}

/**
 * Class Theap file page.
 * The design assumes that quadruples are kept compacted when
 * deletions are performed.
 */

public class THFPage extends Page
        implements ConstSlot, GlobalConst {

    public static final int SIZE_OF_SLOT = 4;
    public static final int DPFIXED = 4 * 2 + 3 * 4;

    public static final int SLOT_CNT = 0;
    public static final int USED_PTR = 2;
    public static final int FREE_SPACE = 4;
    public static final int TYPE = 6;
    public static final int PREV_PAGE = 8;
    public static final int NEXT_PAGE = 12;
    public static final int CUR_PAGE = 16;

    /*
     * Warning:
     * These items must all pack tight, (no padding) for
     * the current implementation to work properly.
     * Be careful when modifying this class.
     */

    /**
     * number of slots in use
     */
    private short slotCnt;

    /**
     * offset of first used byte by data records in data[]
     */
    private short usedPtr;

    /**
     * number of bytes free in data[]
     */
    private short freeSpace;

    /**
     * an arbitrary value used by subclasses as needed
     */
    private short type;

    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();

    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();

    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();

    /**
     * Default constructor
     */

    public THFPage() {
    }

    /**
     * Constructor of class THFPage
     * open a THFPage and make this THFpage point to the given page
     *
     * @param page the given page in Page type
     */

    public THFPage(Page page) {
        data = page.getpage();
    }

    /**
     * Constructor of class THFPage
     * open a existed Thfpage
     *
     * @param apage a page in buffer pool
     */

    public void openTHFpage(Page apage) {
        data = apage.getpage();
    }

    /**
     * Constructor of class THFPage
     * initialize a new page
     *
     * @param pageNo the page number of a new page to be initialized
     * @param apage  the Page to be initialized
     * @throws IOException I/O errors
     * @see Page
     */

    public void init(PageId pageNo, Page apage)
            throws IOException {
        data = apage.getpage();

        slotCnt = 0; // no slots in use
        Convert.setShortValue(slotCnt, SLOT_CNT, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        usedPtr = (short) MAX_SPACE; // offset in data array (grow backwards)
        Convert.setShortValue(usedPtr, USED_PTR, data);

        freeSpace = (short) (MAX_SPACE - DPFIXED); // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

    }

    /**
     * @return byte array
     */

    public byte[] getTHFpageArray() {
        return data;
    }

    /**
     * Dump contents of a page
     *
     * @throws IOException I/O errors
     */
    public void dumpPage()
            throws IOException {
        int i, n;
        int length, offset;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        usedPtr = Convert.getShortValue(USED_PTR, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("usedPtr= " + usedPtr);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("slotCnt= " + slotCnt);

        for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
            length = Convert.getShortValue(n, data);
            offset = Convert.getShortValue(n + 2, data);
            System.out.println("slotNo " + i + " offset= " + offset);
            System.out.println("slotNo " + i + " length= " + length);
        }

    }

    /**
     * @return PageId of previous page
     * @throws IOException I/O errors
     */
    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    /**
     * sets value of prevPage to pageNo
     *
     * @param pageNo page number for previous page
     * @throws IOException I/O errors
     */
    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    /**
     * @return page number of next page
     * @throws IOException I/O errors
     */
    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     *
     * @param pageNo page number for next page
     * @throws IOException I/O errors
     */
    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return page number of current page
     * @throws IOException I/O errors
     */
    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     *
     * @param pageNo page number for current page
     * @throws IOException I/O errors
     */
    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /**
     * @return the ype
     * @throws IOException I/O errors
     */
    public short getType()
            throws IOException {
        type = Convert.getShortValue(TYPE, data);
        return type;
    }

    /**
     * sets value of type
     *
     * @param valtype an arbitrary value
     * @throws IOException I/O errors
     */
    public void setType(short valtype)
            throws IOException {
        type = valtype;
        Convert.setShortValue(type, TYPE, data);
    }

    /**
     * @return slotCnt used in this page
     * @throws IOException I/O errors
     */
    public short getSlotCnt()
            throws IOException {
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        return slotCnt;
    }

    /**
     * sets slot contents
     *
     * @param slotno the slot number
     * @param length length of quadruple the slot contains
     * @param offset offset of quadruple
     * @throws IOException I/O errors
     */
    public void setSlot(int slotno, int length, int offset)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        Convert.setShortValue((short) length, position, data);
        Convert.setShortValue((short) offset, position + 2, data);
    }

    /**
     * @param slotno slot number
     * @return the length of quadruple the given slot contains
     * @throws IOException I/O errors
     */
    public short getSlotLength(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position, data);
        return val;
    }

    /**
     * @param slotno slot number
     * @return the offset of quadruple the given slot contains
     * @throws IOException I/O errors
     */
    public short getSlotOffset(int slotno)
            throws IOException {
        int position = DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position + 2, data);
        return val;
    }

    /**
     * inserts a new quadruple onto the page, returns QID of this quadruple
     *
     * @param quadruple a quadruple to be inserted
     * @return QID of quadruple, null if sufficient space does not exist
     * @throws IOException I/O errors
     */
    public QID insertQuadruple(byte[] quadruple)
            throws IOException {
        QID qid = new QID();

        int quadLen = quadruple.length;
        int spaceNeeded = quadLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.

        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        if (spaceNeeded > freeSpace) {
            return null;

        } else {

            // look for an empty slot
            slotCnt = Convert.getShortValue(SLOT_CNT, data);
            int i;
            short length;
            for (i = 0; i < slotCnt; i++) {
                length = getSlotLength(i);
                if (length == EMPTY_SLOT)
                    break;
            }

            if (i == slotCnt) // use a new slot
            {
                // adjust free space
                freeSpace -= spaceNeeded;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);

                slotCnt++;
                Convert.setShortValue(slotCnt, SLOT_CNT, data);

            } else {
                // reusing an existing slot
                freeSpace -= quadLen;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);
            }

            usedPtr = Convert.getShortValue(USED_PTR, data);
            usedPtr -= quadLen; // adjust usedPtr
            Convert.setShortValue(usedPtr, USED_PTR, data);

            // insert the slot info onto the data page
            setSlot(i, quadLen, usedPtr);

            // insert data onto the data page
            System.arraycopy(quadruple, 0, data, usedPtr, quadLen);
            curPage.pid = Convert.getIntValue(CUR_PAGE, data);
            qid.pageNo.pid = curPage.pid;
            qid.slotNo = i;
            return qid;
        }
    }

    /**
     * delete the quadruple with the specified qid
     *
     * @param qid the quadruple ID
     * @throws IOException                 I/O errors
     * @throws InvalidQSlotNumberException Invalid slot number
     */
    public void deleteQuadruple(QID qid)
            throws IOException,
            InvalidQSlotNumberException {
        int slotNo = qid.slotNo;
        short quadLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        // first check if the quadruple being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (quadLen > 0)) {
            // The quadruples always need to be compacted, as they are
            // not necessarily stored on the page in the order that
            // they are listed in the slot index.

            // offset of quadruple being deleted
            int offset = getSlotOffset(slotNo);
            usedPtr = Convert.getShortValue(USED_PTR, data);
            int newSpot = usedPtr + quadLen;
            int size = offset - usedPtr;

            // shift bytes to the right
            System.arraycopy(data, usedPtr, data, newSpot, size);

            // now need to adjust offsets of all valid slots that refer
            // to the left of the quadruple being removed. (by the size of the hole)

            int i, n, chkoffset;
            for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
                if ((getSlotLength(i) >= 0)) {
                    chkoffset = getSlotOffset(i);
                    if (chkoffset < offset) {
                        chkoffset += quadLen;
                        Convert.setShortValue((short) chkoffset, n + 2, data);
                    }
                }
            }

            // move used Ptr forward
            usedPtr += quadLen;
            Convert.setShortValue(usedPtr, USED_PTR, data);

            // increase freespace by size of hole
            freeSpace = Convert.getShortValue(FREE_SPACE, data);
            freeSpace += quadLen;
            Convert.setShortValue(freeSpace, FREE_SPACE, data);

            setSlot(slotNo, EMPTY_SLOT, 0); // mark slot free
        } else {
            throw new InvalidQSlotNumberException(null, "QUADRUPLEHEAPFILE: INVALID_SLOTNO");
        }
    }

    /**
     * @return QID of first record on page, null if page contains no quadruples.
     * @throws IOException I/O errors
     */
    public QID firstQuadruple()
            throws IOException {
        QID qid = new QID();
        // find the first non-empty slot

        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        int i;
        short length;
        for (i = 0; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                break;
        }

        if (i == slotCnt)
            return null;

        // found a non-empty slot

        qid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        qid.pageNo.pid = curPage.pid;

        return qid;
    }

    /**
     * @param curQid current quadruple ID
     * @return QID of next quadruple on the page, null if no more
     *         quadruples exist on the page
     * @throws IOException I/O errors
     */
    public QID nextQuadruple(QID curQid)
            throws IOException {
        QID qid = new QID();
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        int i = curQid.slotNo;
        short length;

        // find the next non-empty slot
        for (i++; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                break;
        }

        if (i >= slotCnt)
            return null;

        // found a non-empty slot

        qid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        qid.pageNo.pid = curPage.pid;

        return qid;
    }

    /**
     * copies out quadruple with QID qid into quadruple pointer.
     * <br>
     *
     * @param qid the quadruple ID
     * @return a Quadruple
     * @throws InvalidQSlotNumberException Invalid slot number
     * @throws IOException                 I/O errors
     * @see Quadruple
     */
    public Quadruple getQuadruple(QID qid)
            throws IOException,
            InvalidQSlotNumberException {
        short quadLen;
        short offset;
        byte[] quad;
        PageId pageNo = new PageId();
        pageNo.pid = qid.pageNo.pid;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = qid.slotNo;

        // length of quadruple being returned
        quadLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        if ((slotNo >= 0) && (slotNo < slotCnt) && (quadLen > 0)
                && (pageNo.pid == curPage.pid)) {
            offset = getSlotOffset(slotNo);
            quad = new byte[quadLen];
            System.arraycopy(data, offset, quad, 0, quadLen);
            Quadruple quadruple = new Quadruple(quad, 0, quadLen);
            return quadruple;
        } else {
            throw new InvalidQSlotNumberException(null, "QUADRUPLEHEAPFILE: INVALID_SLOTNO");
        }

    }

    /**
     * returns a quadruple with given QID qid.
     * <br>
     *
     * @param qid the quadruple ID
     * @return a quadruple
     * @throws InvalidQSlotNumberException Invalid slot number
     * @throws IOException                 I/O errors
     * @see Quadruple
     */
    public Quadruple returnQuadruple(QID qid)
            throws IOException,
            InvalidQSlotNumberException {
        short quadLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = qid.pageNo.pid;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = qid.slotNo;

        // length of quadruple being returned
        quadLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        if ((slotNo >= 0) && (slotNo < slotCnt) && (quadLen > 0)
                && (pageNo.pid == curPage.pid)) {

            offset = getSlotOffset(slotNo);
            Quadruple quadruple = new Quadruple(data, offset, quadLen);
            return quadruple;
        } else {
            throw new InvalidQSlotNumberException(null, "QUADRUPLEHEAPFILE: INVALID_SLOTNO");
        }

    }

    /**
     * returns the amount of available space on the page.
     *
     * @return the amount of available space on the page
     * @throws IOException I/O errors
     */
    public int available_space()
            throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return (freeSpace - SIZE_OF_SLOT);
    }

    /**
     * Determining if the page is empty
     *
     * @return true if the THFPage is has no quadruples in it, false otherwise
     * @throws IOException I/O errors
     */
    public boolean empty()
            throws IOException {
        int i;
        short length;
        // look for an empty slot
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        for (i = 0; i < slotCnt; i++) {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                return false;
        }

        return true;
    }

    /**
     * Compacts the slot directory on an THFPage.
     * WARNING -- this will probably lead to a change in the QIDs of
     * quadruples on the page. You CAN'T DO THIS on most kinds of pages.
     *
     * @throws IOException I/O errors
     */
    protected void compact_slot_dir()
            throws IOException {
        int current_scan_posn = 0; // current scan position
        int first_free_slot = -1; // An invalid position.
        boolean move = false; // Move a quadruple? -- initially false
        short length;
        short offset;

        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);

        while (current_scan_posn < slotCnt) {
            length = getSlotLength(current_scan_posn);

            if ((length == EMPTY_SLOT) && (move == false)) {
                move = true;
                first_free_slot = current_scan_posn;
            } else if ((length != EMPTY_SLOT) && (move == true)) {
                offset = getSlotOffset(current_scan_posn);

                // slot[first_free_slot].length = slot[current_scan_posn].length;
                // slot[first_free_slot].offset = slot[current_scan_posn].offset;
                setSlot(first_free_slot, length, offset);

                // Mark the current_scan_posn as empty
                // slot[current_scan_posn].length = EMPTY_SLOT;
                setSlot(current_scan_posn, EMPTY_SLOT, 0);

                // Now make the first_free_slot point to the next free slot.
                first_free_slot++;

                // slot[current_scan_posn].length == EMPTY_SLOT !!
                while (getSlotLength(first_free_slot) != EMPTY_SLOT) {
                    first_free_slot++;
                }
            }

            current_scan_posn++;
        }

        if (move == true) {
            // Adjust amount of free space on page and slotCnt
            freeSpace += SIZE_OF_SLOT * (slotCnt - first_free_slot);
            slotCnt = (short) first_free_slot;
            Convert.setShortValue(freeSpace, FREE_SPACE, data);
            Convert.setShortValue(slotCnt, SLOT_CNT, data);
        }
    }

}
