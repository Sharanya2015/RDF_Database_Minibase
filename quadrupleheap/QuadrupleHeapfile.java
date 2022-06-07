package quadrupleheap;

import java.io.*;

import diskmgr.*;
import global.*;

/**
 * This quadrupleheapfile implementation is directory-based. We maintain a
 * directory of info about the data pages (which are of type THFPage
 * when loaded into memory).  The directory itself is also composed
 * of THFPages, with each quadruple being of type DataPageInfo
 * as defined below.
 * <p>
 * The first directory page is a header page for the entire database
 * (it is the one to which our filename is mapped by the DB).
 * All directory pages are in a doubly-linked list of pages, each
 * directory entry points to a single data page, which contains
 * the actual quadruples.
 * <p>
 * The quadrupleheapfile data pages are implemented as slotted pages, with
 * the slots at the front and the quadruples in the back, both growing
 * into the free space in the middle of the page.
 * <p>
 * We can store roughly pagesize/sizeof(DataPageInfo) quadruples per
 * directory page; for any given QuadrupleHeapFile insertion, it is likely
 * that at least one of those referenced data pages will have
 * enough free space to satisfy the request.
 */


/**
 * DataPageInfo class : the type of quadruples stored on a directory page.
 * <p>
 * April 9, 1998
 */


interface Filetype {
    int TEMP = 0;
    int ORDINARY = 1;

} // end of Filetype

public class QuadrupleHeapfile implements Filetype, GlobalConst {


    PageId _firstDirPageId;   // page number of header page
    int _ftype;
    private boolean _file_deleted;
    private String _fileName;
    private static int tempfilecount = 0;


    /* get a new datapage from the buffer manager and initialize dpinfo
       @param dpinfop the information in the new THFPage
    */
    private THFPage _newDatapage(QDataPageInfo dpinfop)
            throws QHFException,
            QHFBufMgrException,
            QHFDiskMgrException,
            IOException {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);

        if (pageId == null)
            throw new QHFException(null, "can't new page");

        // initialize internal values of the new page:

        THFPage hfpage = new THFPage();
        hfpage.init(pageId, apage);

        dpinfop.pageId.pid = pageId.pid;
        dpinfop.quadct = 0;
        dpinfop.availspace = hfpage.available_space();

        return hfpage;

    } // end of _newDatapage

    /* Internal QuadrupleHeapFile function (used in getQuadruple and updateQuadruple):
       returns pinned directory page and pinned data page of the specified
       user quadruple(qid) and true if quadruple is found.
       If the user quadruple cannot be found, return false.
    */
    private boolean _findDataPage(QID qid,
                                  PageId dirPageId, THFPage dirpage,
                                  PageId dataPageId, THFPage datapage,
                                  QID rpDataPageQid)
            throws InvalidQSlotNumberException,
            InvalidQuadrupleSizeException,
            QHFException,
            QHFBufMgrException,
            QHFDiskMgrException,
            Exception {
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        THFPage currentDirPage = new THFPage();
        THFPage currentDataPage = new THFPage();
        QID currentDataPageQid = new QID();
        PageId nextDirPageId = new PageId();
        // datapageId is stored in dpinfo.pageId


        pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

        Quadruple aquadruple = new Quadruple();

        while (currentDirPageId.pid != INVALID_PAGE) {// Start While01
            // ASSERTIONS:
            //  currentDirPage, currentDirPageId valid and pinned and Locked.

            for (currentDataPageQid = currentDirPage.firstQuadruple();
                 currentDataPageQid != null;
                 currentDataPageQid = currentDirPage.nextQuadruple(currentDataPageQid)) {
                try {
                    aquadruple = currentDirPage.getQuadruple(currentDataPageQid);
                } catch (InvalidQSlotNumberException e)// check error! return false(done)
                {
                    return false;
                }

                QDataPageInfo dpinfo = new QDataPageInfo(aquadruple);
                try {
                    pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);


                    //check error;need unpin currentDirPage
                } catch (Exception e) {
                    unpinPage(currentDirPageId, false/*undirty*/);
                    dirpage = null;
                    datapage = null;
                    throw e;
                }


                // ASSERTIONS:
                // - currentDataPage, currentDataPageQid, dpinfo valid
                // - currentDataPage pinned

                if (dpinfo.pageId.pid == qid.pageNo.pid) {
                    aquadruple = currentDataPage.returnQuadruple(qid);
                    // found user's quadruple on the current datapage which itself
                    // is indexed on the current dirpage.  Return both of these.

                    dirpage.setpage(currentDirPage.getpage());
                    dirPageId.pid = currentDirPageId.pid;

                    datapage.setpage(currentDataPage.getpage());
                    dataPageId.pid = dpinfo.pageId.pid;

                    rpDataPageQid.pageNo.pid = currentDataPageQid.pageNo.pid;
                    rpDataPageQid.slotNo = currentDataPageQid.slotNo;
                    return true;
                } else {
                    // user quadruple not found on this datapage; unpin it
                    // and try the next one
                    unpinPage(dpinfo.pageId, false /*undirty*/);

                }

            }

            // if we would have found the correct datapage on the current
            // directory page we would have already returned.
            // therefore:
            // read in next directory page:

            nextDirPageId = currentDirPage.getNextPage();
            try {
                unpinPage(currentDirPageId, false /*undirty*/);
            } catch (Exception e) {
                throw new QHFException(e, "quadrupleheapfile,_find,unpinpage failed");
            }

            currentDirPageId.pid = nextDirPageId.pid;
            if (currentDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
                if (currentDirPage == null)
                    throw new QHFException(null, "pinPage return null page");
            }


        } // end of While01
        // checked all dir pages and all data pages;quadruple not found:(

        dirPageId.pid = dataPageId.pid = INVALID_PAGE;

        return false;


    } // end of _findDatapage		     

    /**
     * Initialize.  A null name produces a temporary quadrupleheapfile which will be
     * deleted by the destructor.  If the name already denotes a file, the
     * file is opened; otherwise, a new empty file is created.
     *
     * @throws QHFException        quadrupleheapfile exception
     * @throws QHFBufMgrException  exception thrown from bufmgr layer
     * @throws QHFDiskMgrException exception thrown from diskmgr layer
     * @throws IOException        I/O errors
     */
    public QuadrupleHeapfile(String name)
            throws QHFException,
            QHFBufMgrException,
            QHFDiskMgrException,
            IOException {
        // Give us a prayer of destructing cleanly if construction fails.
        _file_deleted = true;
        _fileName = null;

        if (name == null) {
            // If the name is NULL, allocate a temporary name
            // and no logging is required.
            _fileName = "tempHeapFile";
            String useId = new String("user.name");
            String userAccName;
            userAccName = System.getProperty(useId);
            _fileName = _fileName + userAccName;

            String filenum = Integer.toString(tempfilecount);
            _fileName = _fileName + filenum;
            _ftype = TEMP;
            try
            {
                //System.out.println("Get file Entry::"+_fileName);
                PageId tempPageId = get_file_entry(_fileName);

                if(tempPageId != null)
                {
                    System.out.println(tempPageId.pid);
                    if(tempPageId.pid > 0)
                    {
                        delete_file_entry(_fileName);
                    }
                }
            }
            catch(Exception ex)
            {
                System.out.println("Exception occured while deleting::" + ex.getMessage());
            }
            tempfilecount++;

        } else {
            _fileName = name;
            _ftype = ORDINARY;
        }

        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized.  This case is detected via a failure
        // in the db->get_file_entry() call.  In the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file

        Page apage = new Page();
        _firstDirPageId = null;
        if (_ftype == ORDINARY)
            _firstDirPageId = get_file_entry(_fileName);

        if (_firstDirPageId == null) {
            // file doesn't exist. First create it.
            _firstDirPageId = newPage(apage, 1);
            // check error
            if (_firstDirPageId == null)
                throw new QHFException(null, "can't new page");

            add_file_entry(_fileName, _firstDirPageId);
            // check error(new exception: Could not add file entry

            THFPage firstDirPage = new THFPage();
            firstDirPage.init(_firstDirPageId, apage);
            PageId pageId = new PageId(INVALID_PAGE);

            firstDirPage.setNextPage(pageId);
            firstDirPage.setPrevPage(pageId);
            unpinPage(_firstDirPageId, true /*dirty*/);


        }
        _file_deleted = false;
        // ASSERTIONS:
        // - ALL private data members of class QuadrupleHeapfile are valid:
        //
        //  - _firstDirPageId valid
        //  - _fileName valid
        //  - no datapage pinned yet

    } // end of constructor 

    /**
     * Return number of quadruple in file.
     *
     * @throws InvalidQSlotNumberException invalid slot number
     * @throws InvalidQuadrupleSizeException  invalid quadruple size
     * @throws QHFBufMgrException          exception thrown from bufmgr layer
     * @throws QHFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public int getQuadCnt()
            throws InvalidQSlotNumberException,
            InvalidQuadrupleSizeException,
            QHFDiskMgrException,
            QHFBufMgrException,
            IOException {
        int answer = 0;
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        PageId nextDirPageId = new PageId(0);

        THFPage currentDirPage = new THFPage();
        Page pageinbuffer = new Page();

        while (currentDirPageId.pid != INVALID_PAGE) {
            pinPage(currentDirPageId, currentDirPage, false);

            QID qid = new QID();
            Quadruple aquadruple;
            for (qid = currentDirPage.firstQuadruple();
                 qid != null;    // qid==NULL means no more quadruple
                 qid = currentDirPage.nextQuadruple(qid)) {
                aquadruple = currentDirPage.getQuadruple(qid);
                QDataPageInfo dpinfo = new QDataPageInfo(aquadruple);

                answer += dpinfo.quadct;
            }

            // ASSERTIONS: no more quadruple
            // - we have read all datapage quadruples on
            //   the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false /*undirty*/);
            currentDirPageId.pid = nextDirPageId.pid;
        }

        // ASSERTIONS:
        // - if error, exceptions
        // - if end of quadrupleheapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of quadrupleheapfile: currentDirPageId valid


        return answer;
    } // end of getQuadCnt

    /**
     * Insert quadruple into file, return its Qid.
     *
     * @param quadruplePtr pointer of the quadruple
     * @return the qid of the quadruple
     * @throws InvalidQSlotNumberException invalid slot number
     * @throws InvalidQuadrupleSizeException  invalid quadruple size
     * @throws QSpaceNotAvailableException no space left
     * @throws QHFException                quadrupleheapfile exception
     * @throws QHFBufMgrException          exception thrown from bufmgr layer
     * @throws QHFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public QID insertQuadruple(byte[] quadruplePtr)
            throws InvalidQSlotNumberException,
            InvalidQuadrupleSizeException,
            QSpaceNotAvailableException,
            QHFException,
            QHFBufMgrException,
            QHFDiskMgrException,
            IOException {
        int dpinfoLen = 0;
        int recLen = quadruplePtr.length;
        boolean found;
        QID currentDataPageQid = new QID();
        Page pageinbuffer = new Page();
        THFPage currentDirPage = new THFPage();
        THFPage currentDataPage = new THFPage();

        THFPage nextDirPage = new THFPage();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId();  // OK

        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

        found = false;
        Quadruple aquadruple;
        QDataPageInfo dpinfo = new QDataPageInfo();
        while (found == false) { //Start While01
            // look for suitable dpinfo-struct
            for (currentDataPageQid = currentDirPage.firstQuadruple();
                 currentDataPageQid != null;
                 currentDataPageQid =
                         currentDirPage.nextQuadruple(currentDataPageQid)) {
                aquadruple = currentDirPage.getQuadruple(currentDataPageQid);

                dpinfo = new QDataPageInfo(aquadruple);

                // need check the quadruple length == DataPageInfo'slength

                if (recLen <= dpinfo.availspace) {
                    found = true;
                    break;
                }
            }

            // two cases:
            // (1) found == true:
            //     currentDirPage has a datapagequadruple which can accomodate
            //     the quadruple which we have to insert
            // (2) found == false:
            //     there is no datapagequadruple on the current directory page
            //     whose corresponding datapage has enough space free
            //     several subcases: see below
            if (found == false) { //Start IF01
                // case (2)

                // on the current directory page is no datapagequadruple which has
                // enough free space
                //
                // two cases:
                //
                // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
                //         if there is enough space on the current directory page
                //         to accomodate a new datapagequadruple (type DataPageInfo),
                //         then insert a new DataPageInfo on the current directory
                //         page
                // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
                //         look at the next directory page, if necessary, create it.

                if (currentDirPage.available_space() >= dpinfo.size) {
                    //Start IF02
                    // case (2.1) : add a new data page quadruple into the
                    //              current directory page
                    currentDataPage = _newDatapage(dpinfo);
                    // currentDataPage is pinned! and dpinfo->pageId is also locked
                    // in the exclusive mode

                    // didn't check if currentDataPage==NULL, auto exception


                    // currentDataPage is pinned: insert its quadruple
                    // calling a THF function


                    aquadruple = dpinfo.convertToQuadruple();

                    byte[] tmpData = aquadruple.getQuadrupleByteArray();
                    currentDataPageQid = currentDirPage.insertQuadruple(tmpData);

                    QID tmpqid = currentDirPage.firstQuadruple();


                    // need catch error here!
                    if (currentDataPageQid == null)
                        throw new QHFException(null, "no space to insert quadruple.");

                    // end the loop, because a new datapage with its quadruple
                    // in the current directorypage was created and inserted into
                    // the quadrupleheapfile; the new datapage has enough space for the
                    // quadruple which the user wants to insert

                    found = true;

                } //end of IF02
                else {  //Start else 02
                    // case (2.2)
                    nextDirPageId = currentDirPage.getNextPage();
                    // two sub-cases:
                    //
                    // (2.2.1) nextDirPageId != INVALID_PAGE:
                    //         get the next directory page from the buffer manager
                    //         and do another look
                    // (2.2.2) nextDirPageId == INVALID_PAGE:
                    //         append a new directory page at the end of the current
                    //         page and then do another loop

                    if (nextDirPageId.pid != INVALID_PAGE) { //Start IF03
                        // case (2.2.1): there is another directory page:
                        unpinPage(currentDirPageId, false);

                        currentDirPageId.pid = nextDirPageId.pid;

                        pinPage(currentDirPageId,
                                currentDirPage, false);


                        // now go back to the beginning of the outer while-loop and
                        // search on the current directory page for a suitable datapage
                    } //End of IF03
                    else {  //Start Else03
                        // case (2.2): append a new directory page after currentDirPage
                        //             since it is the last directory page
                        nextDirPageId = newPage(pageinbuffer, 1);
                        // need check error!
                        if (nextDirPageId == null)
                            throw new QHFException(null, "can't new page");

                        // initialize new directory page
                        nextDirPage.init(nextDirPageId, pageinbuffer);
                        PageId temppid = new PageId(INVALID_PAGE);
                        nextDirPage.setNextPage(temppid);
                        nextDirPage.setPrevPage(currentDirPageId);

                        // update current directory page and unpin it
                        // currentDirPage is already locked in the Exclusive mode
                        currentDirPage.setNextPage(nextDirPageId);
                        unpinPage(currentDirPageId, true/*dirty*/);

                        currentDirPageId.pid = nextDirPageId.pid;
                        currentDirPage = new THFPage(nextDirPage);

                        // remark that MINIBASE_BM->newPage already
                        // pinned the new directory page!
                        // Now back to the beginning of the while-loop, using the
                        // newly created directory page.

                    } //End of else03
                } // End of else02
                // ASSERTIONS:
                // - if found == true: search will end and see assertions below
                // - if found == false: currentDirPage, currentDirPageId
                //   valid and pinned

            }//end IF01
            else { //Start else01
                // found == true:
                // we have found a datapage with enough space,
                // but we have not yet pinned the datapage:

                // ASSERTIONS:
                // - dpinfo valid

                // System.out.println("find the dirpagequadruple on current page");

                pinPage(dpinfo.pageId, currentDataPage, false);
                //currentDataPage.openTHFpage(pageinbuffer);


            }//End else01
        } //end of While01

        // ASSERTIONS:
        // - currentDirPageId, currentDirPage valid and pinned
        // - dpinfo.pageId, currentDataPageQid valid
        // - currentDataPage is pinned!

        if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
            throw new QHFException(null, "invalid PageId");

        if (!(currentDataPage.available_space() >= recLen))
            throw new QSpaceNotAvailableException(null, "no available space");

        if (currentDataPage == null)
            throw new QHFException(null, "can't find Data page");


        QID qid;
        qid = currentDataPage.insertQuadruple(quadruplePtr);

        dpinfo.quadct++;
        dpinfo.availspace = currentDataPage.available_space();


        unpinPage(dpinfo.pageId, true /* = DIRTY */);

        // DataPage is now released
        aquadruple = currentDirPage.returnQuadruple(currentDataPageQid);
        QDataPageInfo dpinfo_ondirpage = new QDataPageInfo(aquadruple);


        dpinfo_ondirpage.availspace = dpinfo.availspace;
        dpinfo_ondirpage.quadct = dpinfo.quadct;
        dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
        dpinfo_ondirpage.flushToQuadruple();


        unpinPage(currentDirPageId, true /* = DIRTY */);


        return qid;

    }

    /**
     * Delete quadruple from file with given qid.
     *
     * @return true quadruple deleted  false:quadruple not found
     * @throws InvalidQSlotNumberException invalid slot number
     * @throws InvalidQuadrupleSizeException  invalid quadruple size
     * @throws QHFException                quadrupleheapfile exception
     * @throws QHFBufMgrException          exception thrown from bufmgr layer
     * @throws QHFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public boolean deleteQuadruple(QID qid)
            throws InvalidQSlotNumberException,
            InvalidQuadrupleSizeException,
            QHFException,
            QHFBufMgrException,
            QHFDiskMgrException,
            Exception {
        boolean status;
        THFPage currentDirPage = new THFPage();
        PageId currentDirPageId = new PageId();
        THFPage currentDataPage = new THFPage();
        PageId currentDataPageId = new PageId();
        QID currentDataPageQid = new QID();

        status = _findDataPage(qid,
                currentDirPageId, currentDirPage,
                currentDataPageId, currentDataPage,
                currentDataPageQid);

        if (status != true) return status;    // quadruple not found

        // ASSERTIONS:
        // - currentDirPage, currentDirPageId valid and pinned
        // - currentDataPage, currentDataPageid valid and pinned

        // get datapageinfo from the current directory page:
        Quadruple aquadruple;

        aquadruple = currentDirPage.returnQuadruple(currentDataPageQid);
        QDataPageInfo pdpinfo = new QDataPageInfo(aquadruple);

        // delete the quadruple on the datapage
        currentDataPage.deleteQuadruple(qid);

        pdpinfo.quadct--;
        pdpinfo.flushToQuadruple();    //Write to the buffer pool
        if (pdpinfo.quadct >= 1) {
            // more quadruples remain on datapage so it still hangs around.
            // we just need to modify its directory entry

            pdpinfo.availspace = currentDataPage.available_space();
            pdpinfo.flushToQuadruple();
            unpinPage(currentDataPageId, true /* = DIRTY*/);

            unpinPage(currentDirPageId, true /* = DIRTY */);


        } else {
            // the quadruple is already deleted:
            // we're removing the last quadruple on datapage so free datapage
            // also, free the directory page if
            //   a) it's not the first directory page, and
            //   b) we've removed the last QDataPageInfo quadruple on it.

            // delete empty Qdatapage: (does it get unpinned automatically? -NO, Ranjani)
            unpinPage(currentDataPageId, false /*undirty*/);

            freePage(currentDataPageId);

            // delete corresponding DataPageInfo-entry on the directory page:
            // currentDataPageQid points to datapage (from for loop above)

            currentDirPage.deleteQuadruple(currentDataPageQid);


            // ASSERTIONS:
            // - currentDataPage, currentDataPageId invalid
            // - empty datapage unpinned and deleted

            // now check whether the directory page is empty:

            currentDataPageQid = currentDirPage.firstQuadruple();

            // st == OK: we still found a qdatapageinfo quadruple on this directory page
            PageId pageId;
            pageId = currentDirPage.getPrevPage();
            if ((currentDataPageQid == null) && (pageId.pid != INVALID_PAGE)) {
                // the directory-page is not the first directory page and it is empty:
                // delete it

                // point previous page around deleted page:

                THFPage prevDirPage = new THFPage();
                pinPage(pageId, prevDirPage, false);

                pageId = currentDirPage.getNextPage();
                prevDirPage.setNextPage(pageId);
                pageId = currentDirPage.getPrevPage();
                unpinPage(pageId, true /* = DIRTY */);


                // set prevPage-pointer of next Page
                pageId = currentDirPage.getNextPage();
                if (pageId.pid != INVALID_PAGE) {
                    THFPage nextDirPage = new THFPage();
                    pageId = currentDirPage.getNextPage();
                    pinPage(pageId, nextDirPage, false);

                    //nextDirPage.openTHFpage(apage);

                    pageId = currentDirPage.getPrevPage();
                    nextDirPage.setPrevPage(pageId);
                    pageId = currentDirPage.getNextPage();
                    unpinPage(pageId, true /* = DIRTY */);

                }

                // delete empty directory page: (automatically unpinned?)
                unpinPage(currentDirPageId, false/*undirty*/);
                freePage(currentDirPageId);


            } else {
                // either (the directory page has at least one more datapagequadruple
                // entry) or (it is the first directory page):
                // in both cases we do not delete it, but we have to unpin it:

                unpinPage(currentDirPageId, true /* == DIRTY */);


            }
        }
        return true;
    }


    /**
     * Updates the specified quadruple in the quadrupleheapfile.
     *
     * @param qid:      the quadruple which needs update
     * @param newQuadruple: the new content of the quadruple
     * @return true:update success   false: can't find the quadruple
     * @throws InvalidQSlotNumberException invalid slot number
     * @throws InvalidQUpdateException     invalid update on quadruple
     * @throws InvalidQuadrupleSizeException  invalid quadruple size
     * @throws QHFException                quadrupleheapfile exception
     * @throws QHFBufMgrException          exception thrown from bufmgr layer
     * @throws QHFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public boolean updateQuadruple(QID qid, Quadruple newQuadruple)
            throws InvalidQSlotNumberException,
            InvalidQUpdateException,
            InvalidQuadrupleSizeException,
            QHFException,
            QHFDiskMgrException,
            QHFBufMgrException,
            Exception {
        boolean status;
        THFPage dirPage = new THFPage();
        PageId currentDirPageId = new PageId();
        THFPage dataPage = new THFPage();
        PageId currentDataPageId = new PageId();
        QID currentDataPageQid = new QID();

        status = _findDataPage(qid,
                currentDirPageId, dirPage,
                currentDataPageId, dataPage,
                currentDataPageQid);

        if (status != true) return status;    // quadruple not found
        Quadruple aquadruple = new Quadruple();
        aquadruple = dataPage.returnQuadruple(qid);

        // Assume update a quadruple with a quadruple whose length is equal to
        // the original quadruple

        if (newQuadruple.getLength() != newQuadruple.getLength()) {
            unpinPage(currentDataPageId, false /*undirty*/);
            unpinPage(currentDirPageId, false /*undirty*/);

            throw new InvalidQUpdateException(null, "invalid quadruple update");

        }

        // new copy of this quadruple fits in old space;
        aquadruple.quadrupleCopy(newQuadruple);
        unpinPage(currentDataPageId, true /* = DIRTY */);

        unpinPage(currentDirPageId, false /*undirty*/);


        return true;
    }


    /**
     * Read quadruple from file, returning pointer and length.
     *
     * @param qid QuadrupleId
     * @return a Quadruple. if Quadruple==null, no more quadruples
     * @throws InvalidQSlotNumberException invalid slot number
     * @throws InvalidQuadrupleSizeException  invalid quadruple size
     * @throws QSpaceNotAvailableException no space left
     * @throws QHFException                quadrupleheapfile exception
     * @throws QHFBufMgrException          exception thrown from bufmgr layer
     * @throws QHFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public Quadruple getQuadruple(QID qid)
            throws InvalidQSlotNumberException,
            InvalidQuadrupleSizeException,
            QHFException,
            QHFDiskMgrException,
            QHFBufMgrException,
            Exception {
        boolean status;
        THFPage dirPage = new THFPage();
        PageId currentDirPageId = new PageId();
        THFPage dataPage = new THFPage();
        PageId currentDataPageId = new PageId();
        QID currentDataPageQid = new QID();

        status = _findDataPage(qid,
                currentDirPageId, dirPage,
                currentDataPageId, dataPage,
                currentDataPageQid);

        if (status != true) return null; // quadruple not found

        Quadruple aquadruple = new Quadruple();
        aquadruple = dataPage.getQuadruple(qid);

        /*
         * getQuadruple has copied the contents of qid into quadPtr and fixed up
         * quadLen also.  We simply have to unpin dirpage and qdatapage which
         * were originally pinned by _findDataPage.
         */

        unpinPage(currentDataPageId, false /*undirty*/);

        unpinPage(currentDirPageId, false /*undirty*/);


        return aquadruple;  //(true?)OK, but the caller need check if aquadruple==NULL

    }


    /**
     * Initiate a sequential scan.
     *
     * @throws InvalidQuadrupleSizeException Invalid quadruple size
     * @throws IOException               I/O errors
     */
    public TScan openScan()
            throws InvalidQuadrupleSizeException,
            IOException {
        TScan newscan = new TScan(this);
        return newscan;
    }


    /**
     * Delete the file from the database.
     *
     * @throws InvalidQSlotNumberException  invalid slot number
     * @throws InvalidQuadrupleSizeException   invalid quadruple size
     * @throws QFileAlreadyDeletedException file is deleted already
     * @throws QHFBufMgrException           exception thrown from bufmgr layer
     * @throws QHFDiskMgrException          exception thrown from diskmgr layer
     * @throws IOException                 I/O errors
     */
    public void deleteFile()
            throws InvalidQSlotNumberException,
            QFileAlreadyDeletedException,
            InvalidQuadrupleSizeException,
            QHFBufMgrException,
            QHFDiskMgrException,
            IOException {
        if (_file_deleted)
            throw new QFileAlreadyDeletedException(null, "file alread deleted");


        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;

        // Deallocate all data pages
        PageId currentDirPageId = new PageId();
        currentDirPageId.pid = _firstDirPageId.pid;
        PageId nextDirPageId = new PageId();
        nextDirPageId.pid = 0;
        Page pageinbuffer = new Page();
        THFPage currentDirPage = new THFPage();
        Quadruple aquadruple;

        pinPage(currentDirPageId, currentDirPage, false);
        //currentDirPage.openTHFpage(pageinbuffer);

        QID qid = new QID();
        while (currentDirPageId.pid != INVALID_PAGE) {
            for (qid = currentDirPage.firstQuadruple();
                 qid != null;
                 qid = currentDirPage.nextQuadruple(qid)) {
                aquadruple = currentDirPage.getQuadruple(qid);
                QDataPageInfo dpinfo = new QDataPageInfo(aquadruple);
                //int dpinfoLen = aquadruple.length;

                freePage(dpinfo.pageId);

            }
            // ASSERTIONS:
            // - we have freePage()'d all data pages referenced by
            // the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId,false);
            freePage(currentDirPageId);

            currentDirPageId.pid = nextDirPageId.pid;
            if (nextDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false);
                //currentDirPage.openTHF(pageinbuffer);
            }
        }

        delete_file_entry(_fileName);
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     *
     * @see bufmgr pinPage
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws QHFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new QHFBufMgrException(e, "Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     *
     * @see bufmgr unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws QHFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new QHFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

    private void freePage(PageId pageno)
            throws QHFBufMgrException {

        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            throw new QHFBufMgrException(e, "Heapfile.java: freePage() failed");
        }

    } // end of freePage

    private PageId newPage(Page page, int num)
            throws QHFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new QHFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    } // end of newPage

    private PageId get_file_entry(String filename)
            throws QHFDiskMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            throw new QHFDiskMgrException(e, "Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void add_file_entry(String filename, PageId pageno)
            throws QHFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
        } catch (Exception e) {
            throw new QHFDiskMgrException(e, "Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private void delete_file_entry(String filename)
            throws QHFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            throw new QHFDiskMgrException(e, "Heapfile.java: delete_file_entry() failed");
        }

    } // end of delete_file_entry


}// End of HeapFile
