package labelheap;

import java.io.*;
import java.util.Arrays;

import diskmgr.*;
import global.*;

/**  This labelheapfile implementation is directory-based. We maintain a
 *  directory of info about the data pages (which are of type LHFPage
 *  when loaded into memory).  The directory itself is also composed
 *  of LHFPages, with each label being of type DataPageInfo
 *  as defined below.
 *
 *  The first directory page is a header page for the entire database
 *  (it is the one to which our filename is mapped by the DB).
 *  All directory pages are in a doubly-linked list of pages, each
 *  directory entry points to a single data page, which contains
 *  the actual labels.
 *
 *  The labelheapfile data pages are implemented as slotted pages, with
 *  the slots at the front and the labels in the back, both growing
 *  into the free space in the middle of the page.
 *
 *  We can store roughly pagesize/sizeof(DataPageInfo) labels per
 *  directory page; for any given LabelHeapFile insertion, it is likely
 *  that at least one of those referenced data pages will have
 *  enough free space to satisfy the request.
 */

/**
 * DataPageInfo class : the type of labels stored on a directory page.
 *
 * April 9, 1998
 */

interface Filetype {
	int TEMP = 0;
	int ORDINARY = 1;

} // end of Filetype

// this class is implemented similar to HeapFile class of relational DB

public class LabelHeapFile implements Filetype, GlobalConst {

	PageId _firstDirPageId; // page number of header page
	int _ftype;
	private boolean _file_deleted;
	private String _fileName;
	private static int tempfilecount = 0;

	/*
	 * get a new datapage from the buffer manager and initialize dpinfo
	 * 
	 * @param dpinfop the information in the new LHFPage
	 */
	private LHFPage _newDatapage(DataPageLabelInfo dpinfop)
			throws LHFException,
			LHFBufMgrException,
			LHFDiskMgrException,
			IOException {
		Page apage = new Page();
		PageId pageId = new PageId();
		pageId = newPage(apage, 1);

		if (pageId == null)
			throw new LHFException(null, "can't add new page");

		// initialize internal values of the new page:

		LHFPage lhfpage = new LHFPage();
		lhfpage.init(pageId, apage);

		dpinfop.pageId.pid = pageId.pid;
		dpinfop.recct = 0;
		dpinfop.availspace = lhfpage.available_space();

		return lhfpage;

	} // end of _newDatapage

	/*
	 * Internal LabelHeapFile function (used in getLabel and updateLabel):
	 * returns pinned directory page and pinned data page of the specified
	 * label(lid) and true if label is found.
	 * If the user label cannot be found, return false.
	 */
	private boolean _findDataPage(LID lid,
			PageId dirPageId, LHFPage dirpage,
			PageId dataPageId, LHFPage datapage,
			LID rpDataPageLid)
			throws LabelInvalidSlotNumberException,
			InvalidLabelSizeException,
			LHFException,
			LHFBufMgrException,
			LHFDiskMgrException,
			Exception {
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		LHFPage currentDirPage = new LHFPage();
		LHFPage currentDataPage = new LHFPage();
		LID currentDataPageLid = new LID();
		PageId nextDirPageId = new PageId();
		// datapageId is stored in dpinfo.pageId

		pinPage(currentDirPageId, currentDirPage, false/* read disk */);

		Label alabel = new Label();

		while (currentDirPageId.pid != INVALID_PAGE) {// Start While01
														// ASSERTIONS:
														// currentDirPage, currentDirPageId valid and pinned and Locked.

			for (currentDataPageLid = currentDirPage
					.firstLabel(); currentDataPageLid != null; currentDataPageLid = currentDirPage
							.nextLabel(currentDataPageLid)) {
				try {
					alabel = currentDirPage.getLabel(currentDataPageLid);
				} catch (LabelInvalidSlotNumberException e)// check error! return false(done)
				{
					return false;
				}

				DataPageLabelInfo dpinfo = new DataPageLabelInfo(alabel);
				try {
					pinPage(dpinfo.pageId, currentDataPage, false/* Rddisk */);

					// check error;need unpin currentDirPage
				} catch (Exception e) {
					unpinPage(currentDirPageId, false/* undirty */);
					dirpage = null;
					datapage = null;
					throw e;
				}

				// ASSERTIONS:
				// - currentDataPage, currentDataPageLid, dpinfo valid
				// - currentDataPage pinned

				if (dpinfo.pageId.pid == lid.pageNo.pid) {
					alabel = currentDataPage.returnLabel(lid);
					// found label on the current datapage which itself
					// is indexed on the current dirpage. Return both of these.

					dirpage.setpage(currentDirPage.getpage());
					dirPageId.pid = currentDirPageId.pid;

					datapage.setpage(currentDataPage.getpage());
					dataPageId.pid = dpinfo.pageId.pid;

					rpDataPageLid.pageNo.pid = currentDataPageLid.pageNo.pid;
					rpDataPageLid.slotNo = currentDataPageLid.slotNo;
					return true;
				} else {
					// label not found on this datapage; unpin it
					// and try the next one
					unpinPage(dpinfo.pageId, false /* undirty */);

				}

			}

			// if we would have found the correct datapage on the current
			// directory page we would have already returned.
			// therefore:
			// read in next directory page:

			nextDirPageId = currentDirPage.getNextPage();
			try {
				unpinPage(currentDirPageId, false /* undirty */);
			} catch (Exception e) {
				throw new LHFException(e, "labelheapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			if (currentDirPageId.pid != INVALID_PAGE) {
				pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
				if (currentDirPage == null)
					throw new LHFException(null, "pinPage return null page");
			}

		} // end of While01
			// checked all dir pages and all data pages;label not found:(

		dirPageId.pid = dataPageId.pid = INVALID_PAGE;

		return false;

	} // end of _findDatapage

	/**
	 * Initialize. A null name produces a temporary labelheapfile which will be
	 * deleted by the destructor. If the name already denotes a file, the
	 * file is opened; otherwise, a new empty file is created.
	 *
	 * @exception LHFException        labelheapfile exception
	 * @exception LHFBufMgrException  exception thrown from bufmgr layer
	 * @exception LHFDiskMgrException exception thrown from diskmgr layer
	 * @exception IOException        I/O errors
	 */
	public LabelHeapFile(String name)
			throws LHFException,
			LHFBufMgrException,
			LHFDiskMgrException,
			IOException

	{
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
			tempfilecount++;

		} else {
			_fileName = name;
			_ftype = ORDINARY;
		}

		// The constructor gets run in two different cases.
		// In the first case, the file is new and the header page
		// must be initialized. This case is detected via a failure
		// in the db->get_file_entry() call. In the second case, the
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
				throw new LHFException(null, "can't new page");

			add_file_entry(_fileName, _firstDirPageId);
			// check error(new exception: Could not add file entry

			LHFPage firstDirPage = new LHFPage();
			firstDirPage.init(_firstDirPageId, apage);
			PageId pageId = new PageId(INVALID_PAGE);

			firstDirPage.setNextPage(pageId);
			firstDirPage.setPrevPage(pageId);
			unpinPage(_firstDirPageId, true /* dirty */ );

		}
		_file_deleted = false;
		// ASSERTIONS:
		// - ALL private data members of class LabelHeapfile are valid:
		//
		// - _firstDirPageId valid
		// - _fileName valid
		// - no datapage pinned yet

	} // end of constructor

	/**
	 * Return number of labels in file.
	 *
	 * @exception LabelInvalidSlotNumberException invalid slot number
	 * @exception InvalidLabelSizeException       invalid label size
	 * @exception LHFBufMgrException              exception thrown from bufmgr layer
	 * @exception LHFDiskMgrException             exception thrown from diskmgr
	 *                                            layer
	 * @exception IOException                     I/O errors
	 */
	public int getLabelCnt()
			throws LabelInvalidSlotNumberException,
			InvalidLabelSizeException,
			LHFDiskMgrException,
			LHFBufMgrException,
			IOException

	{
		int answer = 0;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		LHFPage currentDirPage = new LHFPage();
		Page pageinbuffer = new Page();

		while (currentDirPageId.pid != INVALID_PAGE) {
			pinPage(currentDirPageId, currentDirPage, false);

			LID lid = new LID();
			Label alabel;
			for (lid = currentDirPage.firstLabel(); lid != null; // lid==NULL means no more label
					lid = currentDirPage.nextLabel(lid)) {
				alabel = currentDirPage.getLabel(lid);
				DataPageLabelInfo dpinfo = new DataPageLabelInfo(alabel);

				answer += dpinfo.recct;
			}

			// ASSERTIONS: no more label
			// - we have read all datapage labels on
			// the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			unpinPage(currentDirPageId, false /* undirty */);
			currentDirPageId.pid = nextDirPageId.pid;
		}

		// ASSERTIONS:
		// - if error, exceptions
		// - if end of labelheapfile reached: currentDirPageId == INVALID_PAGE
		// - if not yet end of labelheapfile: currentDirPageId valid

		return answer;
	}

	// public LID insertLabel(byte[] label) throws LHFException,
	// LabelInvalidSlotNumberException, LHFBufMgrException, LHFDiskMgrException,
	// InvalidLabelSizeException, LabelSpaceNotAvailableException, IOException {
	// String labelInString = Arrays.toString(label);
	// return insertLabel(labelInString);
	// }

	/**
	 * Insert label into file, return its Lid.
	 *
	 * @param labelPtr pointer of the label
	 * @param labelLen the length of the label
	 *
	 * @exception LabelInvalidSlotNumberException invalid slot number
	 * @exception InvalidLabelSizeException       invalid label size
	 * @exception LabelSpaceNotAvailableException no space left
	 * @exception LHFException                    labelheapfile exception
	 * @exception LHFBufMgrException               exception thrown from bufmgr layer
	 * @exception LHFDiskMgrException              exception thrown from diskmgr
	 *                                            layer
	 * @exception IOException                     I/O errors
	 *
	 * @return the lid of the label
	 */
	public LID insertLabel(byte[] labelPtr)
			throws LabelInvalidSlotNumberException,
			InvalidLabelSizeException,
			LabelSpaceNotAvailableException,
			LHFException,
			LHFBufMgrException,
			LHFDiskMgrException,
			IOException {
		int dpinfoLen = 0;
		// byte[] recPtr = recPtrS.getBytes();
		int labelLen = labelPtr.length;
		boolean found;
		LID currentDataPageLid = new LID();
		Page pageinbuffer = new Page();
		LHFPage currentDirPage = new LHFPage();
		LHFPage currentDataPage = new LHFPage();
		LHFPage nextDirPage = new LHFPage();
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId = new PageId(); // OK

		pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
		found = false;
		Label alabel;
		DataPageLabelInfo dpinfo = new DataPageLabelInfo();
		while (found == false) { // Start While01
									// look for suitable dpinfo-struct
			for (currentDataPageLid = currentDirPage
					.firstLabel(); currentDataPageLid != null; currentDataPageLid = currentDirPage
							.nextLabel(currentDataPageLid)) {
				alabel = currentDirPage.getLabel(currentDataPageLid);

				dpinfo = new DataPageLabelInfo(alabel);

				// need check the label length == DataPageInfo'slength

				if (labelLen <= dpinfo.availspace) {
					found = true;
					break;
				}
			}
			// two cases:
			// (1) found == true:
			// currentDirPage has a datapagelabel which can accomodate
			// the label which we have to insert
			// (2) found == false:
			// there is no datapagelabel on the current directory page
			// whose corresponding datapage has enough space free
			// several subcases: see below
			if (found == false) { // Start IF01
									// case (2)

				// System.out.println("no datapagelabel on the current directory is OK");
				// System.out.println("dirpage availspace "+currentDirPage.available_space());

				// on the current directory page is no datapagelabel which has
				// enough free space
				//
				// two cases:
				//
				// - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
				// if there is enough space on the current directory page
				// to accomodate a new datapagelabel (type DataPageInfo),
				// then insert a new DataPageInfo on the current directory
				// page
				// - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
				// look at the next directory page, if necessary, create it.

				if (currentDirPage.available_space() >= dpinfo.size) {
					// Start IF02
					// case (2.1) : add a new data page label into the
					// current directory page
					currentDataPage = _newDatapage(dpinfo);
					// currentDataPage is pinned! and dpinfo->pageId is also locked
					// in the exclusive mode

					// didn't check if currentDataPage==NULL, auto exception

					// currentDataPage is pinned: insert its label
					// calling a LHFPage function

					alabel = dpinfo.convertToLabel();

					byte[] tmpData = alabel.getLabelByteArray();
					currentDataPageLid = currentDirPage.insertLabel(tmpData);

					LID tmplid = currentDirPage.firstLabel();

					if (currentDataPageLid == null)
						throw new LHFException(null, "no space to insert label.");

					// end the loop, because a new datapage with its label
					// in the current directorypage was created and inserted into
					// the labelheapfile; the new datapage has enough space for the
					// label which the user wants to insert

					found = true;

				} // end of IF02
				else { // Start else 02
						// case (2.2)
					nextDirPageId = currentDirPage.getNextPage();
					// two sub-cases:
					//
					// (2.2.1) nextDirPageId != INVALID_PAGE:
					// get the next directory page from the buffer manager
					// and do another look
					// (2.2.2) nextDirPageId == INVALID_PAGE:
					// append a new directory page at the end of the current
					// page and then do another loop

					if (nextDirPageId.pid != INVALID_PAGE) { // Start IF03
																// case (2.2.1): there is another directory page:
						unpinPage(currentDirPageId, false);

						currentDirPageId.pid = nextDirPageId.pid;

						pinPage(currentDirPageId,
								currentDirPage, false);

						// now go back to the beginning of the outer while-loop and
						// search on the current directory page for a suitable datapage
					} // End of IF03
					else { // Start Else03
							// case (2.2): append a new directory page after currentDirPage
							// since it is the last directory page
						nextDirPageId = newPage(pageinbuffer, 1);
						// need check error!
						if (nextDirPageId == null)
							throw new LHFException(null, "can't new page");

						// initialize new directory page
						nextDirPage.init(nextDirPageId, pageinbuffer);
						PageId temppid = new PageId(INVALID_PAGE);
						nextDirPage.setNextPage(temppid);
						nextDirPage.setPrevPage(currentDirPageId);

						// update current directory page and unpin it
						// currentDirPage is already locked in the Exclusive mode
						currentDirPage.setNextPage(nextDirPageId);
						unpinPage(currentDirPageId, true/* dirty */);

						currentDirPageId.pid = nextDirPageId.pid;
						currentDirPage = new LHFPage(nextDirPage);

						// remark that MINIBASE_BM->newPage already
						// pinned the new directory page!
						// Now back to the beginning of the while-loop, using the
						// newly created directory page.

					} // End of else03
				} // End of else02
					// ASSERTIONS:
					// - if found == true: search will end and see assertions below
					// - if found == false: currentDirPage, currentDirPageId
					// valid and pinned

			} // end IF01
			else { // Start else01
					// found == true:
					// we have found a datapage with enough space,
					// but we have not yet pinned the datapage:

				// ASSERTIONS:
				// - dpinfo valid

				// System.out.println("find the dirpagelabel on current page");

				pinPage(dpinfo.pageId, currentDataPage, false);
				// currentDataPage.openHFpage(pageinbuffer);

			} // End else01
		} // end of While01

		// ASSERTIONS:
		// - currentDirPageId, currentDirPage valid and pinned
		// - dpinfo.pageId, currentDataPageLid valid
		// - currentDataPage is pinned!

		if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
			throw new LHFException(null, "invalid PageId");

		if (!(currentDataPage.available_space() >= labelLen))
			throw new LabelSpaceNotAvailableException(null, "no available space");

		if (currentDataPage == null)
			throw new LHFException(null, "can't find Data page");

		LID lid;
		lid = currentDataPage.insertLabel(labelPtr);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();

		unpinPage(dpinfo.pageId, true /* = DIRTY */);

		// DataPage is now released
		alabel = currentDirPage.returnLabel(currentDataPageLid);
		DataPageLabelInfo dpinfo_ondirpage = new DataPageLabelInfo(alabel);

		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
		dpinfo_ondirpage.flushToLabel();

		unpinPage(currentDirPageId, true /* = DIRTY */);

		return lid;

	}

	/**
	 * Delete label from file with given lid.
	 *
	 * @exception LabelInvalidSlotNumberException invalid slot number
	 * @exception InvalidLabelSizeException       invalid label size
	 * @exception LHFException                     labelheapfile exception
	 * @exception LHFBufMgrException               exception thrown from bufmgr layer
	 * @exception LHFDiskMgrException              exception thrown from diskmgr
	 *                                            layer
	 * @exception Exception                       other exception
	 *
	 * @return true label deleted false:label not found
	 */
	public boolean deleteLabel(LID lid)
			throws LabelInvalidSlotNumberException,
			InvalidLabelSizeException,
			LHFException,
			LHFBufMgrException,
			LHFDiskMgrException,
			Exception

	{
		boolean status;
		LHFPage currentDirPage = new LHFPage();
		PageId currentDirPageId = new PageId();
		LHFPage currentDataPage = new LHFPage();
		PageId currentDataPageId = new PageId();
		LID currentDataPageLid = new LID();

		status = _findDataPage(lid,
				currentDirPageId, currentDirPage,
				currentDataPageId, currentDataPage,
				currentDataPageLid);

		if (status != true)
			return status; // label not found

		// ASSERTIONS:
		// - currentDirPage, currentDirPageId valid and pinned
		// - currentDataPage, currentDataPageid valid and pinned

		// get datapageinfo from the current directory page:
		Label alabel;

		alabel = currentDirPage.returnLabel(currentDataPageLid);
		DataPageLabelInfo pdpinfo = new DataPageLabelInfo(alabel);

		// delete the label on the datapage
		currentDataPage.deleteLabel(lid);

		pdpinfo.recct--;
		pdpinfo.flushToLabel(); // Write to the buffer pool
		if (pdpinfo.recct >= 1) {
			// more labels remain on datapage so it still hangs around.
			// we just need to modify its directory entry

			pdpinfo.availspace = currentDataPage.available_space();
			pdpinfo.flushToLabel();
			unpinPage(currentDataPageId, true /* = DIRTY */);

			unpinPage(currentDirPageId, true /* = DIRTY */);

		} else {
			// the label is already deleted:
			// we're removing the last label on datapage so free datapage
			// also, free the directory page if
			// a) it's not the first directory page, and
			// b) we've removed the last DataPageInfo label on it.

			// delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
			unpinPage(currentDataPageId, false /* undirty */);

			freePage(currentDataPageId);

			// delete corresponding DataPageInfo-entry on the directory page:
			// currentDataPageLid points to datapage (from for loop above)

			currentDirPage.deleteLabel(currentDataPageLid);

			// ASSERTIONS:
			// - currentDataPage, currentDataPageId invalid
			// - empty datapage unpinned and deleted

			// now check whether the directory page is empty:

			currentDataPageLid = currentDirPage.firstLabel();

			// st == OK: we still found a datapageinfo label on this directory page
			PageId pageId;
			pageId = currentDirPage.getPrevPage();
			if ((currentDataPageLid == null) && (pageId.pid != INVALID_PAGE)) {
				// the directory-page is not the first directory page and it is empty:
				// delete it

				// point previous page around deleted page:

				LHFPage prevDirPage = new LHFPage();
				pinPage(pageId, prevDirPage, false);

				pageId = currentDirPage.getNextPage();
				prevDirPage.setNextPage(pageId);
				pageId = currentDirPage.getPrevPage();
				unpinPage(pageId, true /* = DIRTY */);

				// set prevPage-pointer of next Page
				pageId = currentDirPage.getNextPage();
				if (pageId.pid != INVALID_PAGE) {
					LHFPage nextDirPage = new LHFPage();
					pageId = currentDirPage.getNextPage();
					pinPage(pageId, nextDirPage, false);

					// nextDirPage.openHFpage(apage);

					pageId = currentDirPage.getPrevPage();
					nextDirPage.setPrevPage(pageId);
					pageId = currentDirPage.getNextPage();
					unpinPage(pageId, true /* = DIRTY */);

				}

				// delete empty directory page: (automatically unpinned?)
				unpinPage(currentDirPageId, false/* undirty */);
				freePage(currentDirPageId);

			} else {
				// either (the directory page has at least one more datapagelabel
				// entry) or (it is the first directory page):
				// in both cases we do not delete it, but we have to unpin it:

				unpinPage(currentDirPageId, true /* == DIRTY */);

			}
		}
		return true;
	}

	/**
	 * Updates the specified label in the labelheapfile.
	 * 
	 * @param lid:      the label which needs update
	 * @param newlabel: the new content of the label
	 *
	 * @exception LabelInvalidSlotNumberException invalid slot number
	 * @exception LabelInvalidUpdateException     invalid update on label
	 * @exception InvalidLabelSizeException       invalid label size
	 * @exception LHFException                     heapfile exception
	 * @exception LHFBufMgrException               exception thrown from bufmgr layer
	 * @exception LHFDiskMgrException              exception thrown from diskmgr
	 *                                            layer
	 * @exception Exception                       other exception
	 * @return true:update success false: can't find the label
	 */
	public boolean updateLabel(LID lid, Label newlabel)
			throws LabelInvalidSlotNumberException,
			LabelInvalidUpdateException,
			InvalidLabelSizeException,
			LHFException,
			LHFDiskMgrException,
			LHFBufMgrException,
			Exception {
		boolean status;
		LHFPage dirPage = new LHFPage();
		PageId currentDirPageId = new PageId();
		LHFPage dataPage = new LHFPage();
		PageId currentDataPageId = new PageId();
		LID currentDataPageLid = new LID();

		status = _findDataPage(lid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageLid);

		if (status != true)
			return status; // label not found
		Label alabel = new Label();
		alabel = dataPage.returnLabel(lid);

		// Assume update a label with a length is equal to
		// the original label

		if (newlabel.getLength() != alabel.getLength()) {
			unpinPage(currentDataPageId, false /* undirty */);
			unpinPage(currentDirPageId, false /* undirty */);

			throw new LabelInvalidUpdateException(null, "invalid label update");

		}

		// new copy of this label fits in old space;
		alabel.labelCopy(newlabel);
		unpinPage(currentDataPageId, true /* = DIRTY */);

		unpinPage(currentDirPageId, false /* undirty */);

		return true;
	}

	public boolean updateLabel(LID lid, String newLabel)
			throws LabelInvalidSlotNumberException,
			LabelInvalidUpdateException,
			InvalidLabelSizeException,
			LHFException,
			LHFDiskMgrException,
			LHFBufMgrException,
			Exception {
		boolean status;
		LHFPage dirPage = new LHFPage();
		PageId currentDirPageId = new PageId();
		LHFPage dataPage = new LHFPage();
		PageId currentDataPageId = new PageId();
		LID currentDataPageLid = new LID();

		status = _findDataPage(lid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageLid);

		if (status != true)
			return status; // label not found
		Label alabel = new Label();
		alabel = dataPage.returnLabel(lid);

		// Assume update a label whose length is equal to
		// the original label

		if (newLabel.length() != alabel.getLength()) {
			unpinPage(currentDataPageId, false /* undirty */);
			unpinPage(currentDirPageId, false /* undirty */);

			throw new LabelInvalidUpdateException(null, "invalid label update");

		}

		// new copy of this label fits in old space;
		alabel.labelStringCopy(newLabel);
		unpinPage(currentDataPageId, true /* = DIRTY */);

		unpinPage(currentDirPageId, false /* undirty */);

		return true;
	}

	/**
	 * Read label from file, returning pointer and length.
	 * 
	 * @param lid Label ID
	 *
	 * @exception LabelInvalidSlotNumberException invalid slot number
	 * @exception InvalidLabelSizeException       invalid label size
	 * @exception LabelSpaceNotAvailableException no space left
	 * @exception LHFException                     heapfile exception
	 * @exception LHFBufMgrException               exception thrown from bufmgr layer
	 * @exception LHFDiskMgrException              exception thrown from diskmgr
	 *                                            layer
	 * @exception Exception                       other exception
	 *
	 * @return a label String.
	 */
	public String getLabel(LID lid)
			throws LabelInvalidSlotNumberException,
			InvalidLabelSizeException,
			LHFException,
			LHFDiskMgrException,
			LHFBufMgrException,
			Exception {
		boolean status;
		LHFPage dirPage = new LHFPage();
		PageId currentDirPageId = new PageId();
		LHFPage dataPage = new LHFPage();
		PageId currentDataPageId = new PageId();
		LID currentDataPageLid = new LID();

		status = _findDataPage(lid,
				currentDirPageId, dirPage,
				currentDataPageId, dataPage,
				currentDataPageLid);

		if (status != true)
			return null; // label not found

		Label alabel = new Label();
		alabel = dataPage.getLabel(lid);

		/*
		 * getLabel has copied the contents of lid into labelPtr and fixed up
		 * labelLen also. We simply have to unpin dirpage and datapage which
		 * were originally pinned by _findDataPage.
		 */

		unpinPage(currentDataPageId, false /* undirty */);

		unpinPage(currentDirPageId, false /* undirty */);

		return alabel.getLabel(); // (true?)OK, but the caller need check if alabel==NULL

	}

	/**
	 * Initiate a sequential scan.
	 * 
	 * @exception InvalidLabelSizeException Invalid label size
	 * @exception IOException               I/O errors
	 *
	 */
	public LScan openScan()
			throws InvalidLabelSizeException,
			IOException {
		LScan newscan = new LScan(this);
		return newscan;
	}

	/**
	 * Delete the file from the database.
	 *
	 * @exception LabelInvalidSlotNumberException  invalid slot number
	 * @exception InvalidLabelSizeException        invalid label size
	 * @exception LabelFileAlreadyDeletedException file is deleted already
	 * @exception LHFBufMgrException                exception thrown from bufmgr
	 *                                             layer
	 * @exception LHFDiskMgrException               exception thrown from diskmgr
	 *                                             layer
	 * @exception IOException                      I/O errors
	 */
	public void deleteFile()
			throws LabelInvalidSlotNumberException,
			LabelFileAlreadyDeletedException,
			InvalidLabelSizeException,
			LHFBufMgrException,
			LHFDiskMgrException,
			IOException {
		if (_file_deleted)
			throw new LabelFileAlreadyDeletedException(null, "file alread deleted");

		// Mark the deleted flag (even if it doesn't get all the way done).
		_file_deleted = true;

		// Deallocate all data pages
		PageId currentDirPageId = new PageId();
		currentDirPageId.pid = _firstDirPageId.pid;
		PageId nextDirPageId = new PageId();
		nextDirPageId.pid = 0;
		Page pageinbuffer = new Page();
		LHFPage currentDirPage = new LHFPage();
		Label alabel;

		pinPage(currentDirPageId, currentDirPage, false);
		// currentDirPage.openHFpage(pageinbuffer);

		LID lid = new LID();
		while (currentDirPageId.pid != INVALID_PAGE) {
			for (lid = currentDirPage.firstLabel(); lid != null; lid = currentDirPage.nextLabel(lid)) {
				alabel = currentDirPage.getLabel(lid);
				DataPageLabelInfo dpinfo = new DataPageLabelInfo(alabel);
				// int dpinfoLen = alabel.length;

				freePage(dpinfo.pageId);

			}
			// ASSERTIONS:
			// - we have freePage()'d all data pages referenced by
			// the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			freePage(currentDirPageId);

			currentDirPageId.pid = nextDirPageId.pid;
			if (nextDirPageId.pid != INVALID_PAGE) {
				pinPage(currentDirPageId, currentDirPage, false);
				// currentDirPage.openHFpage(pageinbuffer);
			}
		}

		delete_file_entry(_fileName);
	}

	/**
	 * short cut to access the pinPage function in bufmgr package.
	 * 
	 * @see bufmgr.pinPage
	 */
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws LHFBufMgrException {

		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new LHFBufMgrException(e, "LabelHeapfile.java: pinPage() failed");
		}

	} // end of pinPage

	/**
	 * short cut to access the unpinPage function in bufmgr package.
	 * 
	 * @see bufmgr.unpinPage
	 */
	private void unpinPage(PageId pageno, boolean dirty)
			throws LHFBufMgrException {

		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new LHFBufMgrException(e, "LabelHeapfile.java: unpinPage() failed");
		}

	} // end of unpinPage

	private void freePage(PageId pageno)
			throws LHFBufMgrException {

		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			throw new LHFBufMgrException(e, "LabelHeapfile.java: freePage() failed");
		}

	} // end of freePage

	private PageId newPage(Page page, int num)
			throws LHFBufMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page, num);
		} catch (Exception e) {
			throw new LHFBufMgrException(e, "LabelHeapfile.java: newPage() failed");
		}

		return tmpId;

	} // end of newPage

	private PageId get_file_entry(String filename)
			throws LHFDiskMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			throw new LHFDiskMgrException(e, "LabelHeapfile.java: get_file_entry() failed");
		}

		return tmpId;

	} // end of get_file_entry

	private void add_file_entry(String filename, PageId pageno)
			throws LHFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
		} catch (Exception e) {
			throw new LHFDiskMgrException(e, "LabelHeapfile.java: add_file_entry() failed");
		}

	} // end of add_file_entry

	private void delete_file_entry(String filename)
			throws LHFDiskMgrException {

		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			throw new LHFDiskMgrException(e, "LabelHeapfile.java: delete_file_entry() failed");
		}

	} // end of delete_file_entry

}// End of HeapFile
