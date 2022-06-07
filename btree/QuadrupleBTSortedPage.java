/*
 * @(#) QuadrupleSortedPage.java
 * This class is derived from SortedPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *
 *      by Xiaohu Li (xiaohu@cs.wisc.edu)
 */

//package labelbtree;
package btree;

import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import heap.*;
//import btree.*;
import quadrupleheap.*;

/**
 * QuadrupleBTsortedPage class
 * just holds abstract quadruples in sorted order, based
 * on how they compare using the key interface from LabelBT.java.
 */
public class QuadrupleBTSortedPage extends THFPage {

  int keyType; // it will be initialized in BTFile

  /**
   * pin the page with pageno, and get the corresponding SortedPage
   * 
   * @param pageno  input parameter. To specify which page number the
   *                QuadrupleBTSortedPage will correspond to.
   * @param keyType input parameter. It specifies the type of key. It can be
   *                AttrType.attrString or AttrType.attrInteger.
   * @exception ConstructPageException error for QuadrupleBTSortedPage constructor
   */
  public QuadrupleBTSortedPage(PageId pageno, int keyType)
      throws ConstructPageException {
    super();
    try {
      // super();
      SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
      this.keyType = keyType;
    } catch (Exception e) {
      throw new ConstructPageException(e, "construct sorted page failed");
    }
  }

  /**
   * associate the SortedPage instance with the Page instance
   * 
   * @param page    input parameter. To specify which page the
   *                QuadrupleBTSortedPage will correspond to.
   * @param keyType input parameter. It specifies the type of key. It can be
   *                AttrType.attrString or AttrType.attrInteger.
   */
  public QuadrupleBTSortedPage(Page page, int keyType) {

    super(page);
    this.keyType = keyType;
  }

  /**
   * new a page, and associate the QuadrupleSortedPage instance with the Page
   * instance
   * 
   * @param keyType input parameter. It specifies the type of key. It can be
   *                AttrType.attrString or AttrType.attrInteger.
   * @exception ConstructPageException error for QuadrupleBTSortedPage constructor
   */
  public QuadrupleBTSortedPage(int keyType)
      throws ConstructPageException {
    super();
    try {
      Page apage = new Page();
      PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
      if (pageId == null)
        throw new ConstructPageException(null, "construct new page failed");
      this.init(pageId, apage);
      this.keyType = keyType;
    } catch (Exception e) {
      e.printStackTrace();
      throw new ConstructPageException(e, "construct sorted page failed");
    }
  }

  /**
   * Performs a sorted insertion of a quadruple on an quadruple page. The
   * quadruples are
   * sorted in increasing key order.
   * Only the slot directory is rearranged. The data quadruples remain in
   * the same positions on the page.
   * 
   * @param entry the entry to be inserted. Input parameter.
   * @return its qid where the entry was inserted; null if no space left.
   * @exception InsertRecException error when insert
   */
  protected QID insertQuadruple(KeyDataEntry entry)
      throws InsertRecException {
    int i;
    short nType;
    QID qid;
    byte[] quadrupleData;
    // ASSERTIONS:
    // - the slot directory is compressed; Inserts will occur at the end
    // - slotCnt gives the number of slots used

    // general plan:
    // 1. Insert the quadruple into the page,
    // which is then not necessarily any more sorted
    // 2. Sort the page by rearranging the slots (insertion sort)

    try {

      quadrupleData = QuadrupleBT.getBytesFromEntry(entry);
      qid = super.insertQuadruple(quadrupleData);
      if (qid == null)
        return null;

      if (entry.data instanceof QuadrupleLeafData)
        nType = NodeType.LEAF;
      else // entry.data instanceof IndexData
        nType = NodeType.INDEX;

      // performs a simple insertion sort
      for (i = getSlotCnt() - 1; i > 0; i--) {

        KeyClass key_i, key_iplus1;

        key_i = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i),
            getSlotLength(i), keyType, nType).key;

        key_iplus1 = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i - 1),
            getSlotLength(i - 1), keyType, nType).key;

        if (QuadrupleBT.keyCompare(key_i, key_iplus1) < 0) {
          // switch slots:
          int ln, off;
          ln = getSlotLength(i);
          off = getSlotOffset(i);
          setSlot(i, getSlotLength(i - 1), getSlotOffset(i - 1));
          setSlot(i - 1, ln, off);
        } else {
          // end insertion sort
          break;
        }

      }

      // ASSERTIONS:
      // - quadruple keys increase with increasing slot number
      // (starting at slot 0)
      // - slot directory compacted

      qid.slotNo = i;
      return qid;
    } catch (Exception e) {
      throw new InsertRecException(e, "insert quadruple failed");
    }

  } // end of insertQuadruple

  /**
   * Deletes a quadruple from a sorted quadruple page. It also calls
   * QuadrupleHFPage.compact_slot_dir() to compact the slot directory.
   * 
   * @param q it specifies where a quadruple will be deleted
   * @return true if success; false if qid is invalid(no quadruple in the qid).
   * @exception DeleteRecException error when delete
   */
  public boolean deleteSortedQuadruple(QID qid)
      throws DeleteRecException {
    try {

      deleteQuadruple(qid);
      compact_slot_dir();
      return true;
      // ASSERTIONS:
      // - slot directory is compacted
    } catch (Exception e) {
      if (e instanceof heap.InvalidSlotNumberException)
        return false;
      else
        throw new DeleteRecException(e, "delete quadruple failed");
    }
  } // end of deleteSortedQuadruple

  /**
   * How many quadruples are in the page
   * 
   * @param return the number of quadruples.
   * @exception IOException I/O errors
   */
  protected int numberOfQuadruple()
      throws IOException {
    return getSlotCnt();
  }
};