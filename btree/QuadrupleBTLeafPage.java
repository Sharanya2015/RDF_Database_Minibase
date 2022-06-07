/*
 * @(#) QuadrupleBTIndexPage.java
 * This class is derieved from BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */

package btree;

import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import heap.*;

/**
 * A QuadrupleBTLeafPage is a leaf page on a Quadruple B+ tree. It holds
 * abstract
 * <key, QID> pairs; it doesn't know anything about the keys
 * (their lengths or their types), instead relying on the abstract
 * interface consisting of QuadrupleBT.java.
 */
public class QuadrupleBTLeafPage extends QuadrupleBTSortedPage {

  /**
   * pin the page with pageno, and get the corresponding BTLeafPage,
   * also it sets the type to be NodeType.LEAF.
   * 
   * @param pageno  Input parameter. To specify which page number the
   *                QuadrupleBTLeafPage will correspond to.
   * @param keyType either AttrType.attrInteger or AttrType.attrString.
   *                Input parameter.
   * @exception IOException            error from the lower layer
   * @exception ConstructPageException QuadrupleBTLeafPage constructor error
   */
  public QuadrupleBTLeafPage(PageId pageno, int keyType)
      throws IOException,
      ConstructPageException {
    super(pageno, keyType);
    setType(NodeType.LEAF);
  }

  /**
   * associate the QuadrupleBTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF.
   * 
   * @param page    input parameter. To specify which page the
   *                QuadrupleBTLeafPage will correspond to.
   * @param keyType either AttrType.attrInteger or AttrType.attrString.
   *                Input parameter.
   * @exception IOException            error from the lower layer
   * @exception ConstructPageException QuadrupleBTLeafPage constructor error
   */
  public QuadrupleBTLeafPage(Page page, int keyType)
      throws IOException,
      ConstructPageException {
    super(page, keyType);
    setType(NodeType.LEAF);
  }

  /**
   * new a page, associate the QuadrupleBTLeafPage instance with the Page
   * instance,
   * also it sets the type to be NodeType.LEAF.
   * 
   * @param keyType either AttrType.attrInteger or AttrType.attrString.
   *                Input parameter.
   * @exception IOException            error from the lower layer
   * @exception ConstructPageException QuadrupleBTLeafPage constructor error
   */
  public QuadrupleBTLeafPage(int keyType)
      throws IOException,
      ConstructPageException {
    super(keyType);
    setType(NodeType.LEAF);
  }

  /**
   * insertQuadruple
   * READ THIS DESCRIPTION CAREFULLY. THERE ARE TWO LIDs
   * WHICH MEAN TWO DIFFERENT THINGS.
   * Inserts a key, qid value into the leaf node. This is
   * accomplished by a call to SortedPage::insertQuadruple()
   * Parameters:
   * 
   * @param key     - the key value of the data quadruple. Input parameter.
   * @param dataQid - the lid of the data quadruple. This is
   *                stored on the leaf page along with the
   *                corresponding key value. Input parameter.
   *
   * @return - the qid of the inserted leaf quadruple data entry,
   *         i.e., the <key, dataQid> pair.
   * @exception LeafInsertRecException error when insert
   */
  public QID insertQuadruple(KeyClass key, QID dataQid)
      throws LeafInsertRecException {
    KeyDataEntry entry;

    try {
      entry = new KeyDataEntry(key, dataQid);

      return insertQuadruple(entry);
    } catch (Exception e) {
      throw new LeafInsertRecException(e, "insert quadruple failed");
    }
  } // end of insertQuadruple

  /**
   * Iterators.
   * One of the two functions: getFirst and getNext
   * which provide an iterator interface to the quadruples on a
   * QuadrupleBTLeafPage.
   * 
   * @param qid It will be modified and the first lid in the leaf page
   *            will be passed out by itself. Input and Output parameter.
   * @return return the first KeyDataEntry in the leaf page.
   *         null if no more quadruple
   * @exception IteratorException iterator error
   */
  public KeyDataEntry getFirst(QID qid)
      throws IteratorException {

    KeyDataEntry entry;

    try {
      qid.pageNo = getCurPage();
      qid.slotNo = 0; // begin with first slot

      if (getSlotCnt() <= 0) {
        return null;
      }

      entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
          keyType, NodeType.LEAF);

      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get first entry failed");
    }
  } // end of getFirst

  /**
   * Iterators.
   * One of the two functions: getFirst and getNext which provide an
   * iterator interface to the quadruples on a QuadrupleBTLeafPage.
   * 
   * @param qid It will be modified and the next lid will be passed out
   *            by itself. Input and Output parameter.
   * @return return the next KeyDataEntry in the leaf page.
   *         null if no more quadruple.
   * @exception IteratorException iterator error
   */

  public KeyDataEntry getNext(QID qid)
      throws IteratorException {
    KeyDataEntry entry;
    int i;
    try {
      qid.slotNo++; // must before any return;
      i = qid.slotNo;

      if (qid.slotNo >= getSlotCnt()) {
        return null;
      }

      entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i),
          keyType, NodeType.LEAF);

      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get next entry failed");
    }
  }

  /**
   * getCurrent returns the current quadruple in the iteration; it is like
   * getNext except it does not advance the iterator.
   * 
   * @param qid the current qid. Input and Output parameter. But
   *            Output=Input.
   * @return return the current KeyDataEntry
   * @exception IteratorException iterator error
   */
  public KeyDataEntry getCurrent(QID qid)
      throws IteratorException {
    qid.slotNo--;
    return getNext(qid);
  }

  /**
   * delete a data entry in the leaf page.
   * 
   * @param dEntry the entry will be deleted in the leaf page. Input parameter.
   * @return true if deleted; false if no dEntry in the page
   * @exception LeafDeleteException error when delete
   */
  public boolean delEntry(KeyDataEntry dEntry)
      throws LeafDeleteException {
    KeyDataEntry entry;
    QID qid = new QID();

    try {
      for (entry = getFirst(qid); entry != null; entry = getNext(qid)) {
        if (entry.equals(dEntry)) {
          if (super.deleteSortedQuadruple(qid) == false)
            throw new LeafDeleteException(null, "Delete quadruple failed");
          return true;
        }

      }
      return false;
    } catch (Exception e) {
      throw new LeafDeleteException(e, "delete entry failed");
    }

  } // end of delEntry

  /*
   * used in full delete
   * 
   * @param leafPage the sibling page of this. Input parameter.
   * 
   * @param parentIndexPage the parant of leafPage and this. Input parameter.
   * 
   * @param direction -1 if "this" is left sibling of leafPage ;
   * 1 if "this" is right sibling of leafPage. Input parameter.
   * 
   * @param deletedKey the key which was already deleted, and cause
   * redistribution. Input parameter.
   * 
   * @exception LeafRedistributeException
   * 
   * @return true if redistrbution success. false if we can not redistribute them.
   */
  boolean redistribute(QuadrupleBTLeafPage leafPage, QuadrupleBTIndexPage parentIndexPage,
      int direction, KeyClass deletedKey)
      throws LeafRedistributeException {
    boolean st;
    // assertion: leafPage pinned
    try {
      if (direction == -1) { // 'this' is the left sibling of leafPage
        if ((getSlotLength(getSlotCnt() - 1) + available_space() + 8 /* 2*sizeof(slot) */) > ((MAX_SPACE - DPFIXED)
            / 2)) {
          // cannot spare a quadruple for its underflow sibling
          return false;
        } else {
          // move the last quadruple to its sibling

          // get the last quadruple
          KeyDataEntry lastEntry;
          lastEntry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(getSlotCnt() - 1),
              getSlotLength(getSlotCnt() - 1), keyType, NodeType.LEAF);

          // get its sibling's first quadruple's key for adjusting parent pointer
          QID dummyQid = new QID();
          KeyDataEntry firstEntry;
          firstEntry = leafPage.getFirst(dummyQid);

          // insert it into its sibling
          leafPage.insertQuadruple(lastEntry);

          // delete the last quadruple from the old page
          QID delQid = new QID();
          delQid.pageNo = getCurPage();
          delQid.slotNo = getSlotCnt() - 1;
          if (deleteSortedQuadruple(delQid) == false)
            throw new LeafRedistributeException(null, "delete quadruple failed");

          // adjust the entry pointing to sibling in its parent
          if (deletedKey != null)
            st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
          else
            st = parentIndexPage.adjustKey(lastEntry.key,
                firstEntry.key);
          if (st == false)
            throw new LeafRedistributeException(null, "adjust key failed");
          return true;
        }
      } else { // 'this' is the right sibling of pptr
        if ((getSlotLength(0) + available_space() + 8) > ((MAX_SPACE - DPFIXED) / 2)) {
          // cannot spare a quadruple for its underflow sibling
          return false;
        } else {
          // move the first quadruple to its sibling

          // get the first quadruple
          KeyDataEntry firstEntry;
          firstEntry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(0),
              getSlotLength(0), keyType,
              NodeType.LEAF);

          // insert it into its sibling
          QID dummyQid = new QID();
          leafPage.insertQuadruple(firstEntry);

          // delete the first quadruple from the old page
          QID delQid = new QID();
          delQid.pageNo = getCurPage();
          delQid.slotNo = 0;
          if (deleteSortedQuadruple(delQid) == false)
            throw new LeafRedistributeException(null, "delete quadruple failed");

          // get the current first quadruple of the old page
          // for adjusting parent pointer.
          KeyDataEntry tmpEntry;
          tmpEntry = getFirst(dummyQid);

          // adjust the entry pointing to itself in its parent
          st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
          if (st == false)
            throw new LeafRedistributeException(null, "adjust key failed");
          return true;
        }
      }
    } catch (Exception e) {
      throw new LeafRedistributeException(e, "redistribute failed");
    }
  } // end of redistribute

} // end of LabelBTLeafPage
