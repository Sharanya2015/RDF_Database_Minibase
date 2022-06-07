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
import quadrupleheap.*;

/**
 * A QuadrupleBTIndexPage is an index page on a label B+ tree. It holds abstract
 * {key, PageId} pairs; it doesn't know anything about the keys
 * (their lengths or their types), instead relying on the abstract
 * interface in QuadrupleBT.java
 * See those files for our {key,data} pairing interface and implementation.
 */
public class QuadrupleBTIndexPage extends QuadrupleBTSortedPage {

  /**
   * pin the page with pageno, and get the corresponding QuadrupleBTIndexPage,
   * also it sets the type of node to be NodeType.INDEX.
   * 
   * @param pageno  Input parameter. To specify which page number the
   *                BTIndexPage will correspond to.
   * @param keyType either AttrType.attrInteger or AttrType.attrString.
   *                Input parameter.
   * @exception IOException            error from the lower layer
   * @exception ConstructPageException error when QuadrupleBTIndexpage constructor
   */
  public QuadrupleBTIndexPage(PageId pageno, int keyType)
      throws IOException,
      ConstructPageException {
    super(pageno, keyType);
    setType(NodeType.INDEX);
  }

  /**
   * associate the QuadrupleBTIndexPage instance with the Page instance,
   * also it sets the type of node to be NodeType.INDEX.
   * 
   * @param page    input parameter. To specify which page the
   *                BTIndexPage will correspond to.
   * @param keyType either AttrType.attrInteger or AttrType.attrString.
   *                Input parameter.
   * @exception IOException            error from the lower layer
   * @exception ConstructPageException error when BTIndexpage constructor
   */
  public QuadrupleBTIndexPage(Page page, int keyType)
      throws IOException,
      ConstructPageException {
    super(page, keyType);
    setType(NodeType.INDEX);
  }

  /*
   * new a page, associate the BTIndexPage instance with the Page instance,
   * also it sets the type of node to be NodeType.INDEX.
   * 
   * @param keyType either AttrType.attrInteger or AttrType.attrString.
   * Input parameter.
   * 
   * @exception IOException error from the lower layer
   * 
   * @exception ConstructPageException error when BTIndexpage constructor
   */
  public QuadrupleBTIndexPage(int keyType)
      throws IOException,
      ConstructPageException {
    super(keyType);
    setType(NodeType.INDEX);
  }

  /**
   * It inserts a <key, pageNo> value into the index page,
   * 
   * @key the key value in <key, pageNO>. Input parameter.
   * @pageNo the pageNo in <key, pageNO>. Input parameter.
   * @return It returns the lid where the quadruple is inserted;
   *         null if no space left.
   * @exception IndexInsertRecException error when insert
   */
  public QID insertKey(KeyClass key, PageId pageNo)
      throws IndexInsertRecException {
    QID qid;
    KeyDataEntry entry;
    try {
      entry = new KeyDataEntry(key, pageNo);
      qid = super.insertQuadruple(entry);
      return qid;
    } catch (Exception e) {
      throw new IndexInsertRecException(e, "Insert failed");

    }
  }

  /*
   * OPTIONAL: fullDeletekey
   * This is optional, and is only needed if you want to do full deletion.
   * Return its lid. delete key may != key. But delete key <= key,
   * and the delete key is the first biggest key such that delete key <= key
   * 
   * @param key the key used to search. Input parameter.
   * 
   * @exception IndexFullDeleteException if no quadruple deleted or failed by
   * any reason
   * 
   * @return lid of the quadruple deleted. Can not return null.
   */
  QID deleteKey(KeyClass key)
      throws IndexFullDeleteException {
    KeyDataEntry entry;
    QID qid = new QID();

    try {

      entry = getFirst(qid);

      if (entry == null)
        // it is supposed there is at least a quadruple
        throw new IndexFullDeleteException(null, "No quadruples found");

      if (QuadrupleBT.keyCompare(key, entry.key) < 0)
        // it is supposed to not smaller than first key
        throw new IndexFullDeleteException(null, "First key is bigger");

      while (QuadrupleBT.keyCompare(key, entry.key) > 0) {
        entry = getNext(qid);
        if (entry == null)
          break;
      }

      if (entry == null)
        qid.slotNo--;
      else if (QuadrupleBT.keyCompare(key, entry.key) != 0)
        qid.slotNo--; // we want to delete the previous key

      deleteSortedQuadruple(qid);
      return qid;
    } catch (Exception e) {
      throw new IndexFullDeleteException(e, "Full delelte failed");
    }
  } // end of deleteKey

  /*
   * This function encapsulates the search routine to search a
   * QuadrupleBTIndexPage by B++ search algorithm
   * 
   * @param key the key value used in search algorithm. Input parameter.
   * 
   * @return It returns the page_no of the child to be searched next.
   * 
   * @exception IndexSearchException Index search failed;
   */
  PageId getPageNoByKey(KeyClass key)
      throws IndexSearchException {
    KeyDataEntry entry;
    int i;

    try {

      for (i = getSlotCnt() - 1; i >= 0; i--) {
        entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i),
            getSlotLength(i), keyType, NodeType.INDEX);

        if (QuadrupleBT.keyCompare(key, entry.key) >= 0) {
          return ((IndexData) entry.data).getData();
        }
      }

      return getPrevPage();
    } catch (Exception e) {
      throw new IndexSearchException(e, "Get entry failed");
    }

  } // getPageNoByKey

  /**
   * Iterators.
   * One of the two functions: getFirst and getNext
   * which provide an iterator interface to the quadruples on a
   * QuadrupleBTIndexPage.
   * 
   * @param lid It will be modified and the first lid in the index page
   *            will be passed out by itself. Input and Output parameter.
   * @return return the first KeyDataEntry in the index page.
   *         null if NO MORE quadruple
   * @exception IteratorException iterator error
   */
  public KeyDataEntry getFirst(QID qid)
      throws IteratorException {

    KeyDataEntry entry;

    try {
      qid.pageNo = getCurPage();
      qid.slotNo = 0; // begin with first slot

      if (getSlotCnt() == 0) {
        return null;
      }

      entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(0),
          getSlotLength(0),
          keyType, NodeType.INDEX);

      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get first entry failed");
    }

  } // end of getFirst

  /**
   * Iterators.
   * One of the two functions: get_first and get_next which provide an
   * iterator interface to the quadruples on a QuadrupleBTIndexPage.
   * 
   * @param lid It will be modified and next lid will be passed out by itself.
   *            Input and Output parameter.
   * @return return the next KeyDataEntry in the index page.
   *         null if no more quadruple
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

      entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i),
          getSlotLength(i),
          keyType, NodeType.INDEX);

      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get next entry failed");
    }
  } // end of getNext

  /**
   * Left Link
   * You will recall that the index pages have a left-most
   * pointer that is followed whenever the search key value
   * is less than the least key value in the index node. The
   * previous page pointer is used to implement the left link.
   * 
   * @return It returns the left most link.
   * @exception IOException error from the lower layer
   */
  protected PageId getLeftLink()
      throws IOException {
    return getPrevPage();
  }

  /**
   * You will recall that the index pages have a left-most
   * pointer that is followed whenever the search key value
   * is less than the least key value in the index node. The
   * previous page pointer is used to implement the left link.
   * The function sets the left link.
   * 
   * @param left the PageId of the left link you wish to set. Input parameter.
   * @exception IOException I/O errors
   */
  protected void setLeftLink(PageId left)
      throws IOException {
    setPrevPage(left);
  }

  /*
   * It is used in full delete
   * 
   * @param key the key is used to search. Input parameter.
   * 
   * @param pageNo It returns the pageno of the sibling. Input and Output
   * parameter.
   * 
   * @return 0 if no sibling; -1 if left sibling; 1 if right sibling.
   * 
   * @exception IndexFullDeleteException delete failed
   */

  int getSibling(KeyClass key, PageId pageNo)
      throws IndexFullDeleteException {

    try {
      if (getSlotCnt() == 0) // there is no sibling
        return 0;

      int i;
      KeyDataEntry entry;
      for (i = getSlotCnt() - 1; i >= 0; i--) {
        entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i),
            getSlotLength(i), keyType, NodeType.INDEX);
        if (QuadrupleBT.keyCompare(key, entry.key) >= 0) {
          if (i != 0) {
            entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i - 1),
                getSlotLength(i - 1), keyType, NodeType.INDEX);
            pageNo.pid = ((IndexData) entry.data).getData().pid;
            return -1; // left sibling
          } else {
            pageNo.pid = getLeftLink().pid;
            return -1; // left sibling
          }
        }
      }
      entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(0),
          getSlotLength(0), keyType, NodeType.INDEX);
      pageNo.pid = ((IndexData) entry.data).getData().pid;
      return 1; // right sibling
    } catch (Exception e) {
      throw new IndexFullDeleteException(e, "Get sibling failed");
    }
  } // end of getSibling

  /*
   * find the position for old key by findKeyData,
   * where the newKey will be returned .
   * 
   * @newKey It will replace certain key in index page. Input parameter.
   * 
   * @oldKey It helps us to find which key will be replaced by
   * the newKey. Input parameter.
   * 
   * @return false if no key was found; true if success.
   * 
   * @exception IndexFullDeleteException delete failed
   */

  boolean adjustKey(KeyClass newKey, KeyClass oldKey)
      throws IndexFullDeleteException {

    try {

      KeyDataEntry entry;
      entry = findKeyData(oldKey);
      if (entry == null)
        return false;

      QID qid = deleteKey(entry.key);
      if (qid == null)
        throw new IndexFullDeleteException(null, "Qid is null");

      qid = insertKey(newKey, ((IndexData) entry.data).getData());
      if (qid == null)
        throw new IndexFullDeleteException(null, "Qid is null");

      return true;
    } catch (Exception e) {
      throw new IndexFullDeleteException(e, "Adjust key failed");
    }
  } // end of adjustKey

  /*
   * find entry for key by Quadruple B+ tree algorithm,
   * but entry.key may not equal KeyDataEntry.key returned.
   * 
   * @param key input parameter.
   * 
   * @return return that entry if found; otherwise return null;
   * 
   * @exception IndexSearchException index search failed
   * 
   */
  KeyDataEntry findKeyData(KeyClass key)
      throws IndexSearchException {
    KeyDataEntry entry;

    try {

      for (int i = getSlotCnt() - 1; i >= 0; i--) {
        entry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(i),
            getSlotLength(i), keyType, NodeType.INDEX);

        if (QuadrupleBT.keyCompare(key, entry.key) >= 0) {
          return entry;
        }
      }
      return null;
    } catch (Exception e) {
      throw new IndexSearchException(e, "finger key data failed");
    }
  } // end of findKeyData

  /*
   * find a key by B++ algorithm,
   * but returned key may not equal the key passed in.
   * 
   * @param key input parameter.
   * 
   * @return return that key if found; otherwise return null;
   * 
   * @exception IndexSearchException index search failed
   * 
   */
  KeyClass findKey(KeyClass key)
      throws IndexSearchException {
    return findKeyData(key).key;
  }

  /*
   * It is used in full delete
   * 
   * @param indexPage the sibling page of this. Input parameter.
   * 
   * @param parentIndexPage the parant of indexPage and this. Input parameter.
   * 
   * @param direction -1 if "this" is left sibling of indexPage ;
   * 1 if "this" is right sibling of indexPage. Input parameter.
   * 
   * @param deletedKey the key which was already deleted, and cause
   * redistribution. Input parameter.
   * 
   * @exception RedistributeException Redistribution failed
   * 
   * @return true if redistrbution success. false if we can not redistribute them.
   */
  boolean redistribute(QuadrupleBTIndexPage indexPage, QuadrupleBTIndexPage parentIndexPage,
      int direction, KeyClass deletedKey)
      throws RedistributeException {

    // assertion: indexPage and parentIndexPage are pinned
    try {
      boolean st;
      if (direction == -1) { // 'this' is the left sibling of indexPage
        if ((getSlotLength(getSlotCnt() - 1) + available_space()) > ((MAX_SPACE - DPFIXED) / 2)) {
          // cannot spare a quadruple for its underflow sibling
          return false;
        } else {
          // get its sibling's first quadruple's key
          QID dummyQid = new QID();
          KeyDataEntry firstEntry, lastEntry;
          firstEntry = indexPage.getFirst(dummyQid);

          // get the entry pointing to the right sibling

          KeyClass splitKey = parentIndexPage.findKey(firstEntry.key);

          // get the leftmost child pointer of the right sibling
          PageId leftMostPageId = indexPage.getLeftLink();

          // insert <splitKey,leftMostPageId> to its sibling
          indexPage.insertKey(splitKey, leftMostPageId);

          // get the last quadruple of itself
          lastEntry = QuadrupleBT.getEntryFromBytes(getpage(), getSlotOffset(getSlotCnt() - 1),
              getSlotLength(getSlotCnt() - 1), keyType, NodeType.INDEX);

          // set sibling's leftmostchild to be lastPageId
          indexPage.setLeftLink(((IndexData) (lastEntry.data)).getData());

          // delete the last quadruple from the old page
          QID delQid = new QID();
          delQid.pageNo = getCurPage();
          delQid.slotNo = getSlotCnt() - 1;

          if (deleteSortedQuadruple(delQid) == false)
            throw new RedistributeException(null, "Delete quadruple failed");

          // adjust the entry pointing to sibling in its parent
          if (deletedKey != null)
            st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);

          else
            st = parentIndexPage.adjustKey(lastEntry.key, splitKey);
          if (st == false)
            throw new RedistributeException(null, "adjust key failed");
          return true;
        }
      } else { // 'this' is the right sibling of indexPage
        if ((getSlotLength(0) + available_space()) > ((MAX_SPACE - DPFIXED) / 2)) {
          // cannot spare a quadruple for its underflow sibling
          return false;
        } else {
          // get the first quadruple
          KeyDataEntry firstEntry;
          firstEntry = QuadrupleBT.getEntryFromBytes(getpage(),
              getSlotOffset(0),
              getSlotLength(0), keyType, NodeType.INDEX);

          // get its leftmost child pointer
          PageId leftMostPageId = getLeftLink();

          // get the entry in its parent pointing to itself
          KeyClass splitKey;
          splitKey = parentIndexPage.findKey(firstEntry.key);

          // insert <split, leftMostPageId> to its left sibling

          indexPage.insertKey(splitKey, leftMostPageId);

          // set its new leftmostchild
          setLeftLink(((IndexData) (firstEntry.data)).getData());

          // delete the first quadruple
          QID delQid = new QID();
          delQid.pageNo = getCurPage();
          delQid.slotNo = 0;
          if (deleteSortedQuadruple(delQid) == false)
            throw new RedistributeException(null, "delete quadruple failed");

          // adjust the entry pointing to itself in its parent
          if (parentIndexPage.adjustKey(firstEntry.key, splitKey) == false)
            throw new RedistributeException(null, "adjust key failed");
          return true;
        }
      } // else
    } // try
    catch (Exception e) {
      throw new RedistributeException(e, "redistribute failed");
    }
  } // end of redistribute
};