package btree;

import java.io.*;
import global.*;

/**
 * Base class for a index file
 */
public abstract class QuadrupleIndexFile {
  /**
   * Insert entry into the index file.
   * 
   * @param data the key for the entry
   * @param qid  the qid of the quadruple with the key
   * @exception IOException             from lower layers
   * @exception KeyTooLongException     the key is too long
   * @exception KeyNotMatchException    the keys do not match
   * @exception LeafInsertRecException  insert quadruple to leaf page failed
   * @exception IndexInsertRecException insert quadruple to index page failed
   * @exception ConstructPageException  fail to construct a header page
   * @exception UnpinPageException      unpin page failed
   * @exception PinPageException        pin page failed
   * @exception NodeNotMatchException   nodes do not match
   * @exception ConvertException        conversion failed (from global package)
   * @exception DeleteRecException      delete quadruple failed
   * @exception IndexSearchException    index search failed
   * @exception IteratorException       error from iterator
   * @exception LeafDeleteException     delete leaf page failed
   * @exception InsertException         insert quadruple failed
   */
  abstract public void insert(final KeyClass data, final QID qid)
      throws KeyTooLongException,
      KeyNotMatchException,
      LeafInsertRecException,
      IndexInsertRecException,
      ConstructPageException,
      UnpinPageException,
      PinPageException,
      NodeNotMatchException,
      ConvertException,
      DeleteRecException,
      IndexSearchException,
      IteratorException,
      LeafDeleteException,
      InsertException,
      IOException;

  /**
   * Delete entry from the index file.
   * 
   * @param data the key for the entry
   * @param qid  the qid of the quadruple with the key
   * @exception IOException               from lower layers
   * @exception DeleteFashionException    delete fashion undefined
   * @exception LeafRedistributeException failed to redistribute leaf page
   * @exception RedistributeException     redistrubtion failed
   * @exception InsertRecException        insert quadruple failed
   * @exception KeyNotMatchException      keys do not match
   * @exception UnpinPageException        unpin page failed
   * @exception IndexInsertRecException   insert quadruple to index failed
   * @exception FreePageException         free page failed
   * @exception RecordNotFoundException   failed to find the quadruple
   * @exception PinPageException          pin page failed
   * @exception IndexFullDeleteException  full delete on index page failed
   * @exception LeafDeleteException       delete leaf page failed
   * @exception IteratorException         exception from iterating through records
   * @exception ConstructPageException    fail to construct the header page
   * @exception DeleteRecException        delete quadruple failed
   * @exception IndexSearchException      index search failed
   */
  abstract public boolean Delete(final KeyClass data, final QID qid)
      throws DeleteFashionException,
      LeafRedistributeException,
      RedistributeException,
      InsertRecException,
      KeyNotMatchException,
      UnpinPageException,
      IndexInsertRecException,
      FreePageException,
      RecordNotFoundException,
      PinPageException,
      IndexFullDeleteException,
      LeafDeleteException,
      IteratorException,
      ConstructPageException,
      DeleteRecException,
      IndexSearchException,
      IOException;
}