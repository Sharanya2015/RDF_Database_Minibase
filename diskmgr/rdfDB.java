package diskmgr;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import labelheap.Label;
import labelheap.LabelHeapFile;
import quadrupleheap.*;
import heap.*;

import java.util.ArrayList;


public class rdfDB extends DB {

    private String curr_rdfdb;
    private QuadrupleHeapfile TEMP_Quadruple_HF;


	private LabelHeapFile Predicate_HF;   		    // Predicates Heap file
    private int Quadruples_TotalCount = 0; 			// quadruples Count
    private int Entities_TotalCount = 0;         	// entities Count

	private QuadrupleHeapfile Quadruple_HF;
	private LabelBTreeFile Entity_BTree;

	private QuadrupleBTreeFile Quadruple_BTreeIndex;
	private LabelHeapFile Entity_HF; 	  		    // Entity Heap file to store subjects/objects
	private int Subjects_TotalCount = 0; 			// DISTINCT subjects
	private int Obj_Total_Count = 0; 			// DISTINCT objects
	private LabelBTreeFile dup_Obj_tree;    // BTree file to account for duplicate objects
	private LabelBTreeFile Predicate_BTree;
	private QuadrupleBTreeFile Quadruple_BTree;
	private int Pred_Total_Count = 0; 			// predicate Count
    private LabelBTreeFile dupl_B_tree;


	
	public QuadrupleHeapfile getQuadHandle() {
		return Quadruple_HF;
	}

	public LabelHeapFile getEntityHandle() {
		return Entity_HF;
	}

	public LabelHeapFile getPredicateHandle() {
		return Predicate_HF;
	}
	
	public QuadrupleHeapfile getTEMP_Quadruple_HF() {
		return TEMP_Quadruple_HF;
	}

	public QuadrupleBTreeFile getQuadruple_BTree()
			throws GetFileEntryException, PinPageException, ConstructPageException
	{
		QuadrupleBTreeFile Quadruple_BTree = new QuadrupleBTreeFile(curr_rdfdb+"/quadrupleBT");
		return Quadruple_BTree;
	}

    public QuadrupleBTreeFile getQuadruple_BTreeIndex() 
	throws GetFileEntryException, PinPageException, ConstructPageException 
	{
		QuadrupleBTreeFile Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex");
		return Quadruple_BTreeIndex;
	}


	/**
	 * Default Constructor
	 */
	public rdfDB() { }

	/**
	* Close RdfDB
	*/

    public void rdfcloseDB() 
	throws 	PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		try {

			if(Entity_BTree != null)
			{
				Entity_BTree.close();
			}
			if(Predicate_BTree != null)
			{
				Predicate_BTree.close();
			}
			if(Quadruple_BTree != null)
			{
				Quadruple_BTree.close();
			}
            if(dupl_B_tree != null)
			{
				dupl_B_tree.close();
			}
			if(dup_Obj_tree != null)
			{
				dup_Obj_tree.close();
			}
			if(Quadruple_BTreeIndex != null)
			{
				Quadruple_BTreeIndex.close();
			}
			if(TEMP_Quadruple_HF != null && TEMP_Quadruple_HF != getQuadHandle())
			{
				TEMP_Quadruple_HF.deleteFile();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open an existing rdf database
	 * @param dbname Database name
	 */
	public void openrdfDB(String dbname,int type)
	{
		curr_rdfdb = new String(dbname);
		try 
		{
			openDB(dbname);
			rdfDB(type);
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	/**
	 * Create a new RDF database
	 * @param dbname Database name
	 * @param num_pages Num of pages to allocate for the database
	 * @param type different indexing types to use for the database
	 */
	public void openrdfDB(String dbname,int num_pages,int type)
	{
		curr_rdfdb = new String(dbname);
		try
		{
			openDB(dbname,num_pages);
			rdfDB(type);
		}
		catch(Exception e)
		{

			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}


	/**Constructor for the RDF database.
	 * You can create as many btree index files as you want over these quadruple and label heap files
	 */

	public void rdfDB(int type) 
	{
		int keytype = AttrType.attrString;

		/** Initialize counter to zero **/ 
		 PCounter.initialize();
		
		try
		{ 
			TEMP_Quadruple_HF = new QuadrupleHeapfile("tempresult");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		try
		{ 
			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");

		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			Entity_HF = new LabelHeapFile(curr_rdfdb+"/entityHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			Predicate_HF = new LabelHeapFile(curr_rdfdb+"/predicateHF");
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			Entity_BTree = new LabelBTreeFile(curr_rdfdb+"/entityBT",keytype,255,1);
			Entity_BTree.close();
		}

		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			Predicate_BTree = new LabelBTreeFile(curr_rdfdb+"/predicateBT",keytype,255,1);
			Predicate_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
				
		try
		{
			Quadruple_BTree = new QuadrupleBTreeFile(curr_rdfdb+"/quadrupleBT",keytype,255,1);
			Quadruple_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
        
        try
		{
			dupl_B_tree = new LabelBTreeFile(curr_rdfdb+"/dupSubjBT",keytype,255,1);
			dupl_B_tree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try
		{
			dup_Obj_tree = new LabelBTreeFile(curr_rdfdb+"/dupObjBT",keytype,255,1);
			dup_Obj_tree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		try
		{
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error creating B tree index for given index option"+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

	/**
	 *  Return the number of predicates in the RDF DB
	 *  @return int number of Predicates
	 */
	public int getPredicateCnt()
	{
		try
		{
			Predicate_HF = new LabelHeapFile(curr_rdfdb+"/predicateHF");
			Pred_Total_Count = Predicate_HF.getLabelCnt();
		}
		catch (Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Pred_Total_Count;
	}

    /**
	 *  Returns the number of Quadruples in the RDF DB
	 *  @return int number of Quadruples
	 */ 

	public int getQuadrupleCnt()
	{	
		try
		{
			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");
			Quadruples_TotalCount = Quadruple_HF.getQuadCnt();
		}
		catch (Exception e) 
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Quadruples_TotalCount;
	}

    /**
     *  Return the number of entities in the RDF DB
     *  @return int number of Entities
     */ 
    public int getEntityCnt()
    {
            try
            {
                    Entity_HF = new LabelHeapFile(curr_rdfdb+"/entityHF");
                    Entities_TotalCount = Entity_HF.getLabelCnt();
            }
            catch (Exception e) 
            {
                    System.err.println (""+e);
                    e.printStackTrace();
                    Runtime.getRuntime().exit(1);
            }
            return Entities_TotalCount; 
    }

	public EID insertEntity(String EntityLabel)
	{
		int KeyType = AttrType.attrString;
		KeyClass key = new StringKey(EntityLabel);
		EID entityid = null;

		try
		{
			Entity_BTree = new LabelBTreeFile(curr_rdfdb+"/entityBT");

			LID lid = null;
			KeyClass low_key = new StringKey(EntityLabel);
			KeyClass high_key = new StringKey(EntityLabel);
			KeyDataEntry entry = null;
			LabelBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry!=null)
			{
				if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
				{
					lid =  ((LabelLeafData)entry.data).getData();
					entityid = lid.returnEID();
					scan.DestroyBTreeFileScan();
					Entity_BTree.close();
					return entityid;
				}
			}
			scan.DestroyBTreeFileScan();

			lid = Entity_HF.insertLabel(EntityLabel.getBytes());
			//Insert into Entity Btree file key,lid
			Entity_BTree.insert(key,lid);
			entityid = lid.returnEID();
			Entity_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println ("*** Error inserting entity " + e.getStackTrace() + e.getMessage() + e);
			e.printStackTrace();
		}

		return entityid;
	}

    /**
	 *  Return number of distinct subjects in the RDF DB
	 *  @return int number of subjects
	 */ 
	public int getSubjectCnt()
	{
        Subjects_TotalCount = 0;
        KeyDataEntry entry = null;
        KeyDataEntry dup_entry = null;
        try
        {
                Quadruple_BTree = new QuadrupleBTreeFile(curr_rdfdb+"/quadrupleBT");
                dupl_B_tree = new LabelBTreeFile(curr_rdfdb+"/dupSubjBT");
				QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(null,null);
                do
                {
                        entry = scan.get_next();
                        if(entry != null)
                        {
                                String label = ((StringKey)(entry.key)).getKey();
                                String[] temp;
								String delimiter = ":";
                                temp = label.split(delimiter);
                                String subject = temp[0] + temp[1];
								KeyClass low_key = new StringKey(subject);
                                KeyClass high_key = new StringKey(subject);
                                LabelBTFileScan duplicate_scan = dupl_B_tree.new_scan(low_key,high_key);
                                dup_entry = duplicate_scan.get_next();
                                if(dup_entry == null)
                                {
									dupl_B_tree.insert(low_key,new LID(new PageId(Integer.parseInt(temp[1])),Integer.parseInt(temp[0])));

                                }
                                duplicate_scan.DestroyBTreeFileScan();

                        }

                } while(entry!=null);
                scan.DestroyBTreeFileScan();
                Quadruple_BTree.close();

                KeyClass low_key = null;
                KeyClass high_key = null;
                LabelBTFileScan duplicate_scan = dupl_B_tree.new_scan(low_key,high_key);
                do
                {
                        dup_entry = duplicate_scan.get_next();
                        if(dup_entry!=null)
                            Subjects_TotalCount++;

                }while(dup_entry!=null);
                duplicate_scan.DestroyBTreeFileScan();
                dupl_B_tree.close();
        }
        catch(Exception e)
        {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        return Subjects_TotalCount;
	}

    /**
	 *  Returns Object Count
	 *  @return int number of subjects
	 */ 

	public int getObjectCnt()
	{
        Obj_Total_Count = 0;
        KeyDataEntry entry = null;
        KeyDataEntry duplicate_entry = null;
        try
        {
                Quadruple_BTree = new QuadrupleBTreeFile(curr_rdfdb+"/quadrupleBT");
                int keytype = AttrType.attrString;
                dup_Obj_tree = new LabelBTreeFile(curr_rdfdb+"/dupObjBT");
                QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(null,null);
                do
                {
                        entry = scan.get_next();
                        if(entry != null)
                        {
                                String label = ((StringKey)(entry.key)).getKey();
                                String[] temp;
                                String delimiter = ":";
                                temp = label.split(delimiter);
                                String object = temp[4] + temp[5];
                                KeyClass low_key = new StringKey(object);
                                KeyClass high_key = new StringKey(object);
                                LabelBTFileScan duplicate_scan = dup_Obj_tree.new_scan(low_key,high_key);
                                duplicate_entry = duplicate_scan.get_next();
                                if(duplicate_entry == null)
								{
                                        dup_Obj_tree.insert(low_key,new LID(new PageId(Integer.parseInt(temp[4])),Integer.parseInt(temp[5])));

                                }
                                duplicate_scan.DestroyBTreeFileScan();

                        }

                }while(entry!=null);
                scan.DestroyBTreeFileScan();
                Quadruple_BTree.close();

                KeyClass low_key = null;
                KeyClass high_key = null;
                LabelBTFileScan duplicate_scan = dup_Obj_tree.new_scan(low_key,high_key);
                do
                {
                    duplicate_entry = duplicate_scan.get_next();
                        if(duplicate_entry!=null)
                            Obj_Total_Count++;

                }while(duplicate_entry!=null);
                duplicate_scan.DestroyBTreeFileScan();
                dup_Obj_tree.close();
        }
        catch(Exception e)
        {
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }
        return Obj_Total_Count;
	}


	/**
	 * Removes the given entity label from the database.
	 * @return boolean will result in true (gain) when entity is deleted, else returns false (0)
	 */
	public boolean deleteEntity(java.lang.String EntityLabel)
	{
        boolean gain = false;
        int KeyType = AttrType.attrString;
        KeyClass key = new StringKey(EntityLabel);
        EID entityid = null;

        try
        {
                Entity_HF = new LabelHeapFile(curr_rdfdb+"/entityHF");
                Entity_BTree = new LabelBTreeFile(curr_rdfdb+"/entityBT");

                LID lid = null;
                KeyClass low_key = new StringKey(EntityLabel);
                KeyClass high_key = new StringKey(EntityLabel);
                KeyDataEntry entry = null;

                LabelBTFileScan scan = Entity_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry!=null)
                {
                        if(EntityLabel.equals(((StringKey)(entry.key)).getKey()))
                        {
                                lid =  ((LabelLeafData)entry.data).getData();
                                gain = Entity_HF.deleteLabel(lid) & Entity_BTree.Delete(low_key,lid);
                        }
                }
                Entity_BTree.close();
        }
        catch(Exception e)
        {
                System.err.println (" ***** Entity Deleting Error ***** " + e);
                e.printStackTrace();
        }
        return gain;
	}

	public boolean deletePredicate(java.lang.String PredicateLabel)
	{
		boolean gain = false;
		int KeyType = AttrType.attrString;
		KeyClass key = new StringKey(PredicateLabel);
		EID predicateid = null;

		try
		{
			Predicate_HF = new LabelHeapFile(curr_rdfdb+"/predicateHF");
			Predicate_BTree = new LabelBTreeFile(curr_rdfdb+"/predicateBT");

			LID lid = null;
			KeyClass low_key = new StringKey(PredicateLabel);
			KeyClass high_key = new StringKey(PredicateLabel);
			KeyDataEntry entry = null;

			LabelBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry!=null)
			{
				if(PredicateLabel.equals(((StringKey)(entry.key)).getKey()))
				{
					lid =  ((LabelLeafData)entry.data).getData();
					gain = Predicate_HF.deleteLabel(lid) & Predicate_BTree.Delete(low_key,lid);
				}
			}
			scan.DestroyBTreeFileScan();
			Predicate_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println ("***** Deleting Predicates Error ***** " + e);
			e.printStackTrace();
		}

		return gain;
	}

	/**
	 * Insert a entity into the EntityHeapFile
	 * @param PredicateLabel String representing Subject/Object
	 */
	public PID insertPredicate(String PredicateLabel)
	{
		PID predicateid = null;
		LID lid = null;

		int KeyType = AttrType.attrString;
		KeyClass key = new StringKey(PredicateLabel);

		try
		{
			Predicate_BTree = new LabelBTreeFile(curr_rdfdb+"/predicateBT");
			//LabelBT.printAllLeafPages(Predicate_BTree.getHeaderPage());
			KeyClass low_key = new StringKey(PredicateLabel);
			KeyClass high_key = new StringKey(PredicateLabel);
			KeyDataEntry entry = null;

			LabelBTFileScan scan = Predicate_BTree.new_scan(low_key,high_key);
			entry = scan.get_next();
			if(entry != null)
			{
				if(PredicateLabel.compareTo(((StringKey)(entry.key)).getKey()) == 0)
				{
					predicateid = ((LabelLeafData)(entry.data)).getData().returnPID();
					scan.DestroyBTreeFileScan();
					Predicate_BTree.close();
					return predicateid;
				}
			}
			scan.DestroyBTreeFileScan();
			lid = Predicate_HF.insertLabel(PredicateLabel.getBytes());
			Predicate_BTree.insert(key,lid);
			predicateid = lid.returnPID();
			Predicate_BTree.close();
		}
		catch(Exception e)
		{
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return predicateid;
	}


    /**
	 * Inserts the Quadruple into the DB
     * @return QID is returned with <subject, predicate, object> with max confidence
	 */

    public QID insertQuadruple(byte[] quadruplePtr)
	throws Exception
	{
		  QID quadrule_id;
          QID qid = null;
          try
          {
                  Quadruple_BTree = new QuadrupleBTreeFile(curr_rdfdb+"/quadrupleBT");

                  int sub_slotNo = Convert.getIntValue(0,quadruplePtr);
                  int sub_pageNo = Convert.getIntValue(4,quadruplePtr);
                  int pred_slotNo = Convert.getIntValue(8,quadruplePtr);
                  int pred_pageNo = Convert.getIntValue(12,quadruplePtr);
                  int obj_slotNo = Convert.getIntValue(16,quadruplePtr);
                  int obj_pageNo = Convert.getIntValue(20,quadruplePtr);
                  double confidence =Convert.getDoubleValue(24,quadruplePtr);
                  String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
                  KeyClass low_key = new StringKey(key);
                  KeyClass high_key = new StringKey(key);
                  KeyDataEntry entry = null;

                  QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(low_key,high_key);
                  entry = scan.get_next();
                  if(entry != null)
                  {
                          if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                          {

                                  quadrule_id = ((QuadrupleLeafData)(entry.data)).getData();
                                  Quadruple record = Quadruple_HF.getQuadruple(quadrule_id);
                                  double originalConf = record.getConfidence();
                                  if(originalConf < confidence)
                                  {
                                          Quadruple newRecord = new Quadruple(quadruplePtr,0,32);
                                          Quadruple_HF.updateQuadruple(quadrule_id,newRecord);
                                  }
                                  scan.DestroyBTreeFileScan();
                                  Quadruple_BTree.close();
                                  return quadrule_id;
                          }
                  }
                  qid= Quadruple_HF.insertQuadruple(quadruplePtr);

                  Quadruple_BTree.insert(low_key,qid);

                  scan.DestroyBTreeFileScan();
                  Quadruple_BTree.close();
          }
          catch(Exception e)
          {
                  System.err.println ("***** Quadruple record insertion error *****" + e);
                  e.printStackTrace();
                  Runtime.getRuntime().exit(1);
          }

          return qid;
	}

     /**
	 * Removes the given quadruple from the DB.
     * boolean @return success if quadruple is deleted, else return false (0)
	 */

    public boolean deleteQuadruple(byte[] quadruplePtr)
	{
        boolean success = false;
        QID quadruple_id = null;
        try
        {

                Quadruple_BTree = new QuadrupleBTreeFile(curr_rdfdb+"/quadrupleBT");
                int sub_slotNo = Convert.getIntValue(0,quadruplePtr);
                int sub_pageNo = Convert.getIntValue(4,quadruplePtr);
                int pred_slotNo = Convert.getIntValue(8,quadruplePtr);
                int pred_pageNo = Convert.getIntValue(12,quadruplePtr);
                int obj_slotNo = Convert.getIntValue(16,quadruplePtr);
                int obj_pageNo = Convert.getIntValue(20,quadruplePtr);
                double confidence =Convert.getDoubleValue(24,quadruplePtr);
                String key = new String(Integer.toString(sub_slotNo) +':'+ Integer.toString(sub_pageNo) +':'+ Integer.toString(pred_slotNo) + ':' + Integer.toString(pred_pageNo) +':' + Integer.toString(obj_slotNo) +':'+ Integer.toString(obj_pageNo));
                KeyClass low_key = new StringKey(key);
                KeyClass high_key = new StringKey(key);
                KeyDataEntry entry = null;

                // Scan the Btree to account for duplicate predicates
                QuadrupleBTFileScan scan = Quadruple_BTree.new_scan(low_key,high_key);
                entry = scan.get_next();
                if(entry != null)
                {
                        if(key.compareTo(((StringKey)(entry.key)).getKey()) == 0)
                        {
                                // Verified QID exists, hence return it
                                quadruple_id = ((QuadrupleLeafData)(entry.data)).getData();
                                if(quadruple_id!=null)
                                success = Quadruple_HF.deleteQuadruple(quadruple_id);
                        }
                }
                scan.DestroyBTreeFileScan();
                if(entry!=null)
                {
                if(low_key!=null && quadruple_id!=null)
                success = success & Quadruple_BTree.Delete(low_key,quadruple_id);
                }

                Quadruple_BTree.close();

        }
        catch(Exception e)
        {
                System.err.println ("Error : Quadruple Delete" + e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
        }

        return success;
	}




    public Stream openStream(String dbname,int indextype, int orderType, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter)
	{
		Stream streamObj= null;
		try {
			streamObj = new Stream( dbname,indextype,orderType, subjectFilter,  predicateFilter, objectFilter, confidenceFilter);
		} catch (InvalidQSlotNumberException e) {
			e.printStackTrace();
		} catch (InvalidQuadrupleSizeException e) {
			e.printStackTrace();
		} catch (HFException e) {
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return streamObj;

	}

	public Stream openStreamForBasicPattern(String dbname, String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter)
	{
		Stream streamObj= null;
		try {
			streamObj = new Stream( dbname, subjectFilter,  predicateFilter, objectFilter, confidenceFilter);
		} catch (InvalidQSlotNumberException e) {
			e.printStackTrace();
		} catch (InvalidQuadrupleSizeException e) {
			e.printStackTrace();
		} catch (HFException e) {
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return streamObj;
	}
	public void createIndex(int type)
	{

		switch(type)
		{
			case 1:
				buildIndex1();
				break;

			case 2:
				buildIndex2();
				break;

			case 3:
				buildIndex3();
				break;


			case 4:
				buildIndex4();
				break;

			case 5:
				buildIndex5();
				break;

		}
	}

	public void buildIndex1()
	{
		//Unclustered BTree on confidence using sorted Heap File
		try
		{
			if(Quadruple_BTreeIndex != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_rdfdb+"/Quadruple_BTreeIndex");
			}

			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();

			//scan sorted heap file and insert into btree index
			Quadruple_BTreeIndex = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex");
			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				KeyClass key = new StringKey(temp);
				Quadruple_BTreeIndex.insert(key,qid);

			}
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error while creating Index 1 " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}


	public void buildIndex2()
	{
		//Unclustered BTree Index file on subject and confidence
		try
		{
			//destroy existing index first
			if(Quadruple_BTreeIndex  != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_rdfdb+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();

			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex");
			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");
			Entity_HF = new LabelHeapFile(curr_rdfdb+"/entityHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				String subject = Entity_HF.getLabel(quadruple.getSubjectID().returnLID());
				KeyClass key = new StringKey(subject+":"+temp);
				Quadruple_BTreeIndex.insert(key,qid);
			}

			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error while creating Index 3" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

	public void buildIndex3()
	{
		//Unclustered BTree Index file on object and confidence
		try
		{
			//destroy existing index first
			if(Quadruple_BTreeIndex  != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_rdfdb+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();

			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex");
			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");
			Entity_HF = new LabelHeapFile(curr_rdfdb+"/entityHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				String object = Entity_HF.getLabel(quadruple.getObjectID().returnLID());
				KeyClass key = new StringKey(object+":"+temp);
				Quadruple_BTreeIndex.insert(key,qid);
			}

			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error while creating Index 3 " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

	public void buildIndex4()
	{
		//Unclustered BTree Index file on predicate and confidence
		try
		{
			//destroy existing index first
			if(Quadruple_BTreeIndex  != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_rdfdb+"/Quadruple_BTreeIndex");
			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex",keytype,255,1);
			Quadruple_BTreeIndex.close();

			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex");
			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");
			Predicate_HF = new LabelHeapFile(curr_rdfdb+"/predicateHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			double confidence = 0.0;
			while((quadruple = am.getNext(qid)) != null)
			{
				confidence = quadruple.getConfidence();
				String temp = Double.toString(confidence);
				String predicate = Predicate_HF.getLabel(quadruple.getPredicateID().returnLID());
				KeyClass key = new StringKey(predicate+":"+temp);
				Quadruple_BTreeIndex.insert(key,qid);
			}
			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println ("Error while creating Index 4 " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}


	}

	public void buildIndex5()
	{
		//Unclustered BTree Index file on the predicate
		try
		{
			//destroy existing index first
			if(Quadruple_BTreeIndex  != null)
			{
				Quadruple_BTreeIndex.close();
				Quadruple_BTreeIndex.destroyFile();
				destroyIndex(curr_rdfdb+"/Quadruple_BTreeIndex");

			}

			//create new
			int keytype = AttrType.attrString;
			Quadruple_BTreeIndex  = new QuadrupleBTreeFile(curr_rdfdb+"/Quadruple_BTreeIndex",keytype,255,1);

			Quadruple_HF = new QuadrupleHeapfile(curr_rdfdb+"/quadrupleHF");
			Predicate_HF = new LabelHeapFile(curr_rdfdb+"/predicateHF");
			TScan am = new TScan(Quadruple_HF);
			Quadruple quadruple = null;
			QID qid = new QID();
			KeyDataEntry entry = null;
			QuadrupleBTFileScan scan = null;


			while((quadruple = am.getNext(qid)) != null)
			{
				String predicate = Predicate_HF.getLabel(quadruple.getPredicateID().returnLID());
				KeyClass key = new StringKey(predicate);

				Quadruple_BTreeIndex.insert(key,qid);

			}

			am.closescan();
			Quadruple_BTreeIndex.close();
		}
		catch(Exception e)
		{
			System.err.println (" Error while creating Index 5 " + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	private void destroyIndex(String filename)
	{
		try
		{
			if(filename != null)
			{

				QuadrupleBTreeFile bfile = new QuadrupleBTreeFile(filename);

				QuadrupleBTFileScan scan = bfile.new_scan(null,null);
				QID qid = null;
				KeyDataEntry entry = null;
				ArrayList<KeyClass> keys = new ArrayList<KeyClass>();
				ArrayList<QID> qids = new ArrayList<QID>();
				int count = 0;

				while((entry = scan.get_next())!= null)
				{
					qid =  ((QuadrupleLeafData)entry.data).getData();
					keys.add(entry.key);
					qids.add(qid);
					count++;
				}
				scan.DestroyBTreeFileScan();

				for(int i = 0; i < count ;i++)
				{
					bfile.Delete(keys.get(i),qids.get(i));
				}

				bfile.close();

			}
		}
		catch(GetFileEntryException e1)
		{
			System.out.println("Index is Empty .. Expected");
		}
		catch(Exception e)
		{
			System.err.println (" Error destroying Index" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

}