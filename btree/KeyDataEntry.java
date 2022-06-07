/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package btree;
import global.*;

/** KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
   /** key in the (key, data)
    */  
   public KeyClass key;
   /** data in the (key, data)
    */
   public DataClass data;
   private RID rid;
   
  /** Class constructor
   */
  public KeyDataEntry( Integer key2, PageId pageNo) {
     this.key = new IntegerKey(key2); 
     this.data = new IndexData(pageNo);
  }; 



  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, PageId pageNo) {

     data = new IndexData(pageNo); 
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());    
  };


  /** Class constructor.
   */
  public KeyDataEntry( String key, PageId pageNo) {
     this.key = new StringKey(key); 
     this.data = new IndexData(pageNo);
  };

  /** Class constructor.
   */
  public KeyDataEntry( Integer key, RID rid) {
     this.key = new IntegerKey(key); 
     this.data = new LeafData(rid);
  };

  /** Class constructor.
   */
  public KeyDataEntry( Integer key, LID lid) {
     this.key = new IntegerKey(key); 
     this.data = new LabelLeafData(lid);
  };

  /** Class constructor.
   */
  public KeyDataEntry( Integer key, QID qid) {
     this.key = new IntegerKey(key); 
     this.data = new QuadrupleLeafData(qid);
  };

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, RID rid){
     data = new LeafData(rid); 
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());    
  };

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, LID lid){
     data = new LabelLeafData(lid); 
    if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());    
  };

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, QID qid){
     data = new QuadrupleLeafData(qid); 
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());    
  };

  /** Class constructor.
   */
  public KeyDataEntry( String key, RID rid) {
     this.key = new StringKey(key); 
     this.data = new LeafData(rid);
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( String key, LID lid) {
     this.key = new StringKey(key); 
     this.data = new LabelLeafData(lid);
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( String key, QID qid) {
     this.key = new StringKey(key); 
     this.data = new QuadrupleLeafData(qid);
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key,  DataClass data) {
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey()); 

     if ( data instanceof IndexData ) 
        this.data= new IndexData(((IndexData)data).getData());
     else if ( data instanceof LabelLeafData ) 
        this.data= new LabelLeafData(((LabelLeafData)data).getData()); 
     else if ( data instanceof QuadrupleLeafData ) 
        this.data= new QuadrupleLeafData(((QuadrupleLeafData)data).getData()); 
     else if ( data instanceof LeafData ) 
        this.data= new LeafData(((LeafData)data).getData()); 
  }

  /** shallow equal. 
   *  @param entry the entry to check again key. 
   *  @return true, if entry == key; else, false.
   */
  public boolean equals(KeyDataEntry entry) {
      boolean st1,st2;

      if ( key instanceof IntegerKey )
         st1= ((IntegerKey)key).getKey().equals
                  (((IntegerKey)entry.key).getKey());
      else 
         st1= ((StringKey)key).getKey().equals
                  (((StringKey)entry.key).getKey());

      if( data instanceof IndexData )
         st2= ( (IndexData)data).getData().pid==
              ((IndexData)entry.data).getData().pid ;
      else {
       	
	 if( data instanceof LabelLeafData )
         	st2= ((LID)((LabelLeafData)data).getData()).equals
                	(((LID)((LabelLeafData)entry.data).getData()));
	 else if( data instanceof QuadrupleLeafData )
         	st2= ((QID)((QuadrupleLeafData)data).getData()).equals
                	(((QID)((QuadrupleLeafData)entry.data).getData()));
	 else
	        st2= ((RID)((LeafData)data).getData()).equals
        	        (((RID)((LeafData)entry.data).getData()));
      }	
 
      return (st1&&st2);
  }     
}

