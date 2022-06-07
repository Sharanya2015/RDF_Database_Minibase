package bpiterator; 

import heap.*;

/**
 * A structure describing a basic pattern.
 * include a run number and the basic pattern
 */
public class BP_Pnode {
  /** which run does this basic pattern belong */
  public int run_num;

  /** the tuple reference */
  public Tuple tuple;

  /**
   * class constructor, sets <code>run_num</code> to 0 and <code>tuple</code>
   * to null.
   */
  public BP_Pnode() 
  {
    run_num = 0;
    tuple = null; 
  }
  
  /**
   * class constructor, sets <code>run_num</code> and <code>tuple</code>.
   * @param runNum the run number
   * @param t      the tuple
   */
  public BP_Pnode(int runNum, Tuple t) 
  {
    run_num = runNum;
    tuple = t;
  }
  
}

