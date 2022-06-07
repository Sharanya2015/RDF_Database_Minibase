package iterator; 

import global.*;
import bufmgr.*;
import diskmgr.*;
import quadrupleheap.*;

/**
 * A structure describing a Quadruple.
 * include a run number and the Quadruple
 */
public class Quadruplepnode {
  /** which run does this tuple belong */
  public int     run_num;

  /** the tuple reference */
  public Quadruple   quadruple;

  /**
   * class constructor, sets <code>run_num</code> to 0 and <code>Quadruple</code>
   * to null.
   */
  public Quadruplepnode()
  {
    run_num = 0;  // this may need to be changed
    quadruple = null; 
  }
  
  /**
   * class constructor, sets <code>run_num</code> and <code>Quadruple</code>.
   * @param runNum the run number
   * @param t      the tuple
   */
  public Quadruplepnode(int runNum, Quadruple t)
  {
    run_num = runNum;
    quadruple = t;
  }
  
}

