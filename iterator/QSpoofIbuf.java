package iterator;

import quadrupleheap.*;          
import global.*;
import diskmgr.*;
import bufmgr.*;

import java.io.*;

public class QSpoofIbuf implements GlobalConst  {
  
  /**
   *constructor, use the init to initialize
   */
  public void QSpoofIbuf()
    {
      
      hf_scan = null;
    }
  
 
  /**
   *Initialize some necessary inormation, call Iobuf to create the
   *object, and call init to finish intantiation
   *@param n_pages the numbers of page of this buffer
   *@param fd the reference to an Heapfile
   *@param Nquadruples the quadruple numbers of the page
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public  void init(QuadrupleHeapfile fd, byte bufs[][], int n_pages, int Nquadruples)
    throws IOException,
	   Exception
    {
      _fd       = fd;       _bufs        = bufs;
      _n_pages  = n_pages;  q_size       = RDF_QUADRUPLE_SIZE;
      
      q_proc    = 0;        q_in_buf     = 0;
      tot_q_proc= 0;
      curr_page = 0;        q_rd_from_pg = 0;
      done      = false;    q_per_pg     = MINIBASE_PAGESIZE / q_size;
     
      
      n_quadruples = Nquadruples;
     
      // open a scan
      if (hf_scan != null)  hf_scan = null;
      
      try {
	    hf_scan = new TScan(fd);
      }
      catch(Exception e){
	throw e;
      }
      
      
    }
  
   /** 
   *get a tuple from current buffer,pass reference buf to this method
   *usage:temp_tuple = tuple.Get(buf); 
   *@param buf write the result to buf
   *@return the result tuple
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public  Quadruple Get(Quadruple  buf)throws IOException, Exception
    {
      if (tot_q_proc == n_quadruples) done = true;
      
      if (done == true){buf = null; return null;}
      if (q_proc == q_in_buf)
	{
	  try {
	    q_in_buf = readin();
	  }
	  catch (Exception e){
	    throw e;
	  }
	  curr_page = 0; q_rd_from_pg = 0; q_proc = 0;
	}
      
      if (q_in_buf == 0)                        // No tuples read in?
	{
	  done = true; buf = null;return null;
	}
 
      buf.quadrupleSet(_bufs[curr_page],q_rd_from_pg*q_size,q_size);
      tot_q_proc++;
      
      // Setup for next read
      q_rd_from_pg++; q_proc++;
      if (q_rd_from_pg == q_per_pg)
	{
	  q_rd_from_pg = 0; curr_page++;
	}
      return buf;
    }
  
   
  /**
   *@return if the buffer is empty,return true. otherwise false
   */
  public  boolean empty()
    {
      if (tot_q_proc == n_quadruples) done = true;
      return done;
    }
  
  /**
   *
   *@return the numbers of tuples in the buffer
   *@exception IOException some I/O fault
   *@exception InvalidQuadrupleSizeException Heapfile error
   */
  private int readin()throws IOException,InvalidQuadrupleSizeException
    {
      int   q_read = 0, tot_read = 0;
      Quadruple q      = new Quadruple ();
      byte[] q_copy;
      
      curr_page = 0;
      while (curr_page < _n_pages)
	{
	  while (q_read < q_per_pg)
	    {
	      QID qid =new QID();
	      try {
		if ( (q = hf_scan.getNext(qid)) == null) return tot_read;
		q_copy = q.getQuadrupleByteArray();
		System.arraycopy(q_copy,0,_bufs[curr_page],q_read*q_size,q_size); 
	      }
	      catch (Exception e) {
		System.err.println (""+e);
	      }
	      q_read++; tot_read++;
	    } 
	  q_read     = 0;
	  curr_page++;
	}
      return tot_read;
    }
  
  
  private  byte[][] _bufs;
  
  private  int   TEST_fd;
  
  private  QuadrupleHeapfile _fd;
  private  TScan hf_scan;
  private  int    _n_pages;
  private  int    q_size;
  
  private  int    q_proc, q_in_buf;
  private  int    tot_q_proc;
  private  int    q_rd_from_pg, curr_page;
  private  int    q_per_pg;
  private  boolean   done;
  private  int    n_quadruples;
}


