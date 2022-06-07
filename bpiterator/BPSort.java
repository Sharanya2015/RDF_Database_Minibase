package bpiterator;

import java.io.*;
import global.*;
import basicpattern.BasicPattern;
import heap.*;
import iterator.*;

/**
 * The BPSort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class BPSort extends BPIterator implements GlobalConst {
	private static final int ARBIT_RUNS = 10;

	private AttrType[] attrType;
	private short n_cols;
	private short[] str_lens;
	private BPFileScan _am;
	private int _sort_fld;
	private BPOrder order;
	private int _n_pages;
	private byte[][] bufs;
	private boolean first_time;
	private int Nruns;
	private int max_elems_in_heap;
	private int tuple_size;

	private BPpnodeSplayPQ Q;
	private Heapfile[] temp_files;
	private int n_tempfiles;
	private Tuple opTuple;
	private int[] n_tuples;
	private int n_runs;
	private Tuple op_buf;
	private OBuf o_buf;
	private SpoofIbuf[] i_buf;
	private PageId[] bufs_pids;
	private boolean useBM = true; // flag for whether to use buffer manager

	/**
	 * Set up for merging the runs.
	 * Open an input buffer for each run, and insert the first element (min)
	 * from each run into a heap. <code>delete_min() </code> will then get
	 * the minimum of all runs.
	 * 
	 * @param tuple_size size (in bytes) of each tuple
	 * @param n_R_runs   number of runs
	 * @exception IOException     from lower layers
	 * @exception LowMemException there is not enough memory to
	 *                            sort in two passes (a subclass of
	 *                            BPSortException).
	 * @exception BPSortException something went wrong in the lower layer.
	 * @exception Exception       other exceptions
	 */
	private void setup_for_merge(int tuple_size, int n_R_runs)
			throws IOException,
			LowMemException,
			BPSortException,
			Exception {
		// don't know what will happen if n_R_runs > _n_pages
		if (n_R_runs > _n_pages)
			throw new LowMemException("BPSort.java: Not enough memory to sort in two passes.");

		int i;
		BP_Pnode cur_node; // need pq_defs.java

		i_buf = new SpoofIbuf[n_R_runs]; // need io_bufs.java
		for (int j = 0; j < n_R_runs; j++)
			i_buf[j] = new SpoofIbuf();

		// construct the lists, ignore TEST for now
		// this is a patch, I am not sure whether it works well -- bingjie 4/20/98

		for (i = 0; i < n_R_runs; i++) {
			byte[][] apage = new byte[1][];
			apage[0] = bufs[i];

			// need iobufs.java
			i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);

			cur_node = new BP_Pnode();
			cur_node.run_num = i;

			// may need change depending on whether Get() returns the original
			// or make a copy of the tuple, need io_bufs.java ???
			Tuple temp_tuple = new Tuple(tuple_size);

			try {
				temp_tuple.setHdr(n_cols, attrType, str_lens);
			} catch (Exception e) {
				throw new BPSortException(e, "BPSort.java: Tuple.setHdr() failed");
			}

			temp_tuple = i_buf[i].Get(temp_tuple); // need io_bufs.java

			if (temp_tuple != null) {
				/*
				 * System.out.print("Get tuple from run " + i);
				 * temp_tuple.print(_in);
				 */
				cur_node.tuple = temp_tuple; // no copy needed
				try {
					Q.enq(cur_node);
				} catch (BPUtilsException e) {
					throw new BPSortException(e, "BPSort.java: BPUtilsException caught from Q.enq()");
				} catch (UnknowAttrType e) {
					throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from Q.enq()");
				}
			}
		}
		return;
	}

	/**
	 * Generate sorted runs.
	 * 
	 * @param max_elems
	 * @param sortFldType
	 * @return
	 * @throws IOException
	 * @throws BPSortException
	 * @throws BPUtilsException
	 * @throws JoinsException
	 * @throws UnknowAttrType
	 * @throws Exception
	 */
	private int generate_runs(int max_elems, AttrType sortFldType)
			throws IOException,
			BPSortException,
			BPUtilsException,
			JoinsException,
			UnknowAttrType,
			Exception {
		Tuple tuple;
		BP_Pnode cur_node;
		int flag = 1;
		BasicPattern basicPattern;
		BPpnodeSplayPQ Q1 = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
		BPpnodeSplayPQ Q2 = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
		BPpnodeSplayPQ pcurr_Q = Q1;
		BPpnodeSplayPQ pother_Q = Q2;

		int run_num = 0; // keeps track of the number of runs
		int tupleComp;

		int p_elems_curr_Q = 0;
		int p_elems_other_Q = 0;

		while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
			try {
				basicPattern = _am.get_next();
				if (basicPattern != null)
					tuple = basicPattern.getBasicPatternTuple();
				else
					tuple = null;
				if (flag == 1) {
					AttrType[] attrTypes = new AttrType[tuple.noOfFlds()];
					int j = 0;
					while (j < (tuple.noOfFlds() - 1)) {
						attrTypes[j] = new AttrType(AttrType.attrInteger);
						j++;
					}
					attrTypes[j] = new AttrType(AttrType.attrDouble);

					if (_sort_fld == -1) {
						_sort_fld = tuple.noOfFlds();
						BPpnodeSplayPQ Q1_confidence = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
						BPpnodeSplayPQ Q2_confidence = new BPpnodeSplayPQ(_sort_fld, sortFldType, order);
						pcurr_Q = Q1_confidence;
						pother_Q = Q2_confidence;
					}

					short[] stringLengths = new short[1];
					stringLengths[0] = (short) ((tuple.noOfFlds() - 1) * 4 + 1 * 8);

					initializeSort(attrTypes, tuple.noOfFlds(), stringLengths);
					flag = 0;
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new BPSortException(e, "BPSort.java: get_next() failed");
			}

			if (tuple == null) {
				break;
			}
			cur_node = new BP_Pnode();
			cur_node.tuple = new Tuple(tuple); // tuple copy needed -- Bingjie 4/29/98

			pcurr_Q.enq(cur_node);
			p_elems_curr_Q++;
		}

		Tuple lastElem = new Tuple(tuple_size); // need tuple.java
		try {
			lastElem.setHdr(n_cols, attrType, str_lens);
		} catch (Exception e) {
			throw new BPSortException(e, "BPSort.java: setHdr() failed");
		}

		// set the lastElem to be the minimum value for the sort field
		if (order.bpOrder == BPOrder.Ascending) {
			try {
				MIN_VAL(lastElem, sortFldType);
			} catch (UnknowAttrType e) {
				throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from MIN_VAL()");
			} catch (Exception e) {
				throw new BPSortException(e, "MIN_VAL failed");
			}
		} else {
			try {
				MAX_VAL(lastElem, sortFldType);
			} catch (UnknowAttrType e) {
				throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from MAX_VAL()");
			} catch (Exception e) {
				throw new BPSortException(e, "MIN_VAL failed");
			}
		}

		// now the queue is full, starting writing to file while keep trying
		// to add new tuples to the queue. The ones that does not fit are put
		// on the other queue temperarily
		while (true) {
			cur_node = pcurr_Q.deq();
			if (cur_node == null)
				break;
			p_elems_curr_Q--;

			tupleComp = BPUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);

			if ((tupleComp < 0 && order.bpOrder == BPOrder.Ascending)
					|| (tupleComp > 0 && order.bpOrder == BPOrder.Descending)) {
				// doesn't fit in current run, put into the other queue
				try {
					pother_Q.enq(cur_node);
				} catch (UnknowAttrType e) {
					throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from Q.enq()");
				}
				p_elems_other_Q++;
			} else {
				// set lastElem to have the value of the current tuple,
				// need tuple_utils.java
				BPUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, sortFldType);
				// write tuple to output file, need io_bufs.java, type cast???
				// System.out.println("Putting tuple into run " + (run_num + 1));
				// cur_node.tuple.print(_in);

				o_buf.Put(cur_node.tuple);
			}

			// check whether the other queue is full
			if (p_elems_other_Q == max_elems) {
				// close current run and start next run
				n_tuples[run_num] = (int) o_buf.flush(); // need io_bufs.java
				run_num++;

				// check to see whether need to expand the array
				if (run_num == n_tempfiles) {
					Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
					for (int i = 0; i < n_tempfiles; i++) {
						temp1[i] = temp_files[i];
					}
					temp_files = temp1;
					n_tempfiles *= 2;

					int[] temp2 = new int[2 * n_runs];
					for (int j = 0; j < n_runs; j++) {
						temp2[j] = n_tuples[j];
					}
					n_tuples = temp2;
					n_runs *= 2;
				}

				try {
					temp_files[run_num] = new Heapfile(null);
				} catch (Exception e) {
					throw new BPSortException(e, "BPSort.java: create Heapfile failed");
				}

				// need io_bufs.java
				o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

				// set the last Elem to be the minimum value for the sort field
				if (order.bpOrder == BPOrder.Ascending) {
					try {
						MIN_VAL(lastElem, sortFldType);
					} catch (UnknowAttrType e) {
						throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from MIN_VAL()");
					} catch (Exception e) {
						throw new BPSortException(e, "MIN_VAL failed");
					}
				} else {
					try {
						MAX_VAL(lastElem, sortFldType);
					} catch (UnknowAttrType e) {
						throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from MAX_VAL()");
					} catch (Exception e) {
						throw new BPSortException(e, "MIN_VAL failed");
					}
				}

				// switch the current heap and the other heap
				BPpnodeSplayPQ tempQ = pcurr_Q;
				pcurr_Q = pother_Q;
				pother_Q = tempQ;
				int tempelems = p_elems_curr_Q;
				p_elems_curr_Q = p_elems_other_Q;
				p_elems_other_Q = tempelems;
			} else if (p_elems_curr_Q == 0) // now check whether the current queue is empty
			{
				while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
					try {
						basicPattern = _am.get_next();
						if (basicPattern != null) {
							tuple = basicPattern.getBasicPatternTuple();
						} else {
							tuple = null;
						}
					} catch (Exception e) {
						throw new BPSortException(e, "get_next() failed");
					}

					if (tuple == null) {
						break;
					}
					cur_node = new BP_Pnode();
					cur_node.tuple = new Tuple(tuple); // tuple copy needed -- Bingjie 4/29/98

					try {
						pcurr_Q.enq(cur_node);
					} catch (UnknowAttrType e) {
						throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from Q.enq()");
					}
					p_elems_curr_Q++;
				}
			}

			// Check if we are done
			if (p_elems_curr_Q == 0) {
				// current queue empty despite our attemps to fill in
				// indicating no more tuples from input
				if (p_elems_other_Q == 0) {
					// other queue is also empty, no more tuples to write out, done
					break; // of the while(true) loop
				} else {
					// generate one more run for all tuples in the other queue
					// close current run and start next run
					n_tuples[run_num] = (int) o_buf.flush(); // need io_bufs.java
					run_num++;

					// check to see whether need to expand the array
					if (run_num == n_tempfiles) {
						Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
						for (int i = 0; i < n_tempfiles; i++) {
							temp1[i] = temp_files[i];
						}
						temp_files = temp1;
						n_tempfiles *= 2;

						int[] temp2 = new int[2 * n_runs];
						for (int j = 0; j < n_runs; j++) {
							temp2[j] = n_tuples[j];
						}
						n_tuples = temp2;
						n_runs *= 2;
					}

					try {
						temp_files[run_num] = new Heapfile(null);
					} catch (Exception e) {
						throw new BPSortException(e, "BPSort.java: create Heapfile failed");
					}

					// need io_bufs.java
					o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

					// set the last Elem to be the minimum value for the sort field
					if (order.bpOrder == BPOrder.Ascending) {
						try {
							MIN_VAL(lastElem, sortFldType);
						} catch (UnknowAttrType e) {
							throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from MIN_VAL()");
						} catch (Exception e) {
							throw new BPSortException(e, "MIN_VAL failed");
						}
					} else {
						try {
							MAX_VAL(lastElem, sortFldType);
						} catch (UnknowAttrType e) {
							throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from MAX_VAL()");
						} catch (Exception e) {
							throw new BPSortException(e, "MIN_VAL failed");
						}
					}

					// switch the current heap and the other heap
					BPpnodeSplayPQ tempQ = pcurr_Q;
					pcurr_Q = pother_Q;
					pother_Q = tempQ;
					int tempelems = p_elems_curr_Q;
					p_elems_curr_Q = p_elems_other_Q;
					p_elems_other_Q = tempelems;
				}
			} // end of if (p_elems_curr_Q == 0)
		} // end of while (true)

		// close the last run
		n_tuples[run_num] = (int) o_buf.flush();
		run_num++;
		return run_num;
	}

	public void initializeSort(AttrType[] attrTypeArr, short len, short[] sSizeArr)
			throws IOException, BPSortException {

		int strPtr = 0;
		attrType = new AttrType[len];
		int i = 0;
		while (i < len) {
			attrType[i] = new AttrType(attrTypeArr[i].attrType);

			if (attrTypeArr[i].attrType == AttrType.attrString) {
				strPtr++;
			}
			i++;
		}
		n_cols = len;

		str_lens = new short[strPtr];

		strPtr = 0;
		i = 0;
		while (i < len) {
			if (attrType[i].attrType == AttrType.attrString) {
				str_lens[strPtr] = sSizeArr[strPtr];
				strPtr++;
			}
			i++;
		}

		Tuple tup = new Tuple();
		try {
			tup.setHdr(len, attrType, sSizeArr);
		} catch (Exception e) {
			throw new BPSortException(e, "BPSort.java: t.setHdr() failed");
		}
		tuple_size = tup.size();

		bufs_pids = new PageId[_n_pages];
		bufs = new byte[_n_pages][];

		if (useBM) {
			try {
				get_buffer_pages(_n_pages, bufs_pids, bufs);
			} catch (Exception e) {
				throw new BPSortException(e, "BPSort.java: BUFmgr error");
			}
		} else {
			for (int k = 0; k < _n_pages; k++)
				bufs[k] = new byte[MAX_SPACE];
		}

		// as a heuristic, we set the number of runs to an arbitrary value
		// of ARBIT_RUNS
		temp_files = new Heapfile[ARBIT_RUNS];
		n_tempfiles = ARBIT_RUNS;
		n_tuples = new int[ARBIT_RUNS];
		n_runs = ARBIT_RUNS;

		try {
			temp_files[0] = new Heapfile(null);
		} catch (Exception e) {
			throw new BPSortException(e, "BPSort.java: Heapfile error");
		}

		o_buf = new OBuf();

		o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
		// output_tuple = null;

		Q = new BPpnodeSplayPQ(_sort_fld, attrTypeArr[_sort_fld - 1], order);

		op_buf = new Tuple(tuple_size); // need Tuple.java
		try {
			op_buf.setHdr(n_cols, attrType, str_lens);
		} catch (Exception e) {
			throw new BPSortException(e, "BPSort.java: op_buf.setHdr() failed");
		}
	}

	/**
	 * Set lastElem to be the minimum value of the appropriate type
	 * 
	 * @param lastElem    the tuple
	 * @param sortFldType the sort field type
	 * @exception IOException    from lower layers
	 * @exception UnknowAttrType attrSymbol or attrNull encountered
	 */
	private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
			throws IOException,
			FieldNumberOutOfBoundException,
			UnknowAttrType {

		// short[] s_size = new short[Tuple.max_size]; // need Tuple.java
		// AttrType[] junk = new AttrType[1];
		// junk[0] = new AttrType(sortFldType.attrType);

		// short fld_no = 1;

		switch (sortFldType.attrType) {
			case AttrType.attrInteger:
				// lastElem.setHdr(fld_no, junk, null);
				lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
				lastElem.setIntFld(_sort_fld + 1, Integer.MIN_VALUE);
				break;
			case AttrType.attrDouble:
				// lastElem.setHdr(fld-no, junk, null);
				lastElem.setDoubleFld(_sort_fld, Double.MIN_VALUE);
				break;
			default:
				// don't know how to handle attrSymbol, attrNull
				// System.err.println("error in sort.java");
				throw new UnknowAttrType("BPSort.java: don't know how to handle attrSymbol, attrNull");
		}
		return;
	}

	/**
	 * Remove the minimum value among all the runs.
	 * 
	 * @return the minimum tuple removed
	 * @exception IOException     from lower layers
	 * @exception BPSortException something went wrong in the lower layer.
	 */
	private Tuple delete_min()
			throws IOException,
			BPSortException,
			Exception {
		BP_Pnode cur_node; // needs pq_defs.java
		Tuple new_tuple, old_tuple;

		cur_node = Q.deq();
		old_tuple = cur_node.tuple;
		/*
		 * System.out.print("Get ");
		 * old_tuple.print(_in);
		 */
		// we just removed one tuple from one run, now we need to put another
		// tuple of the same run into the queue
		if (i_buf[cur_node.run_num].empty() != true) {
			// run not exhausted
			new_tuple = new Tuple(tuple_size); // need tuple.java??

			try {
				new_tuple.setHdr(n_cols, attrType, str_lens);
			} catch (Exception e) {
				throw new BPSortException(e, "BPSort.java: setHdr() failed");
			}

			new_tuple = i_buf[cur_node.run_num].Get(new_tuple);
			if (new_tuple != null) {
				cur_node.tuple = new_tuple; // no copy needed -- I think Bingjie 4/22/98
				try {
					Q.enq(cur_node);
				} catch (UnknowAttrType e) {
					throw new BPSortException(e, "BPSort.java: UnknowAttrType caught from Q.enq()");
				} catch (BPUtilsException e) {
					throw new BPSortException(e, "BPSort.java: BasicPatternUtilsException caught from Q.enq()");
				}
			} else {
				throw new BPSortException("********** Wait a minute, I thought input is not empty ***************");
			}
		}
		// changed to return Tuple instead of return char array ????
		return old_tuple;
	}

	/**
	 * Set lastElem to be the maximum value of the appropriate type
	 * 
	 * @param lastElem    the tuple
	 * @param sortFldType the sort field type
	 * @exception IOException    from lower layers
	 * @exception UnknowAttrType attrSymbol or attrNull encountered
	 */
	private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
			throws IOException,
			FieldNumberOutOfBoundException,
			UnknowAttrType {

		// short[] s_size = new short[Tuple.max_size]; // need Tuple.java
		// AttrType[] junk = new AttrType[1];
		// junk[0] = new AttrType(sortFldType.attrType);

		// short fld_no = 1;

		switch (sortFldType.attrType) {
			case AttrType.attrInteger:
				// lastElem.setHdr(fld_no, junk, null);
				lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
				lastElem.setIntFld(_sort_fld + 1, Integer.MAX_VALUE);
				break;
			case AttrType.attrDouble:
				// lastElem.setHdr(fld_no, junk, null);
				lastElem.setDoubleFld(_sort_fld, Double.MAX_VALUE);
				break;
			default:
				// don't know how to handle attrSymbol, attrNull
				// System.err.println("error in sort.java");
				throw new UnknowAttrType("BPSort.java: don't know how to handle attrSymbol, attrNull");
		}

		return;
	}

	/**
	 * Class constructor: set up the sorting basic patterns
	 * 
	 * @param iter     an iterator for accessing the basic patterns
	 * @param bpOrder  the sorting order (ASCENDING, DESCENDING)
	 * @param fieldNo  the field number of the field to sort on ( -1 on confidence
	 *                 )
	 * @param pagesNum amount of memory (in pages) available for sorting
	 * @exception IOException     from lower layers
	 * @exception BPSortException something went wrong in the lower layer.
	 */
	public BPSort(BPFileScan iter, BPOrder bpOrder, int fieldNo, int pagesNum)
			throws IOException,
			BPSortException {
		_n_pages = pagesNum;
		_sort_fld = fieldNo;
		_am = iter;
		order = bpOrder;

		if (fieldNo != -1) {
			_sort_fld = fieldNo * 2 - 1;
		}

		first_time = true;
		max_elems_in_heap = 200;
	}

	/**
	 * Returns the next basic pattern in sorted order.
	 * 
	 * @return the next tuple, null if all basic patterns exhausted
	 * @exception IOException     from lower layers
	 * @exception UnknowAttrType  attribute type unknown
	 * @exception LowMemException memory low exception
	 * @exception BPSortException something went wrong in the lower layer.
	 * @exception JoinsException  from <code>generate_runs()</code>.
	 * @exception Exception       other exceptions
	 */
	public BasicPattern get_next()
			throws IOException,
			LowMemException,
			JoinsException,
			BPSortException,
			UnknowAttrType,
			Exception {
		if (first_time) {
			first_time = false;

			AttrType sortFldTyp;
			if (_sort_fld < 0)
				sortFldTyp = new AttrType(AttrType.attrDouble);
			else
				sortFldTyp = new AttrType(AttrType.attrInteger);
			Nruns = generate_runs(max_elems_in_heap, sortFldTyp);
			setup_for_merge(tuple_size, Nruns);
		}

		if (Q.empty()) {
			return null;
		}

		opTuple = delete_min();
		if (opTuple != null) {
			op_buf.tupleCopy(opTuple);
			BasicPattern basicPattern = new BasicPattern(opTuple);
			return basicPattern;
		} else {
			return null;
		}
	}

	/**
	 * Cleaning up, including releasing buffer pages from the buffer pool
	 * and removing temporary files from the database.
	 * 
	 * @exception IOException from lower layers
	 */
	public void close() throws IOException {
		// clean up
		if (!closeFlag) {

			try {
				_am.close();
			} catch (Exception e) {
				try {
					throw new BPSortException(e, "BPSort.java: error in closing iterator.");
				} catch (BPSortException e1) {
					e1.printStackTrace();
				}
			}

			if (useBM) {
				try {
					free_buffer_pages(_n_pages, bufs_pids);
				} catch (Exception e) {
					try {
						throw new BPSortException(e, "BPSort.java: BUFmgr error");
					} catch (BPSortException e1) {
						e1.printStackTrace();
					}
				}
				for (int i = 0; i < _n_pages; i++)
					bufs_pids[i].pid = INVALID_PAGE;
			}

			for (int i = 0; i < temp_files.length; i++) {
				if (temp_files[i] != null) {
					try {
						temp_files[i].deleteFile();
					} catch (Exception e) {
						try {
							throw new BPSortException(e, "BPSort.java: Heapfile error");
						} catch (BPSortException e1) {
							e1.printStackTrace();
						}
					}
					temp_files[i] = null;
				}
			}
			closeFlag = true;
		}
	}
}
