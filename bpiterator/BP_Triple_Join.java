

package bpiterator;

import basicpattern.BasicPattern;
import basicpattern.BasicPatternFieldNumberOutOfBoundException;
import basicpattern.InvalidBasicPatternSizeException;
import diskmgr.Stream;
import global.EID;
import global.PID;
import global.QID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import index.IndexException;
import iterator.JoinsException;
import iterator.NestedLoopException;
import iterator.SortException;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class BP_Triple_Join extends BPIterator {
    int amt_of_mem;
    int num_left_nodes;
    BPIterator left_itr;
    int BPJoinNodePosition;
    int joinOnSubjectOrObject;
    String RightSubjectFilter;
    String RightPredicateFilter;
    String RightObjectFilter;
    double RightConfidenceFilter;
    int[] LeftOutNodePositions;
    int OutputRightSubject;
    int OutputRightObject;
    private boolean done, get_from_outer;
    private Stream right_db_stream;
    private BasicPattern left_side_data;
    private Quadruple right_side_db_data;
    private static HashMap<String, ArrayList<Hasher>> hashMap;
    private int fieldCountHash;
    private int fieldCountSM;
    private HashMap<Integer, Hasher> smHashMap;

    public BP_Triple_Join(int amt_of_mem, int num_left_nodes, BPIterator left_itr, int BPJoinNodePosition,
                          int JoinOnSubjectOrObject, String RightSubjectFilter, String RightPredicateFilter,
                          String RightObjectFilter, double RightConfidenceFilter, int[] LeftOutNodePosition,
                          int OutputRightSubject, int OutputRightObject) {
        this.amt_of_mem = amt_of_mem;
        this.left_itr = left_itr;
        this.num_left_nodes = num_left_nodes;
        this.BPJoinNodePosition = BPJoinNodePosition;
        this.joinOnSubjectOrObject = JoinOnSubjectOrObject;
        this.RightConfidenceFilter = RightConfidenceFilter;
        this.RightObjectFilter = RightObjectFilter;
        this.RightPredicateFilter = RightPredicateFilter;
        this.RightSubjectFilter = RightSubjectFilter;
        this.LeftOutNodePositions = LeftOutNodePosition;
        this.OutputRightObject = OutputRightObject;
        this.OutputRightSubject = OutputRightSubject;
        get_from_outer = true;
        done = false;
        right_db_stream = null;
        left_side_data = null;
        right_side_db_data = null;
    }


    public Heapfile executeHJoin(String fileName) {
        Stream hashStream = SystemDefs.JavabaseDB.openStreamForBasicPattern(SystemDefs.JavabaseDB.db_name(), RightSubjectFilter, RightPredicateFilter, RightObjectFilter, RightConfidenceFilter);
        try {
            figureHash(hashStream, joinOnSubjectOrObject);
            hashStream.closeStream();
            return createJoinHF(BPJoinNodePosition, fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void figureHash(Stream inner, int joinOnObject) throws Exception {
        Quadruple inner_tuple;
        QID tid = new QID();
        hashMap = new HashMap<>();

        Hasher value;
        while ((inner_tuple = inner.getNextQuadSansSort(tid)) != null) {

            double confidence = inner_tuple.getConfidence();
            LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();


            if (joinOnObject == 0) {
                //joining on subject

                EID eid_1 = inner_tuple.getSubjectID();
                String key = Entity_HF.getLabel(inner_tuple.getSubjectID().returnLID());
//                System.out.println("Map Key Val: " + key);
//                System.out.println("------------------------------------------------");

                EID eid_2 = inner_tuple.getObjectID();
                PID pid_i = inner_tuple.getPredicateID();
                value = new Hasher(eid_1, eid_2, pid_i, confidence);

                if (hashMap.containsKey(key)) {
                    ArrayList<Hasher> existingArrayList = hashMap.get(key);
                    existingArrayList.add(value);
                    hashMap.put(key, existingArrayList);
                } else {
                    ArrayList<Hasher> list = new ArrayList<>();
                    list.add(value);
                    hashMap.put(key, list);
                }
            } else {
                //joining on object

                EID eid_1 = inner_tuple.getObjectID();
                String key = Entity_HF.getLabel(inner_tuple.getObjectID().returnLID());
                EID eid_2 = inner_tuple.getSubjectID();
                PID pid_i = inner_tuple.getPredicateID();

                value = new Hasher(eid_1, eid_2, pid_i, confidence);

                if (hashMap.containsKey(key)) {
                    ArrayList<Hasher> existingArrayList = hashMap.get(key);
                    existingArrayList.add(value);
                    hashMap.put(key, existingArrayList);
                } else {
                    ArrayList<Hasher> list = new ArrayList<>();
                    list.add(value);
                    hashMap.put(key, list);
                }
            }
        }
    }

    public Heapfile createJoinHF(int position, String FileName) throws Exception {

        QID tid = new QID();
        BasicPattern leftSide;
        LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();
        int fldcnt = 0;

        Heapfile FIRST_JOIN_FILE = new Heapfile(FileName);

        while ((leftSide = left_itr.get_next()) != null) {

            EID eid_o = leftSide.getEIDFld(position);
            String left_join_val = Entity_HF.getLabel(eid_o.returnLID());
//
//            System.out.println("Left Join Val: " + left_join_val);
//            System.out.println("------------------------------------------------");

            if (hashMap.containsKey(left_join_val)) {

//                System.out.println("Match!!!!!");
                ArrayList<Hasher> mapValue = hashMap.get(left_join_val);

                for (int i = 0; i < mapValue.size(); i++) {

                    Hasher value = mapValue.get(i);
                    EID eid_i1 = value.getEnt_1();
                    EID eid_i2 = value.getEnt_2();
                    PID pid = value.getPredicate();
                    double conf = value.getConfidence();

                    String entity1 = Entity_HF.getLabel(eid_i1.returnLID());
                    String entity2 = Entity_HF.getLabel(eid_i2.returnLID());
                    String predicate = Predicate_HF.getLabel(pid.returnLID());
//                    System.out.println("MapValue: " + "entity1: " + entity1 + " entity2: " + entity2 +" predicate: " + predicate);
                    BasicPattern basicPattern;
                    if (joinOnSubjectOrObject == 0) {
                        //mapkeys are subject. so eid1 is subjectid
                        basicPattern = generateBasicPattern(leftSide, eid_i1, eid_i2, conf);
                    } else {
                        //mapkeys are object. so eid1 is objectid
                        basicPattern = generateBasicPattern(leftSide, eid_i2, eid_i1, conf);
                    }
                    fldcnt = basicPattern.noOfFlds();
                    FIRST_JOIN_FILE.insertRecord(basicPattern.getBasicPatternTuple().getTupleByteArray());
                }
            }
        }

        setFieldCountHash(fldcnt);
        return FIRST_JOIN_FILE;
    }

    public BasicPattern generateBasicPattern(BasicPattern outer_tuple, EID inner_tuple_sid, EID inner_tuple_oid, double confidence_val) throws InvalidBasicPatternSizeException, IOException, FieldNumberOutOfBoundException, BasicPatternFieldNumberOutOfBoundException {

        ArrayList<EID> arrEID = new ArrayList<EID>();

        double tempConf;
        if (confidence_val <= outer_tuple.getDoubleFld(outer_tuple.noOfFlds())) {
            tempConf = confidence_val;
        } else {
            tempConf = outer_tuple.getDoubleFld(outer_tuple.noOfFlds());
        }

        BasicPattern basicPattern = new BasicPattern();
        for (int i = 1; i < outer_tuple.noOfFlds(); i++) {
            for (int j = 0; j < LeftOutNodePositions.length; j++) {
                if (LeftOutNodePositions[j] == i) {
                    arrEID.add(outer_tuple.getEIDFld(i));
                    break;
                }
            }
        }
        if (joinOnSubjectOrObject == 0 && OutputRightSubject == 1) {
            boolean isNodeMatchFound = false;
            int k = 0;
            while (k < LeftOutNodePositions.length) {
                if (LeftOutNodePositions[k] == BPJoinNodePosition) {
                    isNodeMatchFound = true;
                    break;
                }
                k++;
            }
            if (!isNodeMatchFound) {
                arrEID.add(inner_tuple_sid);
            }
        } else if (OutputRightSubject == 1 && joinOnSubjectOrObject == 1) {
            arrEID.add(inner_tuple_sid);
        }
        if (OutputRightObject == 1 && joinOnSubjectOrObject == 1) {
            boolean isNodeMatchFound = false;
            int k = 0;
            while (k < LeftOutNodePositions.length) {
                if (LeftOutNodePositions[k] == BPJoinNodePosition) {
                    isNodeMatchFound = true;
                    break;
                }
                k++;
            }
            if (!isNodeMatchFound) {
                arrEID.add(inner_tuple_oid);
            }
        } else if (OutputRightObject == 1 && joinOnSubjectOrObject == 0) {
            arrEID.add(inner_tuple_oid);
        }
        if (arrEID.size() != 0) {
            basicPattern.setHdr((short) (arrEID.size() + 1));
            int k = 0;
            while (k < arrEID.size()) {
                basicPattern.setEIDForFld(k + 1, arrEID.get(k));
                k++;
            }
            basicPattern.setDoubleFld(k + 1, tempConf);
        }
        basicPattern.print();
        return basicPattern;
    }

    public int getFieldCountHash() {
        return fieldCountHash;
    }

    public void setFieldCountHash(int fieldCount) {
        this.fieldCountHash = fieldCount;
    }

    public int getFieldCountSM() {
        return fieldCountSM;
    }

    public void setFieldCountSM(int fieldCountSM) {
        this.fieldCountSM = fieldCountSM;
    }

    @Override
    public BasicPattern get_next() throws Exception {

        if (done)
            return null;
        do {
            // If get_from_outer is true, Get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file.
            // If a get_next on the outer returns DONE?, then the nested loops
            // join is done too.

            if (get_from_outer) {
                get_from_outer = false;
                if (right_db_stream != null) {
                    right_db_stream.closeStream();
                    right_db_stream = null;
                }

                try {
                    right_db_stream = SystemDefs.JavabaseDB.openStreamForBasicPattern(SystemDefs.JavabaseDB.db_name(),
                            RightSubjectFilter, RightPredicateFilter, RightObjectFilter,
                            RightConfidenceFilter);
                } catch (Exception e) {
                    throw new NestedLoopException(e, "openScan failed");
                }

                if ((left_side_data = left_itr.get_next()) == null) {
                    done = true;
                    if (right_db_stream != null) {
                        right_db_stream.closeStream();
                        right_db_stream = null;
                    }

                    return null;
                }
            }


            LabelHeapFile predHeapFile = SystemDefs.JavabaseDB.getPredicateHandle();
            LabelHeapFile entityHeapFile = SystemDefs.JavabaseDB.getEntityHandle();

            QID qid = new QID();
            while ((right_side_db_data = right_db_stream.getNextQuadSansSort(qid)) != null) {
                String subject_val = entityHeapFile.getLabel(right_side_db_data.getSubjectID().returnLID());
                String predicate_val = predHeapFile.getLabel(right_side_db_data.getPredicateID().returnLID());
                String object_val = entityHeapFile.getLabel(right_side_db_data.getObjectID().returnLID());
                double confidence_val = 0;
                confidence_val = right_side_db_data.getConfidence();
                boolean isFiltersEqual = true;
                if (RightSubjectFilter.compareToIgnoreCase("null") != 0) {
                    isFiltersEqual &= (RightSubjectFilter.compareTo(subject_val) == 0);
                }
                if (RightObjectFilter.compareToIgnoreCase("null") != 0) {
                    isFiltersEqual &= (RightObjectFilter.compareTo(object_val) == 0);
                }
                if (RightPredicateFilter.compareToIgnoreCase("null") != 0) {
                    isFiltersEqual &= (RightPredicateFilter.compareTo(predicate_val) == 0);
                }
                if (RightConfidenceFilter != 0) {
                    isFiltersEqual &= (RightConfidenceFilter <= confidence_val);
                }
                if (isFiltersEqual) {
                    EID leftEID = left_side_data.getEIDFld(BPJoinNodePosition);
                    EID rightEID;
                    ArrayList<EID> eidList = new ArrayList<EID>();
                    double tempConf = 0.0;
                    if (confidence_val <= left_side_data.getDoubleFld(left_side_data.noOfFlds())) {
                        tempConf = confidence_val;
                    } else {
                        tempConf = left_side_data.getDoubleFld(left_side_data.noOfFlds());
                    }
                    if (joinOnSubjectOrObject == 0) {
                        rightEID = right_side_db_data.getSubjectID();
                    } else {
                        rightEID = right_side_db_data.getObjectID();
                    }
                    if (leftEID.equals(rightEID)) {
                        BasicPattern basicPattern = new BasicPattern();
                        for (int i = 1; i < left_side_data.noOfFlds(); i++) {
                            for (int j = 0; j < LeftOutNodePositions.length; j++) {
                                if (LeftOutNodePositions[j] == i) {
                                    eidList.add(left_side_data.getEIDFld(i));
                                    break;
                                }
                            }
                        }
                        if (joinOnSubjectOrObject == 0 && OutputRightSubject == 1) {
                            boolean isNodeMatchFound = false;
                            int k = 0;
                            while (k < LeftOutNodePositions.length) {
                                if (LeftOutNodePositions[k] == BPJoinNodePosition) {
                                    isNodeMatchFound = true;
                                    break;
                                }
                                k++;
                            }
                            if (!isNodeMatchFound) {
                                eidList.add(right_side_db_data.getSubjectID());
                            }
                        } else if (OutputRightSubject == 1 && joinOnSubjectOrObject == 1) {
                            eidList.add(right_side_db_data.getSubjectID());
                        }
                        if (OutputRightObject == 1 && joinOnSubjectOrObject == 1) {
                            boolean isNodeMatchFound = false;
                            int k = 0;
                            while (k < LeftOutNodePositions.length) {
                                if (LeftOutNodePositions[k] == BPJoinNodePosition) {
                                    isNodeMatchFound = true;
                                    break;
                                }
                                k++;
                            }
                            if (!isNodeMatchFound) {
                                eidList.add(right_side_db_data.getObjectID());
                            }
                        } else if (OutputRightObject == 1 && joinOnSubjectOrObject == 0) {
                            eidList.add(right_side_db_data.getObjectID());
                        }
                        if (eidList.size() != 0) {
                            basicPattern.setHdr((short) (eidList.size() + 1));
                            int k = 0;
                            while (k < eidList.size()) {
                                basicPattern.setEIDForFld(k + 1, eidList.get(k));
                                k++;
                            }
                            basicPattern.setDoubleFld(k + 1, tempConf);
                            return basicPattern;
                        }

                    }
                }
            }

            // There has been no match. (otherwise, we would have
            // returned from t//he while loop. Hence, inner is
            // exhausted, => set get_from_outer = TRUE, go to top of loop
            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {

            try {
                if (right_db_stream != null) {
                    right_db_stream.closeStream();
                }
                left_itr.close();
            } catch (Exception e) {
                System.out.println("NestedLoopsJoin.java: error in closing iterator." + e);
            }
            closeFlag = true;
        }

    }

    static class Hasher {
        EID ent_1;
        EID ent_2;
        PID predicate;
        double confidence;

        LabelHeapFile Entity_HF = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile Predicate_HF = SystemDefs.JavabaseDB.getPredicateHandle();

        public Hasher(EID eid_1, EID eid_2, PID predicate, double confidence) {
            this.ent_1 = eid_1;
            this.ent_2 = eid_2;
            this.predicate = predicate;
            this.confidence = confidence;
        }

        public EID getEnt_1() {
            return ent_1;
        }

        public void setEnt_1(EID ent_1) {
            this.ent_1 = ent_1;
        }

        public PID getPredicate() {
            return predicate;
        }

        public void setPredicate(PID predicate) {
            this.predicate = predicate;
        }

        public double getConfidence() {
            return confidence;
        }

        public void setConfidence(double confidence) {
            this.confidence = confidence;
        }

        public EID getEnt_2() {
            return ent_2;
        }

        public void setEnt_2(EID ent_2) {
            this.ent_2 = ent_2;
        }

        @Override
        public String toString() {
            String entity1 = null;
            try {
                entity1 = Entity_HF.getLabel(getEnt_1().returnLID());
                String entity2 = Entity_HF.getLabel(getEnt_2().returnLID());
                String predicate = Predicate_HF.getLabel(getPredicate().returnLID());
                return ("MapValue: " + "entity1: " + entity1 + " entity2: " + entity2 + " predicate: " + predicate + " conf:" + getConfidence());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}
