package diskmgr;

import iterator.QuadrupleSort;
import labelheap.*;
import btree.*;
import global.*;
import quadrupleheap.*;
import basicpattern.*;

import java.util.Date;

public class Stream {

    public static String dbName;
    public String subjectFilter;
    public String predicateFilter;
    public String objectFilter;
    double confidenceFilter;
    boolean nullSubject = false;
    boolean nullObject = false;
    boolean nullPredicate = false;
    boolean nullConfidence = false;

    public static SystemDefs systemDefs = null;
    public static int optionValue = 1;
    public static EID etObjectQID = new EID();
    public static EID etSubjectQID = new EID();
    public static EID etPredicateQID = new EID();

    public static QuadrupleHeapfile heapFileResults = null;
    public QuadrupleSort tSort = null;

    public TScan tScan = null;
    public int PAGE_COUNT_SORT = 200;
    public int SORT_Q_NUM_PAGES = 16;
    boolean fullScan = false;
    public boolean scanBT = false;
    public Quadruple scanBTQuad = null;

    public Stream(String rdfDbName, int indexType, int orderType,
                  String subjectFilter, String predicateFilter, String objectFilter, double confidenceFilter) throws Exception {

        optionValue = orderType;
        dbName = rdfDbName;

        if (subjectFilter.equals("*") || subjectFilter.equals("null"))
            nullSubject = true;
        if (predicateFilter.equals("*") || predicateFilter.equals("null"))
            nullPredicate = true;
        if (objectFilter.equals("*") || objectFilter.equals("null"))
            nullObject = true;
        if (confidenceFilter == -1.0)
            nullConfidence = true;

        String indexScheme = String.valueOf(indexType);

        if (!nullSubject && !nullPredicate && !nullObject && !nullConfidence) {
            scanBTree(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            scanBT = true;
        } else {
            if (Integer.parseInt(indexScheme) == 1 && !nullConfidence) {
                scanTreeOnConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 2 && !nullSubject && !nullConfidence) {
                scanBySubjectAndConfidence(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 3 && !nullObject && !nullConfidence) {
                scanByObjectAndConfidence(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 4 && !nullPredicate && !nullConfidence) {
                scanByPredicateAndConfidence(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 5 && !nullPredicate) {
                scanTreeOnPredicateIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else {
                fullScan = true;
                completeHeapFileScan(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }

            tScan = new TScan(heapFileResults);
            QuadrupleOrder quadrupleOrder = fetchSortOrder();
            try {
                tSort = new QuadrupleSort(tScan, quadrupleOrder, SORT_Q_NUM_PAGES);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Stream(String rdfdbname, String subjectFilter, String predicateFilter, String objectFilter,
                  double confidenceFilter)
            throws Exception {
        dbName = rdfdbname;

        if (subjectFilter.compareToIgnoreCase("null") == 0) {
            nullSubject = true;
        }
        if (predicateFilter.compareToIgnoreCase("null") == 0) {
            nullPredicate = true;
        }
        if (objectFilter.compareToIgnoreCase("null") == 0) {
            nullObject = true;
        }
        if (confidenceFilter == -99.0) {
            nullConfidence = true;
        }

        String indexScheme = rdfdbname.substring(rdfdbname.lastIndexOf('_') + 1);

        if (!nullSubject && !nullPredicate && !nullObject) {
            scanBTree(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            scanBT = true;
        } else {
            if (Integer.parseInt(indexScheme) == 1 && !nullConfidence) {
                scanTreeOnConfidenceIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 2 && !nullSubject && !nullConfidence) {
                scanBySubjectAndConfidence(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 3 && !nullObject && !nullConfidence) {
                scanByObjectAndConfidence(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 4 && !nullPredicate && !nullConfidence) {
                scanByPredicateAndConfidence(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else if (Integer.parseInt(indexScheme) == 5 && !nullPredicate) {
                scanTreeOnPredicateIndex(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            } else {
                fullScan = true;
                completeHeapFileScan(subjectFilter, predicateFilter, objectFilter, confidenceFilter);
            }

            tScan = new TScan(heapFileResults);
        }
    }

    public boolean scanBTree(String Subject, String Predicate, String Object, double Confidence)
            throws Exception {

        if (getEID(Subject) == null) {
            System.out.println("No quadruple with given subject found");
            return false;
        } else
            etSubjectQID = getEID(Subject).returnEID();

        if (getEID(Object) == null) {
            System.out.println("No quadruple with given object found");
            return false;
        } else
            etObjectQID = getEID(Object).returnEID();

        if (getPredicate(Predicate) == null) {
            System.out.println("No quadruple with given predicate found");
            return false;
        } else
            etPredicateQID = getPredicate(Predicate).returnEID();

        String key = getKey(etSubjectQID.slotNo, etSubjectQID.pageNo.pid, etPredicateQID.slotNo,
                etPredicateQID.pageNo.pid,
                etObjectQID.slotNo, etObjectQID.pageNo.pid);

        QuadrupleHeapfile quadrupleHeapfile = SystemDefs.JavabaseDB.getQuadHandle();
        QuadrupleBTreeFile quadrupleBTreeFile = SystemDefs.JavabaseDB.getQuadruple_BTree();
        KeyClass lowerScanLimit = new StringKey(key);
        KeyClass upperScanLimit = new StringKey(key);

        QuadrupleBTFileScan quadrupleBTFileScan = quadrupleBTreeFile.new_scan(lowerScanLimit, upperScanLimit);
        KeyDataEntry entry = quadrupleBTFileScan.get_next();
        if (entry != null) {
            if (key.compareTo(((StringKey) (entry.key)).getKey()) == 0) {
                QID qid = ((QuadrupleLeafData) (entry.data)).getData();
                Quadruple quadruple = quadrupleHeapfile.getQuadruple(qid);
                double confidenceThreshold = quadruple.getConfidence();
                if (confidenceThreshold >= Confidence) {
                    scanBTQuad = new Quadruple(quadruple);
                }
            }
        }
        quadrupleBTFileScan.DestroyBTreeFileScan();
        quadrupleBTreeFile.close();
        return true;
    }

    private void scanTreeOnConfidenceIndex(String subjectFilter, String predicateFilter, String objectFilter,
                                           double confidenceFilter)
            throws Exception {

        boolean value;
        KeyDataEntry entry;
        QID quadrupleId;
        String subject, object, predicate;
        Quadruple record;

        QuadrupleBTreeFile quadrupleBTreeIndex = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile quadrupleHeapfile = SystemDefs.JavabaseDB.getQuadHandle();
        LabelHeapFile labelEntityHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile labelPredicateHeapFile = SystemDefs.JavabaseDB.getPredicateHandle();

        Date date = new Date();
        heapFileResults = new QuadrupleHeapfile(Long.toString(date.getTime()));

        KeyClass lowerScanLimit = new StringKey(Double.toString(confidenceFilter));
        QuadrupleBTFileScan quadrupleBTFileScan = quadrupleBTreeIndex.new_scan(lowerScanLimit, null);

        while ((entry = quadrupleBTFileScan.get_next()) != null) {
            value = true;
            quadrupleId = ((QuadrupleLeafData) entry.data).getData();
            record = quadrupleHeapfile.getQuadruple(quadrupleId);
            subject = labelEntityHeapFile.getLabel(record.getSubjectID().returnLID());
            object = labelEntityHeapFile.getLabel(record.getObjectID().returnLID());
            predicate = labelPredicateHeapFile.getLabel(record.getPredicateID().returnLID());
            if (!nullSubject)
                value = value & (subjectFilter.compareTo(subject) == 0);
            if (!nullObject)
                value = value & (objectFilter.compareTo(object) == 0);
            if (!nullPredicate)
                value = value & (predicateFilter.compareTo(predicate) == 0);
            if (!nullConfidence)
                value = value & (record.getConfidence() >= confidenceFilter);
            if (value)
                heapFileResults.insertQuadruple(record.returnQuadrupleByteArray());

        }

        quadrupleBTFileScan.DestroyBTreeFileScan();
        quadrupleBTreeIndex.close();
    }

    private void scanBySubjectAndConfidence(String subjectFilter, String predicateFilter, String objectFilter,
                                            double confidenceFilter) {
        try {

            boolean value;
            KeyDataEntry entry;
            QID quadrupleId;
            String subject, object, predicate;
            EID subjectId, objectId;
            PID predicateId;
            Quadruple quadruple;
            double confidence;

            QuadrupleBTreeFile quadrupleBTreeIndex = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
            QuadrupleHeapfile quadrupleHeapfile = SystemDefs.JavabaseDB.getQuadHandle();
            LabelHeapFile labelEntityHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile labelPredicateHeapFile = SystemDefs.JavabaseDB.getPredicateHandle();

            Date date = new Date();
            heapFileResults = new QuadrupleHeapfile(Long.toString(date.getTime()));

            KeyClass lowerScanLimit = new StringKey(subjectFilter + ":" + confidenceFilter);
            QuadrupleBTFileScan quadrupleBTFileScan = quadrupleBTreeIndex.new_scan(lowerScanLimit, null);

            while ((entry = quadrupleBTFileScan.get_next()) != null) {
                quadrupleId = ((QuadrupleLeafData) entry.data).getData();
                quadruple = quadrupleHeapfile.getQuadruple(quadrupleId);
                confidence = quadruple.getConfidence();
                subjectId = quadruple.getSubjectID();
                subject = labelEntityHeapFile.getLabel(subjectId.returnLID());
                objectId = quadruple.getObjectID();
                object = labelEntityHeapFile.getLabel(objectId.returnLID());
                predicateId = quadruple.getPredicateID();
                predicate = labelPredicateHeapFile.getLabel(predicateId.returnLID());
                value = true;

                if (!nullSubject)
                    value = value & (subjectFilter.compareTo(subject) == 0);
                if (!nullObject)
                    value = value & (objectFilter.compareTo(object) == 0);
                if (!nullPredicate)
                    value = value & (predicateFilter.compareTo(predicate) == 0);
                if (!nullConfidence)
                    value = value & (confidence >= confidenceFilter);

                if (subjectFilter.compareTo(subject) != 0) {
                    break;
                } else if (value) {
                    heapFileResults.insertQuadruple(quadruple.returnQuadrupleByteArray());
                }
            }
            quadrupleBTFileScan.DestroyBTreeFileScan();
            quadrupleBTreeIndex.close();
        } catch (Exception e) {
            System.err.println("Exception occurred while subject and confidence based index query." + e);
            Runtime.getRuntime().exit(1);
        }

    }

    private void scanByObjectAndConfidence(String subjectFilter, String predicateFilter, String objectFilter,
                                           double confidenceFilter) {
        try {

            boolean value;
            KeyDataEntry entry;
            QID quadrupleId;
            String subject, object, predicate;
            EID subjectId, objectId;
            PID predicateId;
            Quadruple quadruple;
            double confidence;

            QuadrupleBTreeFile quadrupleBTreeIndex = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
            QuadrupleHeapfile quadHandle = SystemDefs.JavabaseDB.getQuadHandle();
            LabelHeapFile labelEntityHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile labelPredicateHeapFile = SystemDefs.JavabaseDB.getPredicateHandle();

            Date date = new Date();
            heapFileResults = new QuadrupleHeapfile(Long.toString(date.getTime()));

            KeyClass lowerScanLimit = new StringKey(objectFilter + ":" + confidenceFilter);
            QuadrupleBTFileScan quadrupleBTFileScan = quadrupleBTreeIndex.new_scan(lowerScanLimit, null);

            while ((entry = quadrupleBTFileScan.get_next()) != null) {

                quadrupleId = ((QuadrupleLeafData) entry.data).getData();
                quadruple = quadHandle.getQuadruple(quadrupleId);
                confidence = quadruple.getConfidence();
                subjectId = quadruple.getSubjectID();
                subject = labelEntityHeapFile.getLabel(subjectId.returnLID());
                objectId = quadruple.getObjectID();
                object = labelEntityHeapFile.getLabel(objectId.returnLID());
                predicateId = quadruple.getPredicateID();
                predicate = labelPredicateHeapFile.getLabel(predicateId.returnLID());

                value = true;

                if (!nullSubject)
                    value = value & (subjectFilter.compareTo(subject) == 0);
                if (!nullObject)
                    value = value & (objectFilter.compareTo(object) == 0);
                if (!nullPredicate)
                    value = value & (predicateFilter.compareTo(predicate) == 0);
                if (!nullConfidence)
                    value = value & (confidence >= confidenceFilter);

                if (objectFilter.compareTo(object) != 0) {
                    break;
                } else if (value) {
                    heapFileResults.insertQuadruple(quadruple.returnQuadrupleByteArray());
                }
            }
            quadrupleBTFileScan.DestroyBTreeFileScan();
            quadrupleBTreeIndex.close();

        } catch (Exception e) {
            System.err.println("Exception occurred while object and confidence based index query." + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

    private void scanByPredicateAndConfidence(String subjectFilter, String predicateFilter, String objectFilter,
                                              double confidenceFilter) {
        try {

            boolean value;
            Quadruple quadruple;
            KeyDataEntry entry;
            String subject, object, predicate;
            EID subjectId, objectId;
            PID predicateId;
            QID quadrupleId;
            double confidence;

            QuadrupleBTreeFile quadrupleBTreeIndex = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
            QuadrupleHeapfile quadHandle = SystemDefs.JavabaseDB.getQuadHandle();
            LabelHeapFile labelEntityHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile labelPredicateHeapFile = SystemDefs.JavabaseDB.getPredicateHandle();

            Date date = new Date();
            heapFileResults = new QuadrupleHeapfile(Long.toString(date.getTime()));

            KeyClass lowerScanLimit = new StringKey(predicateFilter + ":" + confidenceFilter);
            QuadrupleBTFileScan quadrupleBTFileScan = quadrupleBTreeIndex.new_scan(lowerScanLimit, null);

            while ((entry = quadrupleBTFileScan.get_next()) != null) {
                quadrupleId = ((QuadrupleLeafData) entry.data).getData();
                quadruple = quadHandle.getQuadruple(quadrupleId);
                confidence = quadruple.getConfidence();
                subjectId = quadruple.getSubjectID();
                subject = labelEntityHeapFile.getLabel(subjectId.returnLID());
                objectId = quadruple.getObjectID();
                object = labelEntityHeapFile.getLabel(objectId.returnLID());
                predicateId = quadruple.getPredicateID();
                predicate = labelPredicateHeapFile.getLabel(predicateId.returnLID());

                value = true;

                if (!nullSubject)
                    value = value & (subjectFilter.compareTo(subject) == 0);
                if (!nullObject)
                    value = value & (objectFilter.compareTo(object) == 0);
                if (!nullPredicate)
                    value = value & (predicateFilter.compareTo(predicate) == 0);
                if (!nullConfidence)
                    value = value & (confidence >= confidenceFilter);
                if (predicateFilter.compareTo(predicate) != 0) {
                    break;
                } else if (value) {
                    heapFileResults.insertQuadruple(quadruple.returnQuadrupleByteArray());
                }
            }
            quadrupleBTFileScan.DestroyBTreeFileScan();
            quadrupleBTreeIndex.close();

        } catch (Exception e) {
            System.err.println("Exception occurred while predicate and confidence based index query." + e);
            Runtime.getRuntime().exit(1);
        }
    }

    public void scanTreeOnPredicateIndex(String subjectFilter, String predicateFilter, String objectFilter,
                                                double confidenceFilter)
            throws Exception {

        KeyDataEntry entry;
        Quadruple quadruple;
        String subject, object, predicate = null;
        QID quadrupleId;

        QuadrupleBTreeFile quadrupleBTreeIndex = SystemDefs.JavabaseDB.getQuadruple_BTreeIndex();
        QuadrupleHeapfile quadHandle = SystemDefs.JavabaseDB.getQuadHandle();
        LabelHeapFile labelEntityHeapFile = SystemDefs.JavabaseDB.getEntityHandle();
        LabelHeapFile predicateEntityHeapFile = systemDefs.JavabaseDB.getPredicateHandle();

        Date date = new Date();
        heapFileResults = new QuadrupleHeapfile(Long.toString(date.getTime()));

        KeyClass lowerScanLimit = new StringKey(predicateFilter);
        KeyClass upperScanLimit = new StringKey(predicateFilter);

        QuadrupleBTFileScan scan = quadrupleBTreeIndex.new_scan(lowerScanLimit, upperScanLimit);
        entry = scan.get_next();

        try {
            while (entry != null) {
                boolean value = true;
                quadrupleId = ((QuadrupleLeafData) entry.data).getData();
                quadruple = quadHandle.getQuadruple(quadrupleId);

                subject = labelEntityHeapFile.getLabel(quadruple.getSubjectID().returnLID());
                object = labelEntityHeapFile.getLabel(quadruple.getObjectID().returnLID());
                predicate = predicateEntityHeapFile.getLabel(quadruple.getPredicateID().returnLID());

                if (!nullSubject)
                    value = value & (subject.compareTo(subjectFilter) == 0);
                if (!nullObject)
                    value = value & (object.compareTo(objectFilter) == 0);
                if (confidenceFilter <= quadruple.getConfidence())
                    value = true & value;
                else value = false;
                if (value)
                    heapFileResults.insertQuadruple(quadruple.returnQuadrupleByteArray());
                entry = scan.get_next();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        scan.DestroyBTreeFileScan();
        quadrupleBTreeIndex.close();
    }

    private void completeHeapFileScan(String subFilter, String predFilter, String objFilter, double confFilter) {
        try {
            this.subjectFilter = subFilter;
            this.predicateFilter = predFilter;
            this.objectFilter = objFilter;
            this.confidenceFilter = confFilter;
            heapFileResults = SystemDefs.JavabaseDB.getQuadHandle();
        } catch (Exception e) {
            System.err.println("Exception occurred while scanning the entire heap file." + e);
            Runtime.getRuntime().exit(1);
        }
    }

    public QuadrupleOrder fetchSortOrder() {
        QuadrupleOrder sort_order = null;

        switch (optionValue) {
            case 1:
                sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectPredicateObjectConfidence);
                break;

            case 2:
                sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateSubjectObjectConfidence);
                break;

            case 3:
                sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
                break;

            case 4:
                sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateConfidence);
                break;

            case 5:
                sort_order = new QuadrupleOrder(QuadrupleOrder.ObjectConfidence);
                break;

            case 6:
                sort_order = new QuadrupleOrder(QuadrupleOrder.Confidence);
                break;
        }
        return sort_order;
    }

    public Quadruple getNext(QID qid) throws Exception {
        try {
            Quadruple quadruple = null;
            if (scanBT) {
                if (scanBTQuad != null) {
                    Quadruple quad = new Quadruple(scanBTQuad);
                    scanBTQuad = null;
                    return quad;
                }
            } else {
                while ((quadruple = tSort.get_next()) != null) {
                    if (!fullScan)
                        return quadruple;
                    else {
                        boolean value = true;
                        double confidence = quadruple.getConfidence();
                        String subject = SystemDefs.JavabaseDB.getEntityHandle()
                                .getLabel(quadruple.getSubjectID().returnLID());
                        String object = SystemDefs.JavabaseDB.getEntityHandle()
                                .getLabel(quadruple.getObjectID().returnLID());
                        String predicate = SystemDefs.JavabaseDB.getPredicateHandle()
                                .getLabel(quadruple.getPredicateID().returnLID());
                        if (!nullSubject)
                            value = value & (subjectFilter.compareTo(subject) == 0);
                        if (!nullObject)
                            value = value & (objectFilter.compareTo(object) == 0);
                        if (!nullPredicate)
                            value = value & (predicateFilter.compareTo(predicate) == 0);
                        if (!nullConfidence)
                            value = value & (confidence >= confidenceFilter);
                        if (value)
                            return quadruple;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred while getting the next quadruple!" + e);
        }
        return null;
    }

    public Quadruple getNextQuadSansSort(QID qqid) throws Exception {
        try {
            QID qid = new QID();
            Quadruple quadruple = null;
            if (scanBT) {
                if (scanBTQuad != null) {
                    Quadruple temp = new Quadruple(scanBTQuad);
                    scanBTQuad = null;
                    return temp;
                }
            } else {
                while ((quadruple = tScan.getNext(qid)) != null) {
                    if (fullScan == false) {
                        return quadruple;
                    } else {
                        boolean result = true;
                        double confidence = quadruple.getConfidence();
                        String subject = SystemDefs.JavabaseDB.getEntityHandle()
                                .getLabel(quadruple.getSubjectID().returnLID());
                        String object = SystemDefs.JavabaseDB.getEntityHandle()
                                .getLabel(quadruple.getObjectID().returnLID());
                        String predicate = SystemDefs.JavabaseDB.getPredicateHandle()
                                .getLabel(quadruple.getPredicateID().returnLID());

                        if (!nullSubject) {
                            result = result & (subjectFilter.compareTo(subject) == 0);
                        }
                        if (!nullObject) {
                            result = result & (objectFilter.compareTo(object) == 0);
                        }
                        if (!nullPredicate) {
                            result = result & (predicateFilter.compareTo(predicate) == 0);
                        }
                        if (!nullConfidence) {
                            result = result & (confidence >= confidenceFilter);
                        }
                        if (result) {
                            return quadruple;
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Exception occurred while getting the next quadruple!" + e);
        }
        return null;
    }

    public BasicPattern getNextBasicPatternFromQuadruple(QID qid) {
        try {
            Quadruple quad;
            while ((quad = getNextQuadSansSort(qid)) != null) {
                BasicPattern bp = new BasicPattern();
                bp.setHdr((short) 3);
                bp.setEIDForFld(1, quad.getSubjectID());
                bp.setEIDForFld(2, quad.getObjectID());
                bp.setDoubleFld(3, quad.getConfidence());
                return bp;
            }
        } catch (Exception e) {
            System.out.println("Error in Stream get next basic pattern\n" + e);
        }
        return null;
    }

    public BasicPattern getNextBasicPatternFromQuadrupleSortedWay(QID qid) {
        try {
            Quadruple quad;
            while ((quad = getNext(qid)) != null) {
                BasicPattern bp = new BasicPattern();
                bp.setHdr((short) 3);
                bp.setEIDForFld(1, quad.getSubjectID());
                bp.setEIDForFld(2, quad.getObjectID());
                bp.setDoubleFld(3, quad.getConfidence());
                return bp;
            }
        } catch (Exception e) {
            System.out.println("Error in Stream get next basic pattern\n" + e);
        }
        return null;
    }

    public void closeStream() {
        try {
            if (tScan != null)
                tScan.closescan();
            if (heapFileResults != null && heapFileResults != SystemDefs.JavabaseDB.getQuadHandle())
                heapFileResults.deleteFile();
            if (tSort != null)
                tSort.close();
        } catch (Exception e) {
            System.out.println("Exception occurred while closing stream!" + e);
        }
    }

    private String getKey(int subSlotNo, int subPagePid, int predicateSlotNo, int predicatePagePid, int objSlotNo,
                          int objPagePid) {
        return subSlotNo + ":" + subPagePid + ":" + predicateSlotNo + ":" + predicatePagePid + ":" + objSlotNo + ":"
                + objPagePid;
    }

    public static LID getEID(String filter) throws GetFileEntryException, PinPageException, ConstructPageException {

        LabelBTreeFile bTreeFile = new LabelBTreeFile(dbName + "/entityBT");
        KeyClass lowerScanLimit = new StringKey(filter);
        KeyClass upperScanLimit = new StringKey(filter);
        KeyDataEntry entry;
        LID eId = null;

        try {
            LabelBTFileScan scanValues = bTreeFile.new_scan(lowerScanLimit, upperScanLimit);
            entry = scanValues.get_next();
            if (entry == null)
                System.err.println("No quadruple matches the given filters!");
            else {
                eId = ((LabelLeafData) entry.data).getData();
                scanValues.DestroyBTreeFileScan();
            }
            bTreeFile.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return eId;
    }

    public static LID getPredicate(String predicate) {
        LID predicateId = null;
        LabelBTreeFile bTreeFile;
        try {
            KeyDataEntry entry;
            bTreeFile = new LabelBTreeFile(dbName + "/predicateBT");
            KeyClass lowerScanLimit = new StringKey(predicate);
            KeyClass upperScanLimit = new StringKey(predicate);
            LabelBTFileScan scanOutput = bTreeFile.new_scan(lowerScanLimit, upperScanLimit);
            entry = scanOutput.get_next();
            if (entry == null)
                System.err.println("No quadruple matches the given predicate!");
            else {
                predicateId = ((LabelLeafData) entry.data).getData();
            }
            scanOutput.DestroyBTreeFileScan();
            bTreeFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return predicateId;
    }
}