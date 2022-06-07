package dbUtils;

import diskmgr.PCounter;
import global.*;
import iterator.QuadrupleSort;
import labelheap.*;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;
import quadrupleheap.TScan;

import java.io.*;

public class BatchInsert {

    public static SystemDefs systemDefs = null;
    public static boolean dbExists = false;
    private static String TMP = "/tmp/";


    public static void main(String[] args) {


        String fileName, databaseName; int indexOption;

        if (args.length == 3) {
            fileName = args[0];
            indexOption = Integer.parseInt(args[1]);
            databaseName = TMP  + args[2] + "_" + indexOption;

            if (indexOption < 1 || indexOption > 5) {
                System.out.println("Invalid Index Option: " + indexOption + " specified!");
                return;
            }

            File file = new File(fileName);
            if (!(file.exists())) {
                System.out.println("The specified datafile: " + fileName + " does not exist!");
                return;
            }
        } else {
            System.out.println("Please use correct parameters: query DATAFILE INDEXOPTION RDFDBNAME");
            return;
        }

        Quadruple quad;
        QID qid = null;
        PID pid = null;
        EID sid = null, oid = null;

        File dbfl = new File(databaseName);
        if (dbfl.exists()) {
            systemDefs = new SystemDefs(databaseName, 0, 1000, "Clock", indexOption);
            dbExists = true;
        } else {
            systemDefs = new SystemDefs(databaseName, 10000, 1000, "Clock", indexOption);
        }

        try {
            FileInputStream fileInputStream = new FileInputStream(fileName);
            DataInputStream dataInputStream = new DataInputStream(fileInputStream);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataInputStream));
            String strLine; int i;
            double confidence = 0.0;
            while ((strLine = bufferedReader.readLine()) != null) {

                String[] str = strLine.split("\\s+");
                int colonCount = 0;
                for (String unit : str) {
                    if (unit.startsWith(":")) colonCount++;
                }
                if (colonCount != 3) {
                    continue;
                }

                i = 0;
                String[] strings = strLine.split("[\\s\\t:]");
                for (String tag : strings) {
                    if (tag.trim().length() > 0) {
                        try {
                            switch (i) {
                                case 1:
                                    sid = systemDefs.JavabaseDB.insertEntity(tag);
                                    break;
                                case 3:
                                    pid = systemDefs.JavabaseDB.insertPredicate(tag);
                                    break;
                                case 5:
                                    oid = systemDefs.JavabaseDB.insertEntity(tag);
                                    break;
                                case 7:
                                    confidence = Double.parseDouble(tag);
                                    break;
                            }
                        } catch (Exception e) {
                            System.out.println("Exception Occurred While Inserting Record:+" + tag);
                        }
                    }
                    i++;
                }
                quad = new Quadruple();
                quad.setSubjectID(sid);
                quad.setPredicateID(pid);
                quad.setObjectID(oid);
                quad.setConfidence(confidence);

                try {
                    qid = insertQuadruple(quad.getQuadrupleByteArray());
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            dataInputStream.close();

            if (dbExists) {
                try {
                    TScan am = new TScan(systemDefs.JavabaseDB.getQuadHandle());
                    QID q1 = new QID();
                    Quadruple q2 = null;
                    while ((q2 = am.getNext(q1)) != null) {
                        insertQuadruple(q2.getQuadrupleByteArray());
                        systemDefs.JavabaseDB.deleteQuadruple(q2.getQuadrupleByteArray());
                    }
                } catch (Exception e) {
                    manageIntermediateHeapFile(indexOption);
                }
            } else {
                manageIntermediateHeapFile(indexOption);
            }
        } catch (Exception e) {
            System.out.println("Exception Occurred While BatchInsert\n" + e);
        }
        System.out.println("======================================");
        System.out.println(" INDEX SCHEMES");
        System.out.println(" 1: Indexing on Confidence");
        System.out.println(" 2: Indexing on Subject and Confidence");
        System.out.println(" 3: Indexing on Object and Confidence");
        System.out.println(" 4. Indexing on Predicate and Confidence");
        System.out.println(" 5. Indexing on Predicate");
        System.out.println("-------------------------------------");
        System.out.println(" Selected Option: " + indexOption);
        System.out.println("-------------------------------------");
        systemDefs.JavabaseDB.createIndex(indexOption);
        generateDatabaseDetails();
        systemDefs.close();
        System.out.println(" TOTAL PAGE WRITES:" + PCounter.wcounter);
        System.out.println(" TOTAL PAGE READS:" + PCounter.rcounter);
        System.out.println(" BatchInsert Completed Successfully!!");
        return;
    }

    public static QID insertQuadruple(byte[] byteArray) throws Exception {
        QID quadrupleId = null;
        try {
            QuadrupleHeapfile quadrupleHeapfile = systemDefs.JavabaseDB.getTEMP_Quadruple_HF();
            quadrupleId = quadrupleHeapfile.insertQuadruple(byteArray);
        } catch (Exception e) {
            System.err.println("Exception Occurred While Inserting Quadruple" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        return quadrupleId;
    }

    private static void manageIntermediateHeapFile(int indexOption) {
        try {
            TScan tScan = new TScan(systemDefs.JavabaseDB.getTEMP_Quadruple_HF());
            QuadrupleOrder sort_order = null;
            switch (indexOption) {
                case 1:
                    sort_order = new QuadrupleOrder(QuadrupleOrder.Confidence);
                    break;
                case 2:
                    sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectConfidence);
                    break;
                case 3:
                    sort_order = new QuadrupleOrder(QuadrupleOrder.ObjectConfidence);
                    break;
                case 4:
                    sort_order = new QuadrupleOrder(QuadrupleOrder.PredicateConfidence);
                    break;
                case 5:
                    sort_order = new QuadrupleOrder(QuadrupleOrder.SubjectPredicateObjectConfidence);
                    break;
                default:
                    System.out.println(" Invalid IndexOption: " + indexOption + " specified!!");
            }

            QuadrupleSort quadrupleSort = new QuadrupleSort(tScan, sort_order, 200);
            Quadruple quadruple = null;
            while ((quadruple = quadrupleSort.get_next()) != null) {
//                print_quadruple(quadruple);
                systemDefs.JavabaseDB.insertQuadruple(quadruple.getQuadrupleByteArray());
//                System.out.println("*****************************");
            }
            quadrupleSort.close();
        } catch (Exception e) {
            System.out.println("Exception occurred while sorting with Index Option: " + indexOption);
        }
    }

    public static void generateDatabaseDetails() {
        System.out.println(" Total Predicate Count: " + systemDefs.JavabaseDB.getPredicateCnt());
        System.out.println(" Total Quadruple Count: " + systemDefs.JavabaseDB.getQuadrupleCnt());
        System.out.println(" Total Subject Count: " + systemDefs.JavabaseDB.getSubjectCnt());
        System.out.println(" Total Object Count: " + systemDefs.JavabaseDB.getObjectCnt());
        System.out.println(" Total Entity Count: " + systemDefs.JavabaseDB.getEntityCnt());
    }

    private static void displayQuadruple(Quadruple quadruple) throws Exception {
        LabelHeapFile labelHeapFileSubject = systemDefs.JavabaseDB.getEntityHandle();
        String subject = labelHeapFileSubject.getLabel(quadruple.getSubjectID().returnLID());
        LabelHeapFile labelHeapFilePredicate = systemDefs.JavabaseDB.getPredicateHandle();
        String predicate = labelHeapFilePredicate.getLabel(quadruple.getPredicateID().returnLID());
        LabelHeapFile labelHeapFileObject = systemDefs.JavabaseDB.getEntityHandle();
        String object = labelHeapFileObject.getLabel(quadruple.getObjectID().returnLID());
        System.out.println(subject + ":" + predicate + ":" + object + "(" + quadruple.getConfidence() + ")");
    }
}
