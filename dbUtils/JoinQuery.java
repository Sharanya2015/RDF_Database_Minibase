package dbUtils;

import diskmgr.*;
import global.*;
import heap.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import basicpattern.*;
import bpiterator.*;
import index.IndexException;
import iterator.*;

public class JoinQuery {

    public static SystemDefs systemDefs = null;
    static String databaseName = null;

    static String subject = null;
    static String object = null;
    static String predicate = null;
    static String conf = null;
    static int firstLevelJoinCount = 0;
    static int secondLevelJoinCount = 0;

    static int indexOption = 1;
    public static double confidence = -1.0;
    public static int num_of_buf = 200;

    public static List<Integer> secondLONPList = new ArrayList<Integer>();
    public static List<Integer> firstLONPList = new ArrayList<Integer>();
    public static String queryFileName = null;
    public static int FJNP = 0, SJNP = 0, FJONO = 0, SJONO = 0, FORS = 0, SORS = 0, FORO = 0, SORO = 0;
    public static String FRSF = null, SRSF = null, FRPF = null, SRPF = null, FROF = null, SROF = null;
    public static double FRCF = -1.0, SRCF = -1.0;
    public static int[] FLONP = null, SLONP = null;


    public static int  nodePosition = 0,sortOrder = 0, numSortPages = 0;

    private static void hashBasedJoin() throws HFDiskMgrException, HFException, HFBufMgrException, IOException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidRelation, FileScanException, TupleUtilsException, SortException, IndexException, JoinsException, FileAlreadyDeletedException {
        Stream streamForBasicPattern = SystemDefs.JavabaseDB.openStreamForBasicPattern(databaseName, subject, predicate, object, confidence);

        System.out.println("\n==========================================================  HASH RECORDS THAT MATCH FILTER PATTERN =============================================================");
        Heapfile basicPatternHeapFile = new Heapfile("initialBasicPatternHeapFileHashed");
        BasicPattern initialBasicPattern;
        QID quadrupleId = null;
        while ((initialBasicPattern = streamForBasicPattern.getNextBasicPatternFromQuadruple(quadrupleId)) != null) {
            initialBasicPattern.print();
            basicPatternHeapFile.insertRecord(initialBasicPattern.getBasicPatternTuple().getTupleByteArray());
        }
        streamForBasicPattern.closeStream();
        System.out.println("\n============================================================================================================================================================");


        if (basicPatternHeapFile.getRecCnt() > 0) {

            int firstLevelPatternNodeCount;
            Heapfile firstLevelJoinHeapFile;
            FLONP = new int[firstLONPList.size()];
            for (int i = 0; i < firstLONPList.size(); i++) {
                FLONP[i] = firstLONPList.get(i);
            }


            System.out.println("\n================================================================  HASH FIRST LEVEL JOIN RESULTS =================================================================");
            BPFileScan initial_basic_pattern_scan = new BPFileScan("initialBasicPatternHeapFileHashed", 3);

            long startTime = System.currentTimeMillis();
            BP_Triple_Join firstLevelHashJoin = new BP_Triple_Join(num_of_buf, 3, initial_basic_pattern_scan, FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
            firstLevelJoinHeapFile = firstLevelHashJoin.executeHJoin("firstLevelJoinHeapFileHashed");
            firstLevelHashJoin.close();
            firstLevelPatternNodeCount = firstLevelHashJoin.getFieldCountHash();
            basicPatternHeapFile.deleteFile();
            System.out.println(firstLevelPatternNodeCount);
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken: " + (endTime - startTime) + " millis");
            System.out.println("============================================================================================================================================================");


            if (firstLevelPatternNodeCount > 0) {

                int secondLevelPatternNodeCount;
                Heapfile secondLevelJoinHeapFile;
                SLONP = new int[secondLONPList.size()];
                for (int i = 0; i < secondLONPList.size(); i++) {
                    SLONP[i] = secondLONPList.get(i);
                }


                System.out.println("\n================================================================  HASH SECOND LEVEL JOIN RESULTS ================================================================");
                BPFileScan scanFirstFile = new BPFileScan("firstLevelJoinHeapFileHashed", firstLevelPatternNodeCount);
                startTime = System.currentTimeMillis();
                BP_Triple_Join secondLevelHashJoin = new BP_Triple_Join(num_of_buf, firstLevelPatternNodeCount
                        , scanFirstFile, SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
                secondLevelJoinHeapFile = secondLevelHashJoin.executeHJoin("secondLevelJoinHeapFileHashed");
                secondLevelHashJoin.close();
                secondLevelPatternNodeCount = secondLevelHashJoin.getFieldCountHash();
                System.out.println(secondLevelPatternNodeCount);
                endTime = System.currentTimeMillis();
                System.out.println("Time taken: " + (endTime - startTime) + " millis");

                System.out.println("============================================================================================================================================================");
                firstLevelJoinHeapFile.deleteFile();

                if (secondLevelJoinHeapFile.getRecCnt() > 0) {
                    startTime = System.currentTimeMillis();
                    System.out.println("\n======================================================================  HASH SORTED RESULTS =====================================================================");
                    BPFileScan secondResultScan = null;
                    try {
                        secondResultScan = new BPFileScan("secondLevelJoinHeapFileHashed", secondLevelPatternNodeCount);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    BPSort sort = null;
                    BPOrder order = new BPOrder(sortOrder);
                    try {
                        sort = new BPSort(secondResultScan, order, nodePosition, numSortPages);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    try {
                        while ((initialBasicPattern = sort.get_next()) != null) {
                            initialBasicPattern.print();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("\n============================================================================================================================================================");
                    sort.close();
                    endTime = System.currentTimeMillis();
                    System.out.println("Time taken: " + (endTime - startTime) + " millis");
                } else {
                    System.out.println("Second Join Returned No Matches");
                    System.out.println("\n========================================================================================================");
                }
                secondLevelJoinHeapFile.deleteFile();
            } else {
                System.out.println("First Join Returned No Matches");
                System.out.println("\n========================================================================================================");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {

            databaseName = "/tmp/" + args[0];
            indexOption = Integer.parseInt(args[0].split("[_]")[1]);
            queryFileName = args[1];
            num_of_buf = Integer.parseInt(args[2]);


            queryParser();

            File databaseFile = new File(databaseName);
            if (databaseFile.exists()) {
                systemDefs = new SystemDefs(databaseName, 0, num_of_buf, "Clock", indexOption);
            } else {
                System.out.println("Database DOES NOT EXIST!");
                return;
            }

            Stream streamForBasicPattern = SystemDefs.JavabaseDB.openStreamForBasicPattern(databaseName, subject, predicate, object, confidence);
            System.out.println("\n==========================================================  BASIC RECORDS THAT MATCH FILTER PATTERN =============================================================");
            Heapfile basicPatternHeapFile = new Heapfile("initialBasicPatternHeapFile");
            BasicPattern initialBasicPattern;
            QID quadrupleId = null;
            while ((initialBasicPattern = streamForBasicPattern.getNextBasicPatternFromQuadruple(quadrupleId)) != null) {
                initialBasicPattern.print();
                basicPatternHeapFile.insertRecord(initialBasicPattern.getBasicPatternTuple().getTupleByteArray());
            }
            if(streamForBasicPattern!= null) {
                streamForBasicPattern.closeStream();
            }
            System.out.println("\n============================================================================================================================================================");


            if (basicPatternHeapFile.getRecCnt() > 0) {

                int firstLevelPatternNodeCount;
                Heapfile firstLevelJoinHeapFile;
                FLONP = new int[firstLONPList.size()];
                for (int i = 0; i < firstLONPList.size(); i++) {
                    FLONP[i] = firstLONPList.get(i);
                }


                System.out.println("\n================================================================  BASIC FIRST LEVEL JOIN RESULTS =================================================================");
                BPFileScan initial_basic_pattern_scan = new BPFileScan("initialBasicPatternHeapFile", 3);

                long startTime = System.currentTimeMillis();
                BP_Triple_Join firstLevelNestedJoin = new BP_Triple_Join(num_of_buf, 3, initial_basic_pattern_scan, FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
                firstLevelJoinHeapFile = new Heapfile("firstLevelJoinHeapFile");
                BasicPattern basicPattern = firstLevelNestedJoin.get_next();
                try {
                    firstLevelPatternNodeCount = basicPattern.noOfFlds();
                }
                catch (NullPointerException e){
                    System.out.println("First level Join returned no results ");

                    firstLevelNestedJoin.close();
                    basicPatternHeapFile.deleteFile();
                    initial_basic_pattern_scan.close();
                    firstLevelJoinHeapFile.deleteFile();
                    SystemDefs.close();
                    return;
                }
                while (basicPattern != null) {
                    basicPattern.print();
                    firstLevelJoinCount++;
                    firstLevelJoinHeapFile.insertRecord(basicPattern.getBasicPatternTuple().getTupleByteArray());
                    basicPattern = firstLevelNestedJoin.get_next();
                }
                firstLevelNestedJoin.close();
                basicPatternHeapFile.deleteFile();
                long endTime = System.currentTimeMillis();
                System.out.println("Time taken: " + (endTime - startTime) + " millis");
                System.out.println("============================================================================================================================================================");


                if (firstLevelPatternNodeCount > 0) {

                    int secondLevelPatternNodeCount;
                    Heapfile secondLevelJoinHeapFile;
                    SLONP = new int[secondLONPList.size()];
                    for (int i = 0; i < secondLONPList.size(); i++) {
                        SLONP[i] = secondLONPList.get(i);
                    }


                    System.out.println("\n================================================================  BASIC SECOND LEVEL JOIN RESULTS ================================================================");
                    BPFileScan scanFirstFile = new BPFileScan("firstLevelJoinHeapFile", firstLevelPatternNodeCount);

                    startTime = System.currentTimeMillis();
                    secondLevelJoinHeapFile = new Heapfile("secondLevelJoinHeapFile");
                    BP_Triple_Join secondLevelNestedJoin = new BP_Triple_Join(num_of_buf, firstLevelPatternNodeCount
                            , scanFirstFile, SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
                    BasicPattern basicPattern2 = secondLevelNestedJoin.get_next();
                    try {
                        secondLevelPatternNodeCount = basicPattern2.noOfFlds();
                    }catch (NullPointerException e){
                        System.out.println("Second level Join returned no results ");
                        secondLevelNestedJoin.close();
                        firstLevelJoinHeapFile.deleteFile();
                        secondLevelJoinHeapFile.deleteFile();
                        SystemDefs.close();
                        return;
                    }
                    while (basicPattern2 != null) {
                        basicPattern2.print();
                        secondLevelJoinCount++;
                        secondLevelJoinHeapFile.insertRecord(basicPattern2.getBasicPatternTuple().getTupleByteArray());
                        basicPattern2 = secondLevelNestedJoin.get_next();
                    }
                    secondLevelNestedJoin.close();
                    endTime = System.currentTimeMillis();
                    System.out.println("Time taken: " + (endTime - startTime) + " millis");


                    System.out.println("============================================================================================================================================================");
                    firstLevelJoinHeapFile.deleteFile();

                    if (secondLevelJoinHeapFile.getRecCnt() > 0) {
                        startTime = System.currentTimeMillis();
                        System.out.println("\n======================================================================  BASIC SORTED RESULTS =====================================================================");
                        BPFileScan secondResultScan = null;
                        try {
                            secondResultScan = new BPFileScan("secondLevelJoinHeapFile", secondLevelPatternNodeCount);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        BPSort sort = null;
                        BPOrder order = new BPOrder(sortOrder);
                        try {
                            sort = new BPSort(secondResultScan, order, nodePosition, numSortPages);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }


                        try {
                            while ((initialBasicPattern = sort.get_next()) != null) {
                                initialBasicPattern.print();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        System.out.println("\n============================================================================================================================================================");
                        sort.close();
                        endTime = System.currentTimeMillis();
                        System.out.println("Time taken: " + (endTime - startTime) + " millis");
                    } else {
                        System.out.println("Second Join Returned No Matches");
                        System.out.println("\n========================================================================================================");
                    }
                    secondLevelJoinHeapFile.deleteFile();
                } else {
                    System.out.println("First Join Returned No Matches");
                    System.out.println("\n========================================================================================================");
                }
            }

            System.out.println("NET PAGE WRITES: " + PCounter.wcounter);
            System.out.println("NET PAGE READS: " + PCounter.rcounter);
            PCounter.initialize();
            hashBasedJoin();
            System.out.println("NET PAGE WRITES: " + PCounter.wcounter);
            System.out.println("NET PAGE READS: " + PCounter.rcounter);
            PCounter.initialize();
            sortMergeJoinNew(databaseName, indexOption, subject, predicate, object, confidence);
            System.out.println("NET PAGE WRITES: " + PCounter.wcounter);
            System.out.println("NET PAGE READS: " + PCounter.rcounter);
        } else {
            System.out.println("Use correct syntax:query RDFDBNAME QUERYFILE NUMBUF");
            SystemDefs.close();
            return;
        }
        SystemDefs.close();
        System.out.println("============================================================================================================================================================");

    }


    private static void sortMergeJoinNew(String databaseName, int indexOption, String subject, String predicate,
                                      String object, double confidence) throws Exception {

        Stream streamForBasicPattern;
        BasicPattern initialBasicPattern;
        int orderType;
        Heapfile basicPatternHeapFile = new Heapfile("initialBasicPatternHeapFileSM");

        if (FJNP == 2) {
            System.out.println("Sorting on object");
            orderType = 5;
            streamForBasicPattern = SystemDefs.JavabaseDB.openStream(databaseName, indexOption, orderType, subject, predicate, object, confidence);
            System.out.println("\n==========================================================  SORT MERGE RECORDS THAT MATCH FILTER PATTERN =============================================================");
            QID quadrupleId = null;
            while ((initialBasicPattern = streamForBasicPattern.getNextBasicPatternFromQuadrupleSortedWay(quadrupleId)) != null) {
                initialBasicPattern.print();
                basicPatternHeapFile.insertRecord(initialBasicPattern.getBasicPatternTuple().getTupleByteArray());
            }
            streamForBasicPattern.closeStream();
            System.out.println("\n============================================================================================================================================================");
        } else {
            orderType = 3;
            System.out.println("Sorting on subject");
            streamForBasicPattern = SystemDefs.JavabaseDB.openStream(databaseName, indexOption, orderType, subject, predicate, object, confidence);
            System.out.println("\n==========================================================  SORT MERGE RECORDS THAT MATCH FILTER PATTERN =============================================================");
            QID quadrupleId = null;
            while ((initialBasicPattern = streamForBasicPattern.getNextBasicPatternFromQuadrupleSortedWay(quadrupleId)) != null) {
                initialBasicPattern.print();
                basicPatternHeapFile.insertRecord(initialBasicPattern.getBasicPatternTuple().getTupleByteArray());
            }
            streamForBasicPattern.closeStream();
            System.out.println("\n============================================================================================================================================================");
        }

        if (basicPatternHeapFile.getRecCnt() > 0) {

            int firstLevelPatternNodeCount=0;
            Heapfile firstLevelJoinHeapFile;
            FLONP = new int[firstLONPList.size()];
            for (int i = 0; i < firstLONPList.size(); i++) {
                FLONP[i] = firstLONPList.get(i);
            }

            System.out.println("\n================================================================  SORT MERGE FIRST LEVEL JOIN RESULTS =================================================================");
            BPFileScan initial_basic_pattern_scan = new BPFileScan("initialBasicPatternHeapFileSM", 3);

            long startTime = System.currentTimeMillis();
            BP_Triple_Join firstLevelNestedJoin = new BP_Triple_Join(num_of_buf, 3, initial_basic_pattern_scan, FJNP, FJONO, FRSF, FRPF, FROF, FRCF, FLONP, FORS, FORO);
            firstLevelJoinHeapFile = new Heapfile("firstLevelJoinHeapFileSM1");
            BasicPattern basicPattern = firstLevelNestedJoin.get_next();
            firstLevelPatternNodeCount = basicPattern.noOfFlds();
            firstLevelJoinCount=0;
            while (basicPattern != null) {
                basicPattern.print();
                firstLevelJoinCount++;
                firstLevelJoinHeapFile.insertRecord(basicPattern.getBasicPatternTuple().getTupleByteArray());
                basicPattern = firstLevelNestedJoin.get_next();
            }
            firstLevelNestedJoin.close();
            initial_basic_pattern_scan.close();
            basicPatternHeapFile.deleteFile();
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken: " + (endTime - startTime) + " millis");
            System.out.println("============================================================================================================================================================");

            if (firstLevelPatternNodeCount > 0) {

                int secondLevelPatternNodeCount;
                Heapfile secondLevelJoinHeapFile;
                SLONP = new int[secondLONPList.size()];
                for (int i = 0; i < secondLONPList.size(); i++) {
                    SLONP[i] = secondLONPList.get(i);
                }
//                startTime = System.currentTimeMillis();

                BPFileScan firstLevelSMResult = null;
                try {
                    firstLevelSMResult = new BPFileScan("firstLevelJoinHeapFileSM1", firstLevelPatternNodeCount);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                BPSort sort = null;
                BPOrder order = new BPOrder(1);
                try {
                    sort = new BPSort(firstLevelSMResult, order, SJNP, numSortPages);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.println("\n================================================================  SORT MERGE SECOND LEVEL JOIN RESULTS ================================================================");

                long startTime2 = System.currentTimeMillis();
                secondLevelJoinHeapFile = new Heapfile("secondLevelJoinHeapFileSM");
                BP_Triple_Join secondLevelNestedJoin = new BP_Triple_Join(num_of_buf, firstLevelPatternNodeCount,
                        sort, SJNP, SJONO, SRSF, SRPF, SROF, SRCF, SLONP, SORS, SORO);
                BasicPattern basicPattern2 = secondLevelNestedJoin.get_next();
                secondLevelPatternNodeCount = basicPattern2.noOfFlds();
                while (basicPattern2 != null) {
                    basicPattern2.print();
                    secondLevelJoinHeapFile.insertRecord(basicPattern2.getBasicPatternTuple().getTupleByteArray());
                    basicPattern2 = secondLevelNestedJoin.get_next();
                }
                secondLevelNestedJoin.close();
                firstLevelSMResult.close();
                long endTime2 = System.currentTimeMillis();
                System.out.println("Time taken: " + (endTime2 - startTime2) + " millis");
                System.out.println("============================================================================================================================================================");

                firstLevelJoinHeapFile.deleteFile();

                if (secondLevelJoinHeapFile.getRecCnt() > 0) {
                    startTime = System.currentTimeMillis();
                    System.out.println("\n======================================================================  SORT MERGE SORTED RESULTS =====================================================================");
                    BPFileScan secondResultScan = null;
                    try {
                        secondResultScan = new BPFileScan("secondLevelJoinHeapFileSM", secondLevelPatternNodeCount);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    BPSort sort1 = null;
                    BPOrder order1 = new BPOrder(sortOrder);
                    try {
                        sort1 = new BPSort(secondResultScan, order1, nodePosition, numSortPages);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    try {
                        while ((initialBasicPattern = sort1.get_next()) != null) {
                            initialBasicPattern.print();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("\n============================================================================================================================================================");
                    sort1.close();
                    endTime = System.currentTimeMillis();
                    System.out.println("Time taken: " + (endTime - startTime) + " millis");
                } else {
                    System.out.println("Second Join Returned No Matches");
                    System.out.println("\n========================================================================================================");
                }
                secondLevelJoinHeapFile.deleteFile();

                }  else {
                System.out.println("First Join Returned No Matches");
                System.out.println("\n========================================================================================================");


            }
        }else {
            System.out.println("Use correct syntax:query RDFDBNAME QUERYFILE NUMBUF");
            return;
        }
        }


    public static void queryParser() throws IOException {
        FileInputStream fstream = new FileInputStream(queryFileName);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String strLine;
        String[] lines = new String[8];
        int arrayPos = 0;
        while ((strLine = br.readLine()) != null) {
            lines[arrayPos++] = strLine;
        }
        int lineNumber = 0;
        for (String line : lines) {
            parseLines(line, lineNumber);
            lineNumber++;
        }
    }

    public static void parseLines(String line, int lineNumber) {
        if (lineNumber == 1) {
            getFilterParameters(line);
        }
        if (lineNumber == 2) {
            parseRightJoinParams(line, 1);
        }
        if (lineNumber == 4) {
            parseRightJoinParams(line, 2);
        }
        if (lineNumber == 6) {
            parseSortCriteria(line);
        }
    }

    public static void getFilterParameters(String line) {

        String delims = "[()\\[\\]]+";
        String[] segments = line.split(delims);

        String filterValues = segments[1];
        String[] soloFilters = filterValues.split(",");

        if (soloFilters[0].trim().compareTo("*") == 0) subject = "null";
        else subject = soloFilters[0].trim();
        if (soloFilters[1].trim().compareTo("*") == 0) predicate = "null";
        else predicate = soloFilters[1].trim();
        if (soloFilters[2].trim().compareTo("*") == 0) object = "null";
        else object = soloFilters[2].trim();
        if (soloFilters[3].trim().compareTo("*") == 0) conf = "null";
        else conf = soloFilters[3].trim();

        if (conf.compareToIgnoreCase("null") != 0) {
            confidence = Double.parseDouble(conf);
        }
    }

    public static void parseRightJoinParams(String line, int level) {
        String[] contents = line.trim().split(",");
        if (level == 1) {
            FJNP = Integer.parseInt(contents[0].trim());
            FJONO = Integer.parseInt(contents[1].trim());
            if (contents[2].trim().compareTo("*") == 0) FRSF = "null";
            else FRSF = contents[2].trim();
            if (contents[3].trim().compareTo("*") == 0) FRPF = "null";
            else FRPF = contents[3].trim();
            if (contents[4].trim().compareTo("*") == 0) FROF = "null";
            else FROF = contents[4].trim();


            if (contents[5].trim().compareTo("*") != 0) FRCF = Double.parseDouble(contents[5].trim());
            String[] LOP_VAL = contents[6].trim().split("&");
            for (String val : LOP_VAL) firstLONPList.add(Integer.parseInt(val));
            FORS = Integer.parseInt(contents[7].trim());
            FORO = Integer.parseInt(contents[8].trim());
        } else {
            SJNP = Integer.parseInt(contents[0].trim());
            SJONO = Integer.parseInt(contents[1].trim());

            if (contents[2].trim().compareTo("*") == 0) SRSF = "null";
            else SRSF = contents[2].trim();
            if (contents[3].trim().compareTo("*") == 0) SRPF = "null";
            else SRPF = contents[3].trim();
            if (contents[4].trim().compareTo("*") == 0) SROF = "null";
            else SROF = contents[4].trim();

            if (contents[5].trim().compareTo("*") != 0) SRCF = Double.parseDouble(contents[5]);
            String[] LOP_VAL = contents[6].trim().split("&");
            for (String val : LOP_VAL) secondLONPList.add(Integer.parseInt(val));
            SORS = Integer.parseInt(contents[7].trim());
            SORO = Integer.parseInt(contents[8].trim());
        }

    }

    public static void parseSortCriteria(String line) {
        String[] segments = line.trim().split(",");
        {
            sortOrder = Integer.parseInt(segments[0].trim());
            nodePosition = Integer.parseInt(segments[1].trim());
            numSortPages = Integer.parseInt(segments[2].trim());
        }
    }

}
