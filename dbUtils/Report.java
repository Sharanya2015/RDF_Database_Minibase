package dbUtils;

import diskmgr.PCounter;
import global.*;
import labelheap.LabelHeapFile;
import quadrupleheap.Quadruple;
import quadrupleheap.QuadrupleHeapfile;

import java.io.File;

// The class to print the database statistics
public class Report {
    public static void main(String[] args)
    {
        String dbName = null;   //Database name
        int indexOption = 0;    //Index option

        if(args.length == 2 )   //Check if the args are RDFDBNAME INDEXOPTION
        {
            indexOption = Integer.parseInt(args[1]);
            dbName = new String("/tmp/"+args[0]+"_"+indexOption);

            if(indexOption>5 || indexOption<0)
            {
                System.out.println("*** Indexoption only allowed within range: 1 to 5 ***");
                return;
            }
        }
        else
        {
            System.out.println("*** Usage: RDFDBNAME INDEXOPTION***");
            return;
        }


        EID sid = null, oid = null;
        PID pid = null;
        Quadruple q = null;
        QID qid = null;
        SystemDefs sysdef = null;
        int counter = 0;

        File dbfile = new File(dbName); //Check if database already exsist
        if(dbfile.exists())
        {
            //Database already present just open it
            sysdef = new SystemDefs(dbName,0,500,"Clock",indexOption);
            System.out.println("* Opening existing database *");
        }
        else
        {
            System.out.println("*** " + dbName + " Does Not Exist ***");
            return;
        }

        try
        {
            QuadrupleHeapfile qhf = SystemDefs.JavabaseDB.getQuadHandle();
            LabelHeapFile elhf = SystemDefs.JavabaseDB.getEntityHandle();
            LabelHeapFile plhf = SystemDefs.JavabaseDB.getPredicateHandle();
            System.out.println("Report Statistics");
            System.out.println(" RDF DB Name: " + dbName);
            System.out.println(" Database Size: " + dbfile.length() + " bytes");
            System.out.println(" Quadruple Size: " + GlobalConst.RDF_QUADRUPLE_SIZE + " bytes");
            System.out.println(" Page Size: " + SystemDefs.JavabaseDB.db_page_size() + " bytes");
            System.out.println(" Page Replacement Policy: Clock");
            System.out.println(" Quadruple File Record Count: " + qhf.getQuadCnt());
            System.out.println(" Entity File Record Count: " + elhf.getLabelCnt());
            System.out.println(" Predicate File Record Count: " + plhf.getLabelCnt());
            System.out.println(" Number of Pages in DB: " + SystemDefs.JavabaseDB.db_num_pages());
            System.out.println(" Subject count: " + SystemDefs.JavabaseDB.getSubjectCnt());
            System.out.println(" Predicate count: " + SystemDefs.JavabaseDB.getPredicateCnt());
            System.out.println(" Object count: " + SystemDefs.JavabaseDB.getObjectCnt());
            System.out.println(" Entity count: " + SystemDefs.JavabaseDB.getEntityCnt());
            System.out.println(" Quadruple count: " + SystemDefs.JavabaseDB. getQuadrupleCnt());

            System.out.println("############################");
            System.out.println("Page Writes : " + PCounter.wcounter);
            System.out.println("Page Read : " + PCounter.rcounter);
            System.out.println("############################\n\n\n\n");
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        return ;
    }
}
