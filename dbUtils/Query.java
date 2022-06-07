package dbUtils;

import diskmgr.PCounter;
import diskmgr.Stream;
import global.QID;
import global.SystemDefs;
import quadrupleheap.Quadruple;

import java.io.File;

public class Query {

	public static SystemDefs systemDefs = null;

	static String database = null;
	public static int numberBuffers = 100;

	static String subject = null;
	static String object = null;
	static String predicate = null;
	static String confidence = null;
	static String TMP = "/tmp/";
	static int sortOrder = 1;
	static int indexOption = 1;

	public static double updatedConfidence = -1.0;

	public static void main(String[] args)
			throws Exception {
		if (args.length == 8) {
			database = TMP + args[0];
			indexOption = Integer.parseInt(args[1]);
			sortOrder = Integer.parseInt(args[2]);
			subject = args[3];
			predicate = args[4];
			object = args[5];
			confidence = args[6];
			numberBuffers = Integer.parseInt(args[7]);
			database = database + "_" + indexOption;

			if (numberBuffers < 50) {
				System.out.println("Count of buffers too low, setting it to 50");
				numberBuffers = 50;
			}

			if (indexOption > 5 || indexOption < 0) {
				System.out.println("~~~ Please select a number between 1-5 ~~~");
				return;
			}

			if (sortOrder > 6 || sortOrder < 0) {
				System.out.println("~~~ Please select a number between 1-6 ~~~");
				return;
			}

			if (confidence.compareToIgnoreCase("*") != 0) {
				updatedConfidence = Double.parseDouble(confidence);
			}

			File databaseFile = new File(database);
			if (databaseFile.exists()) {
				systemDefs = new SystemDefs(database, 0, numberBuffers, "", indexOption);
				if (sortOrder == 1) System.out.println("Sorting by Subject-Predicate-Object-Confidence");
				if (sortOrder == 2) System.out.println("Sorting by Predicate-Subject-Object-Confidence");
				if (sortOrder == 3) System.out.println("Sorting by Subject-Confidence");
				if (sortOrder == 4) System.out.println("Sorting by Predicate-Confidence");
				if (sortOrder == 5) System.out.println("Sorting by Object-Confidence");
				if (sortOrder == 6) System.out.println("Sorting by Subject-Predicate-Object-Confidence");
				System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			} else {
				System.out.println("~~~ Invalid database, please try again ~~~");
				return;
			}

			Stream stream = SystemDefs.JavabaseDB.
					openStream(database, indexOption, sortOrder, subject, predicate, object, updatedConfidence);
			Quadruple quadruple;
			QID quadrupleId = null;
			while ((quadruple = stream.getNext(quadrupleId)) != null) {
				double confidence = quadruple.getConfidence();
				String subject = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getSubjectID().returnLID());
				String object = SystemDefs.JavabaseDB.getEntityHandle().getLabel(quadruple.getObjectID().returnLID());
				String predicate = SystemDefs.JavabaseDB.getPredicateHandle().getLabel(quadruple.getPredicateID().returnLID());
				System.out.println("Quadruple : " + subject + " " + predicate + " " + object + " " + confidence);
			}
			if (stream != null) {
				stream.closeStream();
			}

		} else {
			System.out.println("Query RDFDBNAME ORDER INDEXOPTION SUBJECTFILTER PREDICATEFILTER OBJECTFILTER CONFIDENCEFILTER NUMBUF***");
			return;
		}

		System.out.println("TOTAL PAGE WRITES: " + PCounter.wcounter);
		System.out.println("TOTAL PAGE READS: " + PCounter.rcounter);
		SystemDefs.close();
	}
}
