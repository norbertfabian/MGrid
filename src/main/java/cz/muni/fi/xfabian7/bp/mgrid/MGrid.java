package cz.muni.fi.xfabian7.bp.mgrid;

import cz.muni.fi.xfabian7.bp.mgrid.dindex.DIndex;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import messif.buckets.BucketStorageException;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;
import messif.objects.util.RankedAbstractObject;
import messif.objects.util.StreamGenericAbstractObjectIterator;
import messif.operations.QueryOperation;
import messif.operations.data.BulkInsertOperation;
import messif.operations.data.InsertOperation;
import messif.operations.query.RangeQueryOperation;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import cz.muni.fi.xfabian7.bp.mgrid.objects.Creator;
import cz.muni.fi.xfabian7.bp.mgrid.objects.MyMetaObject;
import messif.statistics.StatisticTimer;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * This class is the driver class of the searching technique. Lunching the
 * structure creating and generating queries
 *
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic\
 */
public class MGrid {

    //DIndex architecture
    private static DIndex dIndex;

    private static JavaSparkContext sc;

    private static FileSystem fs;

    //Path to the DIndex configuration file as String
    public static final String D_INDEX_PATH = "hdfs://nymfe01.fi.muni.cz:9000/DIndex";
    //public static final String D_INDEX_PATH = "hdfs://nymfe75.fi.muni.cz:9000/DIndex";
    //public static final String D_INDEX_PATH = "hdfs://localhost:9000/DIndex";

    //Prefix of the path to HdfsBuckets as String
    public static final String FILE_PATH = "hdfs://nymfe01.fi.muni.cz:9000/HdfsBucket";
    //public static final String FILE_PATH = "hdfs://nymfe75.fi.muni.cz:9000/HdfsBucket";
    //public static final String FILE_PATH = "hdfs://localhost:9000/HdfsBucket";

    //Path to the haddop core-site.xml as String
    //public static final String HADOOP_CORESITE_FILE_PATH = "/home/norbert/Desktop/hadoop-2.6.0/conf/core-site.xml";
    public static final String HADOOP_CORESITE_FILE_PATH = "/home/xfabian7/BP/hadoop-2.6.0/conf/core-site.xml";
    //public static final String HADOOP_CORESITE_FILE_PATH = "/home/norbert/Desktop/cluster_hadoop-2.6.0/etc/hadoop/core-site.xml";
    public static void main(String[] args) throws IOException, BucketStorageException {

        //Create an Iterator with the pivots given in the FILE_PATH + "pivots" file
        fs = getFileSystem();
        if (!fs.exists(new Path(FILE_PATH + "pivots"))) {
            fs.createNewFile(new Path(FILE_PATH + "pivots"));
        }
        InputStream in = fs.open(new Path(FILE_PATH + "pivots"));
        BufferedReader pivotBufferedStream = new BufferedReader(new InputStreamReader(in));

        AbstractObjectIterator pivots = new StreamGenericAbstractObjectIterator(MyMetaObject.class, pivotBufferedStream);

        //Create an instance of DIndex

        dIndex = new DIndex(0.4f, 70, pivots);
        System.out.println(dIndex.toString());

        //Configure Spark
        SparkConf conf;
        conf = new SparkConf()
                .setAppName("MGrid");
        sc = new JavaSparkContext(conf);

        StatisticTimer timer = StatisticTimer.getStatistics("Timer");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String answer = "a";
        while (!"e".equals(answer)) {

            System.out.println("g - generate new query");
            System.out.println("e - exit");
            System.out.println("c - create file");
            System.out.println("i - write all objects from a file");
            System.out.println("d - delete file");
            System.out.println("da - delete range of files");
            System.out.println("f - add objects into a file");
            System.out.println("b - bulk insert into DIndex");
            System.out.println("o - inser objects into DIndex without using bulk insert");
            System.out.println("di - write DIndex info");
            System.out.println("b - BulkInsert");
            answer = br.readLine();

            switch (answer) {
                case "g": {

                    List<QueryOperation> listQueryOperation = new ArrayList<>();

                    System.out.println("Write how many queries parallelize:");
                    int paralellQueriesAmount = Integer.parseInt(br.readLine());

                    for (int i = 0; i < paralellQueriesAmount; i++) {

                        // Create query
                        System.out.println("write the range");
                        String r = br.readLine();

                        LocalAbstractObject query = Creator.createRandomMyMetaObject("Query" + i);
                        QueryOperation queryOperation = new RangeQueryOperation(query, Float.parseFloat(r));

                        listQueryOperation.add(queryOperation);

                    }

                    timer.start();
                    // Create JRDD from query
                    JavaRDD<QueryOperation> qoRDD = sc.parallelize((List) listQueryOperation);

                    // Find address
                    JavaPairRDD<QueryOperation, String> addrRDD = qoRDD.flatMapToPair(new EvaluateAddress(dIndex));

                    // Evaluate query on the given addresses
                    JavaRDD<Iterable<RankedAbstractObject>> results = addrRDD.map(new EvaluateQuery());

                    //Write the results
                    System.out.println(results.toArray());
                    for (Iterable<RankedAbstractObject> itObj : results.collect()) {
                        for (RankedAbstractObject obj : itObj) {
                            System.out.println(obj);
                        }
                    }
                    timer.stop();
                    System.out.println("Time: " + timer.toString());
                    break;
                }
                //Create file
                case "c": {
                    System.out.println("write the number of the file");
                    String file = br.readLine();
                    fs = getFileSystem();
                    fs.createNewFile(new Path(FILE_PATH + file));
                    break;
                }
                //get all objects from a file
                case "i": {
                    System.out.println("write the number of the file for getting all objects");
                    String file = br.readLine();
                    HdfsStorageBucket hdfsStorage = new HdfsStorageBucket(Integer.MAX_VALUE,
                            8, 0, false, FILE_PATH + file);
                    System.out.println("Object count: " + hdfsStorage.getObjectCount());
                    Iterator<LocalAbstractObject> iterator = hdfsStorage.getAllObjects();
                    while (iterator.hasNext()) {
                        System.out.println(iterator.next());
                    }
                    break;
                }
                //delete file
                case "d": {
                    fs = getFileSystem();
                    System.out.println("write the number of the file");
                    String file = br.readLine();
                    if (file.equals("DIndex")) {
                        fs.delete(new Path(D_INDEX_PATH), true);
                    } else {
                        fs.delete(new Path(FILE_PATH + file), true);
                        fs.delete(new Path(FILE_PATH + file + ".meta"), true);
                    }
                    break;
                }
                //delete file range
                case "da": {
                    fs = getFileSystem();
                    System.out.println("write the first number of the range");
                    String file1 = br.readLine();
                    int i = Integer.parseInt(file1);
                    System.out.println("write the last number of the range");
                    String file2 = br.readLine();
                    int j = Integer.parseInt(file2);
                    for (int k = i; k <= j; k++) {
                        fs.delete(new Path(FILE_PATH + k), true);
                        fs.delete(new Path(FILE_PATH + k + ".meta"), true);
                    }
                    break;
                }
                //add objects to file
                case "f": {
                    System.out.println("write the number of the file for adding object");
                    String file = br.readLine();
                    HdfsStorageBucket hdfsStorage = new HdfsStorageBucket(Integer.MAX_VALUE,
                            8, 0, false, FILE_PATH + file);
                    List<LocalAbstractObject> objects = new ArrayList<>();
                    System.out.println("write how many objects you want to store");
                    String num = br.readLine();
                    for (int i = 0; i < Integer.parseInt(num); i++) {
                        objects.add(Creator.createRandomMyMetaObject(RandomStringUtils.random(10)));
                    }
                    hdfsStorage.addObjects(objects);
                    break;
                }
                //insert 20 objects into DIndex
                case "o": {
                    System.out.println("write how many objects you want to store");
                    String num = br.readLine();
                    for (int i = 0; i < Integer.parseInt(num); i++) {
                        System.out.println("before insert");
                        while (true) {
                            if (dIndex.insert(new InsertOperation(Creator.createRandomMyMetaObject(RandomStringUtils.random(10))))) {
                                break;
                            }
                        }
                        System.out.println("after insert");
                    }
//                    for (int i = 0; i < Integer.parseInt(num); i++) {
//                        dIndex.insert(new InsertOperation( metaObjectsIterator.next()));
//                    }
                    break;
                }
                //Bulk insert
                case "b": {
                    System.out.println("write how many objects you want to store ");
                    String num = br.readLine();
                    System.out.println("id start from number: ");
                    String start = br.readLine();
                    ArrayList<MyMetaObject> listOfObjects = new ArrayList<>();
                    for (int i = Integer.parseInt(start); i < Integer.parseInt(num); i++) {
                        listOfObjects.add((Creator.createRandomMyMetaObject(Integer.toString(i))));

                    }
                    dIndex.bulkInsert(new BulkInsertOperation(listOfObjects));

                    break;

                }

                //write information about DIndex
                case "di": {
                    System.out.println(dIndex.toString());
                    break;
                }

            }
        }

    }

    public static FileSystem getFileSystem() throws IOException {
        final Configuration confFileSystem = new Configuration();
        confFileSystem.addResource(new Path(HADOOP_CORESITE_FILE_PATH));
        FileSystem fs = FileSystem.get(confFileSystem);
        return fs;
    }
}
