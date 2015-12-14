package cz.muni.fi.xfabian7.bp.mgrid.objects;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import messif.buckets.BucketStorageException;
import messif.buckets.impl.DiskBlockBucket;
import messif.objects.LocalAbstractObject;
import messif.objects.util.RankedAbstractObject;
import messif.operations.QueryOperation;
import messif.operations.query.KNNQueryOperation;

/**
 * Testing class for new objects.
 *
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic\
 */
public class Creator {

    /**
     * Creates a Meta data file for {@link MyObject} and {@link MyObjectVect}
     * 
     *
     * @param args no arguments are expected
     * @throws IOException if there was an error writing/reading the data file
     * @throws messif.buckets.BucketStorageException
     */
    public static void main(String[] args) throws IOException, BucketStorageException {

        File[] localBucket = new File[20];
        DiskBlockBucket[] bucket = new DiskBlockBucket[20];

        for (int i = 1; i < 6; i++) {
            localBucket[i] = new File("DiskBlockBucket" + i);
            bucket[i] = new DiskBlockBucket(Integer.MAX_VALUE, 8, 0, localBucket[i]);
            for (int j = 1; j < 11; j++) {
                bucket[i].addObject(createRandomMyMetaObject(Integer.toString(j)));
            }
            System.out.println(bucket[i].getObjectCount());
        }

        QueryOperation queryOperation = new KNNQueryOperation(createRandomMyMetaObject("query"), 5);
        queryOperation.evaluate(bucket[1].getAllObjects());
        for (Iterator iterator = bucket[1].getAllObjects(); iterator.hasNext();) {
            Object next = iterator.next();
            System.out.print("BO:");
            System.out.println(next);
        }
        Iterator<RankedAbstractObject> it = queryOperation.getAnswer();
        while (it.hasNext()) {
            System.out.print("Similar: ");
            RankedAbstractObject obj = it.next();
            System.out.println(obj);
        }
    }

    /**
     * Writes {@code count} randomly generated {@link MyMetaObject}s to the
     * given {@code file}.
     *
     * @param file the file into which to write the data
     * @param count number of objects to generate
     * @throws IOException if there was a problem writing to file
     */
    public static void writeRandomMyMetaObjects(String file, int count) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file)) {
            for (int i = 1; i <= count; i++) {
                createRandomMyMetaObject(Integer.toString(i)).write(out);
            }
        }
    }

    /**
     * Creates a new {@link MyMetaObject} with random number and vector
     * descriptors.
     *
     * @param locator the object locator to use
     * @return the new {@link MyMetaObject}
     */
    public static MyMetaObject createRandomMyMetaObject(String locator) {
        MyObject obj = new MyObject((float) Math.random());
        MyObjectVect objVect = new MyObjectVect((float) Math.random(), (float) Math.random(), (float) Math.random());
        return new MyMetaObject(locator, obj, objVect);
    }

    /**
     * Read each object from the iterator and compute its distance to the given
     * {@code distObject}.
     *
     * @param iterator the iterator over all objects
     * @param distObj the object against which to get the distance
     */
    public static void readDataGetDistance(Iterator<? extends LocalAbstractObject> iterator, LocalAbstractObject distObj) {
        while (iterator.hasNext()) {
            // Read next object from the iterator
            LocalAbstractObject obj = iterator.next();
            // Write the result to the output
            System.out.println(
                    "Distance of " + obj + " to " + distObj + " = "
                    + distObj.getDistance(obj)
            );
        }
    }

}
