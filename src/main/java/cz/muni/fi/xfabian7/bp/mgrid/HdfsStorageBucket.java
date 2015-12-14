/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import messif.buckets.BucketStorageException;
import messif.buckets.LocalBucket;
import messif.buckets.index.IndexComparator;
import messif.buckets.index.ModifiableIndex;
import messif.buckets.index.ModifiableSearch;
import messif.buckets.index.impl.AbstractSearch;
import messif.objects.LocalAbstractObject;
import org.apache.hadoop.conf.Configuration;
import java.util.ListIterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import messif.buckets.CapacityFullException;
import messif.buckets.StorageFailureException;
import messif.operations.data.DeleteOperation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This is an extended LocalBucket storing objects using HDFS files
 *
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic\
 */
public class HdfsStorageBucket extends LocalBucket implements ModifiableIndex<LocalAbstractObject> {

    /**
     * Class serial id for serialization
     */
    private static final long serialVersionUID = 1L;

    //**************** Local data ***********************//
    /**
     * Stored object count
     */
    private int objectCount = 0;

    //**************** HDFS Configuration****************//
    public static final String HADOOP_CORESITE_FILE_PATH = MGrid.HADOOP_CORESITE_FILE_PATH;
    private String path;

    //**************** Constructors ********************//
    public HdfsStorageBucket(HdfsStorageBucket hdfsStorageBucket) {
        this(
                hdfsStorageBucket.getCapacity(),
                hdfsStorageBucket.getSoftCapacity(),
                hdfsStorageBucket.getLowOccupation(),
                false,
                hdfsStorageBucket.getPath()
        );
    }

    public HdfsStorageBucket(long capacity, long softCapacity, long lowOccupation, boolean occupationAsBytes, Integer addr) {
        this(capacity, softCapacity, lowOccupation, occupationAsBytes, MGrid.FILE_PATH + addr);
    }

    public HdfsStorageBucket(long capacity, long softCapacity, long lowOccupation, boolean occupationAsBytes, String path) {
        super(capacity, softCapacity, lowOccupation, occupationAsBytes);
        this.path = path;
        createFile(new Path(path));
        System.out.println("HdfsStorageBucket constructor create bucket:" + path);
        try {
            readMetaFile();
        } catch (IOException ex) {
            Logger.getLogger(HdfsStorageBucket.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //************* Index method overrides ************//
    @Override
    protected ModifiableIndex<LocalAbstractObject> getModifiableIndex() {
        return this;
    }

    //********** bucket info operations **************//
    @Override
    public int size() {
        return objectCount;
    }

    public void setSize(int i) {
        objectCount = i;
    }

    public String getPath() {
        return path;
    }

    public void setPath(int addr) {
        path = MGrid.FILE_PATH + addr;
    }

    //************* File opearations*************//
    public static HdfsStorageBucket getBucket(long capacity, long softCapacity, long lowOccupation, boolean occupationAsBytes, Map<String, Object> parameters) throws IllegalArgumentException {
        if (parameters == null) {
            throw new IllegalArgumentException("Missing address Integer");
        }
        Object addr = parameters.get("HashMapAddress");

        if (addr instanceof Integer) {
            return new HdfsStorageBucket(capacity, softCapacity, lowOccupation, occupationAsBytes, (Integer) addr);
        }
        throw new IllegalArgumentException("Incorrect param type");
    }

    /**
     * Create a new HDFS file with the given path
     *
     * @param path
     */
    public void createFile(Path path) {
        try {
            FileSystem fs = getFileSystem();

            // Check if the file already exists
            if (fs.exists(path)) {
                System.out.println("File " + path + " already exists");
//                fs.close();
                return;
            }
            fs.createNewFile(path);
//            fs.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsStorageBucket.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Delete a file with the given path
     *
     * @param path = path;
     */
    public void deleteFile(Path path) {
        FileSystem fs;
        try {
            fs = getFileSystem();

            if (!fs.exists(path)) {
                return;
            }

            fs.delete(path, true);
            System.out.println("Delete File HdfsStorageBucket");
        } catch (IOException ex) {
            Logger.getLogger(HdfsStorageBucket.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * This method return the FileSystem
     *
     * @return FileSystem
     * @throws IOException
     */
    public FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path(HADOOP_CORESITE_FILE_PATH));
        return FileSystem.get(conf);
    }

    /**
     * This method opens an InputStream over the path
     *
     * @return InputStream
     * @throws IOException
     */
    public InputStream openInputStream(Path path) throws IOException {
        FileSystem fs = getFileSystem();
        if (!fs.exists(path)) {
            createFile(path);
        }
        return fs.open(path);
    }

    /**
     * Return an OutputStream over the path using FileSystem
     *
     * @param fs FileSystem
     * @return OutputStream
     * @throws IOException
     */
    public OutputStream openOutputStream(FileSystem fs, Path path) throws IOException {
        if (!fs.isFile(path)) {
            createFile(path);
        }
        return fs.append(path);
    }

    /**
     * Open an ObjectInputStream over the InputStream
     *
     * @return ObjectInputStream
     * @throws IOException
     */
    public ObjectInputStream openObjectInputStream() throws IOException {
        System.out.println("HdfsStorageBucket openObjectInputStream:" + path);
        return new ObjectInputStream(openInputStream(new Path(path)));
    }

    /**
     * Open an ObjectInputStream over the InputStream. If the files length is
     * longer then 0, open AppendingObjectOutpuStream
     *
     * @param fs
     * @param append
     * @return ObjectOutputStream
     * @throws IOException
     */
    public ObjectOutputStream openObjectOutputStream(FileSystem fs, Path path, boolean append) throws IOException {
        System.out.println("HdfsStorageBucket openObjectOutputStream:" + path);
        if (append) {
            return new AppendingObjectOutputStream(openOutputStream(fs, path));
        }
        return new ObjectOutputStream(openOutputStream(fs, path));
    }

    //************* Meta file methods *****************//
    /**
     * Creating a new meta file on the same path with the same name and .meta
     * suffix
     *
     * @throws IOException
     */
    public void createMetaFile() {
        Path metaFilePath = new Path(path + ".meta");
        deleteFile(metaFilePath);
        createFile(metaFilePath);
        try {
            writeMetaFile();
        } catch (IOException ex) {
            Logger.getLogger(HdfsStorageBucket.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Read serialID and objectCount from meta file
     *
     * @throws IOException
     */
    public void readMetaFile() throws IOException {

        FileSystem fs = getFileSystem();
        Path metaFilePath = new Path(path + ".meta");

        System.out.println("HdfsStorageBucket readMetaFile:" + path);

        if (fs.isFile(metaFilePath)) {

            InputStream in = openInputStream(metaFilePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            setSize(Integer.valueOf(br.readLine()));
            in.close();
        } else {
            createMetaFile();
        }
    }

    /**
     * Write serialID and objectCount into meta file
     *
     * @throws IOException
     */
    public void writeMetaFile() throws IOException {
        FileSystem fs = getFileSystem();
        OutputStream out = openOutputStream(fs, new Path(path + ".meta"));
        PrintStream ps = new PrintStream(out);

        ps.println(objectCount);

        out.close();
//        fs.close();
    }

    //******* Object operations************//
    /**
     * Removes the given object by calling {@link DeleteOperation} on the
     * encapsulated algorithm.
     *
     * @param object the object to delete
     * @throws BucketStorageException if the algorithm does not support delete
     * operation or there was an error deleting the object
     */
    @Override
    public int deleteObject(LocalAbstractObject object, int deleteLimit) throws BucketStorageException {
        ObjectInputStream in = null;
        List<LocalAbstractObject> list = new ArrayList<>();
        try {
            in = openObjectInputStream();
            LocalAbstractObject o;
            int i = 0;
            while (true) {
                i++;
                o = (LocalAbstractObject) in.readObject();
                list.add((LocalAbstractObject) o);
            }
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(HdfsStorageBucket.class.getName()).log(Level.SEVERE, null, ex);
            }
            List objects = list;
            objects.remove(object);

            System.out.println("deleteObject - deleting file!");
            deleteFile(new Path(path));
            createFile(new Path(path));

            objectCount = 0;
            addObjects(objects);

            createMetaFile();
            return 1;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot initialize hdfs storage search: " + e, e);
        }
    }

    @Override
    public ModifiableSearch<LocalAbstractObject> search() throws IllegalStateException {
        return new HdfsStorageSearch<Object>(null, Collections.emptyList());
    }

    @Override
    public <C> ModifiableSearch<LocalAbstractObject> search(IndexComparator<? super C, ? super LocalAbstractObject> comparator, C key) throws IllegalStateException {
        return new HdfsStorageSearch<Object>(null, (List<Object>) Collections.singletonList(key));
    }

    @Override
    public <C> ModifiableSearch<LocalAbstractObject> search(IndexComparator<? super C, ? super LocalAbstractObject> comparator, Collection<? extends C> keys) throws IllegalStateException {
        return new HdfsStorageSearch<C>(comparator, (Collection<? extends C>) (List<Object>) keys);
    }

    @Override
    public <C> ModifiableSearch<LocalAbstractObject> search(IndexComparator<? super C, ? super LocalAbstractObject> comparator, C from, C to) throws IllegalStateException {
        return new HdfsStorageSearch<C>(comparator, from, to);
    }

    /**
     * Add an object to the bucket
     *
     * @param T
     * @return true if the object was successfully added
     * @throws BucketStorageException
     */
    @Override
    public boolean add(LocalAbstractObject object) throws BucketStorageException {
        try {
            // Open output stream if not opened yet (this statement is never reached if the storage is readonly)
            FileSystem fs = getFileSystem();
            ObjectOutputStream oos = openObjectOutputStream(fs, new Path(path), fs.getFileStatus(new Path(path)).getLen() != 0);

            // Write object
            oos.writeObject(object);
            oos.reset();

            // Update internal counters
            objectCount++;

            oos.close();
            createMetaFile();
            return true;
        } catch (EOFException e) {
            throw new CapacityFullException(e.getMessage());
        } catch (IOException e) {
            throw new StorageFailureException("Cannot store object into hdfs storage", e);
        }

    }

    /**
     * Add objects to the bucket using Iterator of LocalAbstractObject
     *
     * @param objects
     * @return amounts of added objects
     * @throws BucketStorageException
     */
    @Override
    public int addObjects(Iterator<? extends LocalAbstractObject> objects) throws BucketStorageException {
        int i = 0;

        try {
            // Open output stream if not opened yet (this statement is never reached if the storage is readonly)
            FileSystem fs = getFileSystem();
            ObjectOutputStream oos = openObjectOutputStream(fs, new Path(path), fs.getFileStatus(new Path(path)).getLen() != 0);
            for (Iterator<? extends LocalAbstractObject> iter = objects; iter.hasNext();) {
                LocalAbstractObject obj = iter.next();

                // Write object
                oos.writeObject(obj);
                oos.reset();

                // Update internal counters
                objectCount++;
            }
            oos.close();
//            fs.close();
            createMetaFile();
        } catch (EOFException e) {
            throw new CapacityFullException(e.getMessage());
        } catch (IOException e) {
            throw new StorageFailureException("Cannot store object into hdfs storage", e);
        }
        i++;

        return i;
    }

    @Override
    public void destroy() throws Throwable {
        super.destroy(); //To change body of generated methods, choose Tools | Templates.
        deleteFile(new Path(path));
        System.out.println("Destroy deleting file");
    }

    private class HdfsStorageSearch<C> extends AbstractSearch<C, LocalAbstractObject> implements ModifiableSearch<LocalAbstractObject> {

        /**
         * Internal stream that reads objects in this storage one by one
         */
        private ObjectInputStream objectInputStream = null;
        /**
         * Position of the last returned object - used for removal
         */

        private final ListIterator<LocalAbstractObject> iterator;

        private HdfsStorageSearch(IndexComparator<? super C, ? super LocalAbstractObject> comparator,
                Collection<? extends C> keys) {
            super(comparator, keys);
            try {
                System.out.println("HdfsStorageSearch constructor:" + path);
                this.iterator = (ListIterator<LocalAbstractObject>) HdfsStorageSearchRead().listIterator();
            } catch (Exception e) {
                throw new IllegalStateException("Cannot initialize hdfs storage search: " + e, e);
            }
        }

        @SuppressWarnings("unchecked")
        private HdfsStorageSearch(IndexComparator<? super C, ? super LocalAbstractObject> comparator,
                C fromKey, C toKey) {
            super(comparator, fromKey, toKey);
            try {
                System.out.println("HdfsStorageSearch constructor:" + path);
                this.iterator = (ListIterator<LocalAbstractObject>) HdfsStorageSearchRead().listIterator();
            } catch (Exception e) {
                throw new IllegalStateException("Cannot initialize hdfs storage search: " + e, e);
            }
        }

        protected List<LocalAbstractObject> HdfsStorageSearchRead() throws IOException {
            List<LocalAbstractObject> list = new ArrayList<>();
            try {
                System.out.println("HdfsStorageSearch read before open IS:" + path);
                this.objectInputStream = openObjectInputStream();
                System.out.println("HdfsStorageSearch read after open IS:" + path);
                LocalAbstractObject object;
                int i = 0;
                while (true) {
                    i++;
                    object = (LocalAbstractObject) objectInputStream.readObject();
                    list.add((LocalAbstractObject) object);
                }
            } catch (EOFException e) {
                objectInputStream.close();
                return list;
            } catch (Exception e) {
                throw new IllegalStateException("Cannot initialize hdfs storage search: " + e, e);
            }
        }

        @Override
        protected LocalAbstractObject readNext() throws BucketStorageException {
            return iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        protected LocalAbstractObject readPrevious() throws BucketStorageException {
            return iterator.hasPrevious() ? iterator.previous() : null;
        }

        @Override
        public void close() {
        }

        @Override
        public void remove() throws IllegalStateException, BucketStorageException {
            LocalAbstractObject object = getCurrentObject();
            if (object == null) {
                throw new IllegalStateException("There is no object to delete yet");
            }
            deleteObject(object);
        }
    }

    public static class AppendingObjectOutputStream extends ObjectOutputStream {

        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // do not write a header, but reset:
            // this line added after another question
            // showed a problem with the original
            reset();
        }

    }

}
