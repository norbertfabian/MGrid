/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid.dindex;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import messif.algorithms.Algorithm;
import messif.buckets.BucketErrorCode;
import messif.buckets.BucketStorageException;
import messif.buckets.LocalBucket;
import messif.objects.LocalAbstractObject;
import messif.objects.util.AbstractObjectIterator;
import messif.objects.util.AbstractObjectList;
import messif.operations.data.BulkInsertOperation;
import messif.operations.data.InsertOperation;
import messif.operations.query.ApproxRangeQueryOperation;
import messif.operations.query.RangeQueryOperation;
import messif.pivotselection.AbstractPivotChooser;
import messif.pivotselection.IncrementalPivotChooser;
import messif.statistics.OperationStatistics;
import messif.statistics.StatisticCounter;
import messif.statistics.StatisticRefCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import cz.muni.fi.xfabian7.bp.mgrid.HdfsStorageBucket;
import cz.muni.fi.xfabian7.bp.mgrid.HdfsStorageBucketDispatcher;
import cz.muni.fi.xfabian7.bp.mgrid.MGrid;
import static cz.muni.fi.xfabian7.bp.mgrid.MGrid.HADOOP_CORESITE_FILE_PATH;

/**
 *
 * @author Vlastislav Dohnal, dohnal@fi.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic\
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic\
 * @since 2008-05-21
 */
public class DIndex extends Algorithm implements Serializable {

    /**
     * Class serial id for serialization
     */
    private static final long serialVersionUID = 2L;

    /**
     * An instance of BucketDispatcher shared within this instance of aD-index
     */
    final HdfsStorageBucketDispatcher disp;

    /**
     * List of aD-index levels (levels of hashing)
     */
    List<Hashing> levels;

    /*
     * Bucket path counetr
     */
    static int pathCounter;

    /**
     * Separation value rho
     */
    final float rho;

    /**
     * A list of fixed pivots. All objects have distances to these pivots and
     * these pivots are used on the first level.
     */
    List<LocalAbstractObject> pivotsFixed;

    private static final IncrementalPivotChooser incrementalPivotChooser = new IncrementalPivotChooser();

    /**
     * Global counter for distance computations made during splits
     */
    protected static final StatisticCounter counterSplitDistanceComputations = StatisticCounter.getStatistics("DistanceComputations.Split");
    /**
     * Global counter for distance computations
     */

    protected static final StatisticCounter counterDistanceComputations = StatisticCounter.getStatistics("DistanceComputations");

    /**
     * Global counter for the number of distance computations per bucket
     */
    protected static final StatisticRefCounter counterBucketDistanceComputations = StatisticRefCounter.getStatistics("BucketDistanceComputations");

    /**
     *
     * @param rho
     * @param bucketCapacity
     * @param pivots
     */
    @Algorithm.AlgorithmConstructor(description = "aD-index with rho, bucket capacity and pivots stream", arguments = {"rho", "bucket capacity in bytes", "stream with pivots"})
    public DIndex(float rho, long bucketCapacity, AbstractObjectIterator<LocalAbstractObject> pivots) {
        super("D-index");

        disp = new HdfsStorageBucketDispatcher(Integer.MAX_VALUE,
                Integer.MAX_VALUE /*bucketCapacity*2*/, bucketCapacity, 0, false,
                HdfsStorageBucket.class);
        this.rho = rho;
        if (pivots != null) {
            pivotsFixed = new ArrayList<>();
            while (pivots.hasNext()) {
                pivotsFixed.add(pivots.next());
            }
        } else {
            pivotsFixed = null;
        }

        FileSystem fs = getFileSystem();

        try {
            if (fs.exists(new Path(MGrid.D_INDEX_PATH))) {
                loadDIndex();
            } else {
                // Create the first level
                pathCounter = 0;
                levels = new ArrayList<>();
                try {
                    // Set global fixed pivots to the first level
                    AbstractPivotChooser chooser = getPivotChooser();
                    if (pivotsFixed != null) {
                        for (LocalAbstractObject pvt : pivotsFixed) {
                            chooser.addPivot(pvt);
                        }
                    }

                    levels.add(new HashingTree(disp, rho, chooser));
                } catch (BucketStorageException | InstantiationException ex) {
                    Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Instantiates a new pivot chooser
     *
     * @return a new instance of pivot choser class
     */
    protected static AbstractPivotChooser getPivotChooser() {
        return incrementalPivotChooser;
    }

    //**************************************************************/
    //******* SEARCH OPERATION *************************************/
    //**************************************************************/
    /**
     * Choices for algorithms evaluating queries
     */
    public enum enQueryEvalAlgoType {

        /**
         *
         */
        EVALUATION_LIMIT_TO_RHO,
        /**
         *
         */
        EVALUATION_COMPLETE;
    }

    /**
     * Choice of the evaluation algorithm for queries
     */
    protected enQueryEvalAlgoType queryEvaluationAlgorithmType = enQueryEvalAlgoType.EVALUATION_COMPLETE;

    /**
     * Sets the type of algorithms used to evaluate queries. This changes
     * behavior from now until this method is called with a different argument.
     *
     * @param type Specific type of evaluation algorithms
     */
    public void setQueryEvaluationAlgorithmType(enQueryEvalAlgoType type) {
        queryEvaluationAlgorithmType = type;
    }

    /**
     * Performs the range search operation. The answer to the query is returned
     * in the passed operation object.
     *
     * The evaluation algorithm is choosed according to the query evaluation
     * algorithm type currently set.
     *
     * @param q specifies the range query parameters
     * @return <code>true</code>
     */
    public List<Integer> rangeSearch(RangeQueryOperation q) {
        // Local operation statistics
        OperationStatistics opStat = OperationStatistics.getLocalThreadStatistics();
        opStat.registerBoundAllStats("DistanceComputations.*|BucketRead");
        List<Integer> addrList = new ArrayList<>();

        switch (queryEvaluationAlgorithmType) {
            case EVALUATION_COMPLETE:
                if (q.getRadius() <= rho) {
                    addrList.addAll(rangeSearchUptoRho(q));
                } else {
                    addrList.addAll(rangeSearchUnlimited(q));
                }
                break;
            case EVALUATION_LIMIT_TO_RHO:
                addrList.addAll(rangeSearchUptoRho(q));
                break;
            default:
                throw new UnsupportedOperationException("Unknown query evaluation algorithm type!");
        }

        // Local operation statistics
        opStat.unbindAllStats();

        return addrList;
    }

    /**
     * Performs the range search operation but the evaluation is limited to rho.
     * The correctness of the answer is ensured for objects closer to the query
     * object than rho.
     *
     * So this implementation accesses one bucket per aD-index level at maximum.
     * The early termination if the query region is exclusively in a separable
     * bucket and skipping levels if the query region is exclusively in the
     * exclusion zone are both implemented.
     *
     * @param q specifies the range query parameters
     * @return <code>true</code>
     */
    protected List<Integer> rangeSearchUptoRho(RangeQueryOperation q) {
        // Local operation statistics should be bound...
        float r = (q.getRadius() > rho) ? rho : q.getRadius();
        List<Integer> addrList = new ArrayList<>();

        for (int l = 0; l < levels.size(); l++) {
            Hashing h = levels.get(l);
            int addr;

            // Check exclusive containtment in a separable bucket
            addr = h.computeAddress(q.getQueryObject(), rho + r, true, l);
            if (addr != ExcludedMiddlePartitioning.ZONE_EXCLUSION) {
                // Return addr
                addrList.add(addr);
                break;
            }

            // Check exclusive containtment in the exclusion
            addr = h.computeAddress(q.getQueryObject(), rho - r, true, l);
            if (addr == ExcludedMiddlePartitioning.ZONE_EXCLUSION) {
                continue;       // Check the next level directly
            }

            // Get the address of the bucket identified.
            addrList.add(addr);
        }

        return addrList;
    }

    /**
     * Performs the range search operation. The answer is correct since no
     * restriction is imposed on the query radius.
     *
     * The early termination if the query region is exclusively in a separable
     * bucket and skipping levels if the query region is exclusively in the
     * exclusion zone are both implemented.
     *
     * @param q specifies the range query parameters
     * @return <code>true</code>
     */
    protected List<Integer> rangeSearchUnlimited(RangeQueryOperation q) {
        List<Integer> addrList = new ArrayList<>();
        for (int l = 0; l < levels.size(); l++) {
            Hashing h = levels.get(l);

            // Check exclusive containtment in a separable bucket
            {
                int addr = h.computeAddress(q.getQueryObject(), rho + q.getRadius(), true, l);
                if (addr != ExcludedMiddlePartitioning.ZONE_EXCLUSION) {
                    // Search this bucket and terminate.
                    addrList.add(addr);
                    break;
                }
            }

            // Checking exclusive containtment in the exclusion is not 
            // necessary becuase the radius is larger than rho. 
            // (Refer to rangeSearch() method.)
            // Access all intersected buckets
            for (int addr : h.computeAddressList(q.getQueryObject(), rho + q.getRadius(), l)) {
                // Evaluate the query on the bucket identified.
                addrList.add(addr);
            }
        }

        return addrList;
    }

    /**
     * Performs the approximate range search operation. The answer to the query
     * is returned in the passed operation object.
     *
     * This algorithm is complete and correct for any type of range query, i.e.
     * no limits are applied on the query radius.
     *
     * @param q specifies the range query parameters
     * @return <code>true</code>
     */
    public List<Integer> rangeSearch(ApproxRangeQueryOperation q) {
        // Local operation statistics
        OperationStatistics opStat = OperationStatistics.getLocalThreadStatistics();
        opStat.registerBoundAllStats("DistanceComputations.*|BucketRead");

        List<Integer> addrList = new ArrayList<>();

        // Try to acommodate the new object in a level starting from the first one.
        for (int l = 0; l < levels.size(); l++) {
            Hashing h = levels.get(l);
            int addr = h.computeAddress(q.getQueryObject(), false, l);

            // Exclusion zone has been hit, so search in the next level
            if (addr == ExcludedMiddlePartitioning.ZONE_EXCLUSION) {
                continue;
            }

            // add address to the list
            addrList.add(addr);
        }

        // Set the quarantee if approximation
        q.setRadiusGuaranteed(rho);

        // Local operation statistics
        opStat.unbindAllStats();

        return addrList;
    }

    //**************************************************************/
    //******* INSERT OPERATION *************************************/
    //**************************************************************/
    
    public int getPathCounter() {
        pathCounter++;
        return pathCounter;
    }
    /**
     * Insert operation of an local object Inserts a new object to the aD-Index
     * structure.
     *
     * @param oper Operation of insert which carries an object to be inserted.
     *
     * @return Returns true on success, otherwise false (when the hard capacity
     * of bucket is reached).
     */
    public boolean insert(InsertOperation oper) {
        // Local operation statistics
        OperationStatistics opStat = OperationStatistics.getLocalThreadStatistics();
        opStat.registerBoundAllStats("DistanceComputations.*|BucketRead");

        boolean ret = insertFromLevel(0, oper.getInsertedObject(), false);

        // Local operation statistics
        opStat.unbindAllStats();

        return ret;
    }

    /**
     * Populates an existing instance (also empty) with the passed list of
     * objects. All split operations are done after all objects are accommodated
     * in a bucket.
     *
     * @param ins list of objects to insert
     * @return <code>true</code>
     */
    public boolean bulkInsert(BulkInsertOperation ins) {
        /**
         * IDEA: insert all objects in the current structure without any
         * modification to it. if any bucket overflows, a new bucket with
         * updated capacity is created and content copied.
         *
         * After the insertion, walk through all overflowing objects (using the
         * original capacity) and do split them
         *
         * OPTIMIZATION: the passed number of objects can be used to create
         * buckets having the corresponding capacity (even disk buckets would be
         * ok)
         */

        // Local operation statistics
        OperationStatistics opStat = OperationStatistics.getLocalThreadStatistics();
        opStat.registerBoundAllStats("DistanceComputations.*|BucketRead");

        // Insert all passed object without splitting
        insertFromLevel(0, ins.getInsertedObjects().iterator(), true);

        // Fix overflowing buckets
        for (int l = 0; l < levels.size(); l++) {
            Hashing h = levels.get(l);

            // Split buckets at this level until none is overflowing
            boolean bucketSplit = true;
            while (bucketSplit) {
                bucketSplit = false;
                Iterator<Integer> addr = h.getAllAddresses();
                while (addr.hasNext()) {
                    int ad = addr.next();

                    // Check the bucket's occupation
                    if (!h.getBucket(ad).isSoftCapacityExceeded()) {
                        continue;
                    }

                    // Split the overflowing bucket
                    System.out.println("DIndex, SplitBucket :" + ad + h.getBucket(ad).toString());
                    splitBucket(l, ad, true);
                    bucketSplit = true;
                }
            }
        }

        // Local operation statistics
        opStat.unbindAllStats();

        return true;
    }

    //**************************************************************/
    //******* INTERNAL DATA MANAGEMENT *****************************/
    //**************************************************************/
    /**
     * Inserts the objects provided by the iterator to any level starting from
     * the given one.
     *
     * @param level index of the level to start inserting the object to
     * @param objects iterator over the objects to insert
     * @param bulkLoading flag specifying whether bulk-loading takes place
     * (bucket splits are forbidden) or not
     * @return <code>true</code> upon success, otherwise <code>false</code>.
     */
    protected boolean insertFromLevel(int level, Iterator<? extends LocalAbstractObject> objects, boolean bulkLoading) {
        if (objects == null) {
            return false;
        }

        while (objects.hasNext()) {
            insertFromLevel(level, objects.next(), bulkLoading);
        }
        return true;
    }

    /**
     * Inserts an objects to any level starting from the given one.
     *
     * @param level index of the level to start inserting the object to
     * @param obj the object to insert
     * @param bulkLoading flag specifying whether bulk-loading takes place
     * (bucket splits are forbidden) or not
     * @return <code>true</code> upon success, otherwise <code>false</code>.
     */
    protected boolean insertFromLevel(int level, LocalAbstractObject obj, boolean bulkLoading) {
        // Test the number of levels
        if (level >= levels.size()) {
            // Allocate a new level
            try {
                levels.add(new HashingTree(disp, rho, getPivotChooser()));
            } catch (BucketStorageException | InstantiationException ex) {
                Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        // If inserting from the first level, compute and store the distances to the fixed pivots
        if (level == 0 && pivotsFixed != null) {
            // Get the precomputed distances filter       
            PrecomputedDistancesByLevelFilter flt = HashingTree.getFilter(obj);
            for (int i = 0; i < pivotsFixed.size(); i++) {
                float dist = flt.getPrecompDist(level, i);
                if (dist == LocalAbstractObject.UNKNOWN_DISTANCE) {
                    dist = obj.getDistance(pivotsFixed.get(i));
                    // Store the distance among precomputed distances
                    flt.addPrecompDist(level, dist);
                }
            }
        }

        // Try to acommodate the new object in a level starting from the first one.
        for (int l = level; l < levels.size(); l++) {
            Hashing h = levels.get(l);
            int addr = h.computeAddress(obj, false, l);

            // Exclusion zone has been hit, so insert the object in the next level
            if (addr == ExcludedMiddlePartitioning.ZONE_EXCLUSION) {
                continue;
            }

            // Insert the new object
            BucketErrorCode err = h.getBucket(addr).addObjectErrCode(obj);
            if (err == BucketErrorCode.SOFTCAPACITY_EXCEEDED && !bulkLoading) {
                splitBucket(l, addr, bulkLoading);
            } else {
                // Update min/max radii

            }

            // The object has been inserted successfully
            break;
        }

        try {
            //Save changes
            saveDIndex();
        } catch (IOException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    /**
     * Splits a bucket of the given level <code>h</code>. The bucket is
     * specified by the passed <code>addr</code>.
     *
     * @param level identifies the hashing level
     * @param addr address of the bucket to split
     * @param bulkLoading flag specifying whether bulk-loading takes place
     * (bucket splits are forbidden) or not
     */
    protected void splitBucket(int level, int addr, boolean bulkLoading) {
        Hashing h = levels.get(level);
        LocalBucket b = h.getBucket(addr);

        counterSplitDistanceComputations.bindTo(counterDistanceComputations);

        float allObjs = b.getObjectCount() / 100.0f;

        // Get the pivot to split the bucket. Select a new one if not available.
        LocalAbstractObject p = h.getSplitPivot(addr);
        if (p == null) {
            p = h.selectNewPivot();
        }

        // Create an instance of partitioning 
        ExcludedMiddlePartitioning partitioning = new ExcludedMiddlePartitioning(b, p, rho);

        // Compute the value of dm
        float dm = partitioning.computeDm();

        // Split the bucket (a new empty bucket is created)
        int newAddr = -1;
        try {
            newAddr = h.split(addr, p, dm);
        } catch (NoSuchElementException | BucketStorageException | InstantiationException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        }

        LocalBucket newB = h.getBucket(newAddr);

        // Redistribute the data in the bucket that has been split.
        AbstractObjectList<LocalAbstractObject> exclusion = partitioning.partitionObjects(b, newB, level);

        // The object within the EXCLUSION_ZONE are stored in the next level.
        if (exclusion.size() > 0) {
            insertFromLevel(level + 1, exclusion.iterator(), bulkLoading);
        }

        // DEBUG info
        if (Logger.getLogger(DIndex.class.getName()).isLoggable(Level.INFO)) {
            String msg = String.format("Split result of bucket %s: %.2f%% : %.2f%% : %.2f%%",
                    h.formatBucketAddress(addr),
                    (float) b.getObjectCount() / allObjs,
                    100.0f - (float) (b.getObjectCount() + newB.getObjectCount()) / allObjs,
                    (float) newB.getObjectCount() / allObjs);
            Logger.getLogger(DIndex.class.getName()).log(Level.INFO, msg);
        }

        counterSplitDistanceComputations.unbind();
    }

    public int getIncrementedPathCounter() {
        return pathCounter;
    }

    @Override
    public String toString() {
        StringBuilder rtv = new StringBuilder();
        int objs = 0, bkts = 0;

        rtv.append("D-index structure:\n");
        rtv.append("rho: ").append(rho).append("\n");
        rtv.append("fixed pivots: ").append((pivotsFixed != null) ? pivotsFixed.size() : 0).append("\n");
        rtv.append("hash levels: ").append(levels.size()).append("\n");
        rtv.append("bucket capacity: ").append(disp.getBucketSoftCapacity()).append(" objs\n");
        for (Hashing h : levels) {
            objs += h.getObjectCount();
            bkts += h.getBucketCount();
        }
        rtv.append("buckets: ").append(bkts).append("\n");
        rtv.append("objects: ").append(objs).append("\n");
        rtv.append("\n");

        for (int l = 0; l < levels.size(); l++) {
            rtv.append("level info: ").append(l).append("\n");
            rtv.append(levels.get(l)).append("\n");
        }

        return rtv.toString();
    }
    //***********************************************************
    //************** SAVE AND LOAD DINDEX ***********************
    //***********************************************************

    public FileSystem getFileSystem() {
        FileSystem fs = null;
        try {
            final Configuration confFileSystem = new Configuration();
            confFileSystem.addResource(new Path(HADOOP_CORESITE_FILE_PATH));
            fs = FileSystem.get(confFileSystem);
        } catch (IOException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
        return fs;
    }

    public void saveDIndex() throws IOException {
        FileSystem fs = getFileSystem();
        try (ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path(MGrid.D_INDEX_PATH)))) {
            oos.writeObject(levels);
            oos.writeObject(pathCounter);
        } catch (IOException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void loadDIndex() {
        ObjectInputStream ois = null;
        try {
            FileSystem fs = getFileSystem();
            ois = new ObjectInputStream(fs.open(new Path(MGrid.D_INDEX_PATH)));
            this.levels = (List<Hashing>) ois.readObject();
            this.pathCounter = (Integer) ois.readObject();
            ois.close();
        } catch (IOException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                ois.close();
            } catch (IOException ex) {
                Logger.getLogger(DIndex.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
