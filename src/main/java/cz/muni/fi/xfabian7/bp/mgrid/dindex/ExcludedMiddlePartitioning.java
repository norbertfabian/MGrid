/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid.dindex;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import messif.buckets.BucketStorageException;
import messif.buckets.LocalBucket;
import messif.objects.util.AbstractObjectList;
import messif.objects.util.AbstractObjectIterator;
import messif.objects.LocalAbstractObject;

/**
 * This class wraps splitting procedure of objects into partitions and
 * estimation of new value of dm.
 *
 * @author Vlastislav Dohnal, dohnal@fi.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic
 * @since 2008-05-21
 */
public class ExcludedMiddlePartitioning implements Serializable {

    /**
     * Class serial id for serialization
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constant denoting inner partition (or zone) = 0
     */
    public static final int ZONE_IN = 0;
    /**
     * Constant denoting outer partition (or zone) = 1
     */
    public static final int ZONE_OUT = 1;
    /**
     * Constant denoting exclusion partition (or zone) = -1
     */
    public static final int ZONE_EXCLUSION = -1;

    /**
     * List of items corresponding to all objects in a bucket
     */
    protected PartListItem items[] = null;

    /**
     * A static instance of the comparator of PartListItems by their distance
     */
    private static final PartListItemCompareByDistance sortByDistance = new PartListItemCompareByDistance();
    /**
     * A static instance of the comparator of PartListItems by their distance
     */
    private static final PartListItemCompareByIndex sortByIndex = new PartListItemCompareByIndex();

    /**
     * Current value of rho
     */
    protected float rho;

    /**
     * Value of dm used in partitioning
     */
    protected float dm = LocalAbstractObject.UNKNOWN_DISTANCE;

    /**
     * Creates a new instance and initializes the internal array of items from
     * the passed parameters.
     *
     * @param bucket source of objects
     * @param pivot pivot used to computed distances
     * @param rho value of rho to use in partitioning
     */
    public ExcludedMiddlePartitioning(LocalBucket bucket, LocalAbstractObject pivot, float rho) {
        this.rho = rho;

        // Allocate the array of items
        items = new PartListItem[bucket.getObjectCount()];

        // For each object in the bucket create a new item.
        //for (AbstractObjectIterator<LocalAbstractObject> it = bucket.getAllObjects(); it.hasNext(); idx++) {
        //int idx = 0;
        AbstractObjectIterator<LocalAbstractObject> it = bucket.getAllObjects();
        int objCount = bucket.getObjectCount();
        for (int idx = 0; idx < objCount; idx++) {
            items[idx] = new PartListItem(it.next().getDistance(pivot), idx);
        }
    }

    /**
     * Estimates the value of dm in a way that a balanced split is achived. It
     * operates over the distances computed from the passed pivot and objects in
     * the passed bucket. The pivot and bucket were given to the constructor.
     *
     * @return value of dm
     */
    public float computeDm() {
        // Sort the list by distance
        Arrays.sort(items, sortByDistance);

        if (rho == 0) {
            dm = items[items.length / 2].getDistance();     // Return the median of distances
            return dm;
        }

        // Non-zero values of rho
        float rho2 = rho * 2;

        int idxIN = items.length - 1;
        int idxBestIN = -1;
        int sizeBestPart = -1;
        int idxOUT = items.length;

        while (idxIN >= sizeBestPart && idxIN >= 0) {
            // The value of distance determined by idxIN is equal to (dm-rho).

            // Get the index of the upper bound (i.e., dm+rho)
            float dmPlusRho = items[idxIN].getDistance() + rho2;
            //int idxOUT = idxIN+1;
            //while (idxOUT < dists.length && dists[idxOUT] <= dmPlusRho)
            //    idxOUT++;
            while (idxOUT > idxIN && items[idxOUT - 1].getDistance() > dmPlusRho) {
                idxOUT--;
            }

            // Compute sizes of individual partitions (estimate the goodness of partitioning).
            int inSize = idxIN + 1;
            int outSize = items.length - idxOUT;

            // Remember the setting if it is better
            if (Math.min(inSize, outSize) > sizeBestPart) {
                sizeBestPart = Math.min(inSize, outSize);
                idxBestIN = idxIN;
            }

            // Move the lower bound down...
            idxIN--;
            // Move over all objects at the same distance.
            while (idxIN >= 0 && items[idxIN].getDistance() == items[idxIN + 1].getDistance()) {
                idxIN--;
            }
        }

        // Best partitioning (equally divided between IN and OUT partitions) with at least 50% of data
        // cannot be established (50% means the sum of IN and OUT sizes)
        if (sizeBestPart < items.length / 4 || idxBestIN == -1) {
            // Return the median plus rho (The data will be split equally between IN partition and exclusion)
            dm = items[items.length / 2].getDistance() + rho;
        } else {
            // Return the best setting
            dm = items[idxBestIN].getDistance() + rho;
        }
        return dm;
    }

    /**
     * Partitions objects in stored in the <code>items</code> array. The objects
     * in the inner partition are deleted from <code>bucketInPart</code>. The
     * objects in the outer partition are inserted in
     * <code>bucketOutPart</code>. The objects in the exclusion partitioning are
     * returned as iterator.
     *
     * It uses the <code>dm</code> value to partition the objects, so it must be
     * initialized by calling {@link #computeDm()} first. Otherwise, a runtime
     * exception is thrown.
     *
     * The objects stored internally in the <code>items</code> array come from
     * the bucket <code>bucketInPart</code>.
     *
     * It stores all distances computed within precomputed distances of the
     * given level of hashing. Storing is not done if a negative value is passed
     * in <code>levelToStoreDistances</code>.
     *
     * @param bucketInPart bucket storing objects in the inner zone
     * @param bucketOutPart bucket storing objects in the outer zone
     * @param levelToStoreDistances current level of hashing in which the
     * precomputed distance will be stored.
     * @return a list of all objects falling in the exclusion zone
     */
    public AbstractObjectList<LocalAbstractObject> partitionObjects(LocalBucket bucketInPart, LocalBucket bucketOutPart, int levelToStoreDistances) {
        if (dm == LocalAbstractObject.UNKNOWN_DISTANCE) {
            throw new RuntimeException("The value of dm has not been initialized yet!!!");
        }

        // Sort items by indexes (items got filled in the call to computeDm())
        Arrays.sort(items, sortByIndex);

        AbstractObjectList<LocalAbstractObject> exclusion = new AbstractObjectList<LocalAbstractObject>();

        // Inspect all objects in the bucketInPart bucket
        //int idx = 0;
        //for (AbstractObjectIterator<LocalAbstractObject> it = bucketInPart.getAllObjects(); it.hasNext(); idx++) {
        AbstractObjectIterator<LocalAbstractObject> it = bucketInPart.getAllObjects();
        int objCount = bucketInPart.getObjectCount();
        for (int idx = 0; idx < objCount; idx++) {
            LocalAbstractObject obj = it.next();
            int part = matchDistance(items[idx].getDistance(), dm, rho);

            // Store the precomputed distance
            if (levelToStoreDistances >= 0) {
                PrecomputedDistancesByLevelFilter flt = obj.getDistanceFilter(PrecomputedDistancesByLevelFilter.class);
                flt.addPrecompDist(levelToStoreDistances, items[idx].getDistance());
            }

            if (part != ZONE_IN) {
                try {
                    bucketInPart.deleteObject(it.getCurrentObject(),1);
                    it.remove();
                    
                    if (part == ZONE_OUT) {
                        // Add the object to the bucket bucketOutPart
                        try {
                            bucketOutPart.addObject(obj);
                        } catch (BucketStorageException ex) {
                            Logger.getLogger(ExcludedMiddlePartitioning.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        // Add objects in the exclusion zone the exclusionList.
                        exclusion.add(obj);
                    }
                } catch (BucketStorageException ex) {
                    Logger.getLogger(ExcludedMiddlePartitioning.class.getName()).log(Level.SEVERE, null, ex);
                }
                
            }
        }

        return exclusion;
    }

    /**
     * Returns an integer value identifying a zone to which an object falls. The
     * object is not passed here, as a substitute its distance from a pivot is
     * used.
     *
     * @param dist Distance from an object to a pivot
     * @param dm Current dm value
     * @param rho Current rho value
     * @return either {@link #ZONE_IN}, {@link #ZONE_OUT} or
     * {@link #ZONE_EXCLUSION}.
     */
    public static int matchDistance(float dist, float dm, float rho) {
        if (dm < 0) {
            throw new RuntimeException("matchDistance: dm cannot be negative!");
        }
        if (dist <= dm - rho) {
            return ZONE_IN;
        } else if (dist > dm + rho) {
            return ZONE_OUT;
        } else {
            return ZONE_EXCLUSION;
        }
    }

    /**
     * Encapsulates metadata about an object which is stored in the bucket that
     * is about to split.
     *
     * Distance, object's relative position (index) within the bucket and the
     * identification of object's partition is stored.
     */
    public static class PartListItem {

        /**
         * Distance of the object from the pivot
         */
        private float dist;

        /**
         * Index of the object in the bucket
         */
        private int index;

        /**
         * Identification of a partition to which the object belongs
         */
        private int part;

        /**
         * Constructs the object with the passed distance and relative position.
         *
         * @param dist
         * @param index
         */
        public PartListItem(float dist, int index) {
            this.dist = dist;
            this.index = index;
            this.part = ZONE_EXCLUSION;  // Initialially in the exclusion zone
        }

        /**
         * Sets the id of the partition to which the object falls.
         *
         * @param id partition id
         */
        public void setPartition(int id) {
            this.part = id;
        }

        /**
         * Returns the assign identification of the partitions to which the
         * object belongs.
         *
         * @return partitiong id
         */
        public int getPartition() {
            return part;
        }

        /**
         * Returns the assign distance of the object.
         *
         * @return value of distance
         */
        public float getDistance() {
            return dist;
        }
    }

    /**
     * Comparator for the PartListItem instances. Sorts them by the distance. If
     * the distance is the same their unique index is used.
     */
    public static class PartListItemCompareByDistance implements Comparator<PartListItem> {

        /**
         * Compares two items based on their distance. If they are at the same
         * distance, their indexes are compared.
         *
         * @param i1 first item
         * @param i2 second item
         * @return <code>-1</code> if <code>i1</code> is less than
         * <code>i2</code>, <code>0</code> if <code>i1</code> is the same is
         * <code>i2</code>, and <code>1</code> if <code>i1</code> is greater
         * than <code>i2</code>.
         */
        public int compare(PartListItem i1, PartListItem i2) {
            if (i1.dist < i2.dist) {
                return -1;
            }
            if (i1.dist > i2.dist) {
                return 1;
            }
            // Distances are the same, compare indexes
            return sortByIndex.compare(i1, i2);
        }

    }

    /**
     * Comparator for the PartListItem instances. Sorts them by their unique
     * indexes.
     */
    public static class PartListItemCompareByIndex implements Comparator<PartListItem> {

        /**
         * Compares two items based on their relative positions that are unique.
         *
         * @param i1 first item
         * @param i2 second item
         * @return <code>-1</code> if <code>i1</code> is less than
         * <code>i2</code>, <code>0</code> if <code>i1</code> is the same is
         * <code>i2</code>, and <code>1</code> if <code>i1</code> is greater
         * than <code>i2</code>.
         */
        public int compare(PartListItem i1, PartListItem i2) {
            if (i1.index < i2.index) {
                return -1;
            }
            if (i1.index > i2.index) {
                return 1;
            }
            // This should not happen since indexes are unique!
            return 0;
        }

    }
}
