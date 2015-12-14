/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid.dindex;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import messif.buckets.BucketStorageException;
import messif.buckets.LocalBucket;
import messif.objects.LocalAbstractObject;
import messif.statistics.StatisticCounter;
import cz.muni.fi.xfabian7.bp.mgrid.HdfsStorageBucketDispatcher;

/**
 *
 * @author Vlastislav Dohnal, dohnal@fi.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic\
 * @since 2008-05-21
 */
public abstract class Hashing implements Serializable {

    /**
     * Class serial id for serialization
     */
    private static final long serialVersionUID = 1L;
    /**
     * A global instance of BucketDispatcher shared within one instance of
     * aD-index
     */
    HdfsStorageBucketDispatcher disp = null;
    /**
     * implicit value of rho used if not given in method parameters
     */
    protected final float rho;
    /**
     * list of pivots used for partitioning
     */
    protected LocalAbstractObject[] pivots = null;
    /**
     * the number of pivots used in the array pivots
     */
    protected int pivotsUsed = 0;

    /**
     * Global counter for distance computations made during address computation
     */
    protected static final StatisticCounter counterAddressDistanceComputations = StatisticCounter.getStatistics("DistanceComputations.Address");
    /**
     * Global counter for distance computations
     */
    protected static final StatisticCounter counterDistanceComputations = StatisticCounter.getStatistics("DistanceComputations");

    /**
     * Creates a new instance of Hashing.
     *
     * This initializes only the <code>dist</code> and <code>rho</code> members.
     * The others are left untouched.
     *
     * @param disp instance of bucket dispatcher used to allocate new buckets
     * @param rho value of the separation coefficient
     */
    public Hashing(HdfsStorageBucketDispatcher disp, float rho) throws BucketStorageException, InstantiationException {
        this.disp = disp;
        this.rho = rho;
    }

    //*******************************************************************
    //************************* ADDRESSING METHODS **********************
    //*******************************************************************
    /**
     * Evaluates distance from the passed object to all pivots of this level of
     * hashing. Before computing any distance, presence of the precomputed
     * distance is tested. If there are some for the given level of hashing,
     * nothing is done.
     *
     * @param obj object to which distances from pivots will be computed and
     * stored
     * @param hashLevel index of level of hashing -- used for obtaining a
     * correct precomputed distances filter
     */
    public abstract void computeDistances(LocalAbstractObject obj, int hashLevel);

    /**
     * Calculates the address of a bucket where the passed object should be
     * stored.
     *
     * @param obj object for which we want to get bucket address
     * @param rho rho used in ball split
     * @param stopEarly stops computing the bucket's address right after the
     * exclusion zone is hit (may save some distance evaluations)
     * @param hashLevel index of the level of aD-index hashing
     * @return an address of the bucket. If exclusion zone is hit,
     * EXCLUSION_ZONE value is returned.
     */
    public abstract int computeAddress(LocalAbstractObject obj, float rho,
            boolean stopEarly, int hashLevel);

    /**
     * Calculates the address of a bucket where the passed object should be
     * stored. Uses the value of <code>rho</code> assigned in the constructor.
     *
     * @param obj An object for which we want to get bucket address
     * @param stopEarly Stops computing the bucket's address right after the
     * exclusion zone is hit (may save some distance evaluations)
     * @param hashLevel index of the level of aD-index hashing
     * @return an address of the bucket. If exclusion zone is hit, the address
     * is {@link ExcludedMiddlePartitioning#ZONE_EXCLUSION EXCLUSION_ZONE}.
     */
    public int computeAddress(LocalAbstractObject obj, boolean stopEarly, int hashLevel) {
        return computeAddress(obj, rho, stopEarly, hashLevel);
    }

    /**
     * Calculates the addresses of all buckets that collide with a query region.
     * In fact, no query region is passed because it is simulated by varying the
     * value of rho. If the passed object falls into an exclusion zone, it is
     * assumed that it intersects all the separable zones (so all combinations
     * are generated).
     *
     * @param obj object for which we want to get bucket address
     * @param rho rho used in ball split
     * @param hashLevel index of the level of aD-index hashing
     * @return addresses of the all buckets that should be searched.
     */
    public abstract int[] computeAddressList(LocalAbstractObject obj, float rho, int hashLevel);

    /**
     * Retrieves an interator over all available bucket addresses.
     */
    public abstract Iterator<Integer> getAllAddresses();

    /**
     *
     * @param addr
     * @return an instance of the bucket corresponding to the passed address.
     * @throws NoSuchElementException is thrown if the <code>addr</code> is not
     * valid.
     */
    public abstract LocalBucket getBucket(int addr) throws NoSuchElementException;

    //*******************************************************************
    //************************* SPLIT METHODS ***************************
    //*******************************************************************
    /**
     * Returns a pivot used for the split of the bucket having the address
     * <code>addr</code>.
     *
     * @param addr address of the bucket to be split
     * @return an instance of pivot which must be used to split the bucket with
     * <code>addr</code>. If such a pivot is not defined, <code>null</code> is
     * returned.
     * @throws NoSuchElementException is thrown if the bucketToSplit value does
     * not refer to any bucket.
     */
    public abstract LocalAbstractObject getSplitPivot(int addr) throws NoSuchElementException;

    /**
     * Select a new pivot which will be used for hashing
     *
     * @return the new pivot, null if the new pivot cannot be selected.
     */
    public abstract LocalAbstractObject selectNewPivot();

    /**
     *
     * @param addr
     * @param newPivot
     * @param newDm
     * @return the address of the new bucket. If a new pivot has not been given
     * and is need for split, -1 is returned.
     * @throws NoSuchElementException is thrown if the addr value does not refer
     * to any bucket.
     * @throws CapacityFullException
     * @throws InstantiationException
     */
    public abstract int split(int addr, LocalAbstractObject newPivot, float newDm) throws NoSuchElementException, BucketStorageException, InstantiationException;

    //*******************************************************************
    //************************* INTERNAL METHODS ************************
    //*******************************************************************
    
    /**
     * Adds a new pivot to the pivots array
     *
     * @param newPivot the pivot to store
     */
    protected void addPivot(LocalAbstractObject newPivot) {
        if (pivots == null) {
            pivots = new LocalAbstractObject[4];
        } else if (pivots.length == pivotsUsed) {
            pivots = Arrays.copyOf(pivots, pivotsUsed + 4);
            // Add the new dm value
        }
        pivots[pivotsUsed++] = newPivot;
    }
    
    //*******************************************************************
    //************************* INFO METHODS ****************************
    //*******************************************************************

    @Override
    public String toString() {
        StringBuffer rtv = new StringBuffer();

        rtv.append("pivots: ").append(pivotsUsed).append("\n");
        rtv.append("objects: ").append(getObjectCount()).append("\n");
        rtv.append("buckets: ").append(getBucketCount()).append("\n");
        rtv.append("occupation: ").append((float)getObjectCount()/(float)getBucketCount()).append("\n");

        return rtv.toString();
    }

    /**
     * Returns an address of a bucket in fancy formatting.
     * 
     * @param addr a bucket's address
     * @return fancy formatted the bucket's address
     */
    public abstract String formatBucketAddress(int addr);

    /** Return the number of objects stored in this hashing
     *
     * @return the number of objects
     */
    public abstract int getObjectCount();

    /** Return the number of buckets used in this hashing
     *
     * @return the number of buckets
     */
    public abstract int getBucketCount();
}
