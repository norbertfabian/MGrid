/*
 * To change this template, choose Tools | Templates
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
import messif.objects.PrecomputedDistancesFixedArrayFilter;
import messif.pivotselection.AbstractPivotChooser;
import cz.muni.fi.xfabian7.bp.mgrid.HdfsStorageBucket;
import cz.muni.fi.xfabian7.bp.mgrid.HdfsStorageBucketDispatcher;
import static cz.muni.fi.xfabian7.bp.mgrid.dindex.DIndex.*;

/**
 * Implementation of a Hashing tree.
 * The directory of indexes to buckets is implemented as a tree. 
 * Each node contains the dm value.
 *
 * @author Vlastislav Dohnal, dohnal@fi.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic\
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic\
 * @since 2015-03-25
 */
public class HashingTree extends Hashing {

    /** Class serial id for serialization */
    private static final long serialVersionUID = 1L;
    /** Global level of extensible hashing */
    protected byte globalLevel;

    /** Length of addrTo* arrays - the number of elements used */
    int addrLen;    
    /** list of buckets IDs and the local level values */
    protected HdfsStorageBucket[] listOfBucketIds;
    /** list of local level values of buckets */
    protected byte[] listOfLocalLevelValuesOfBuckets;
    /** list of leaf nodes corresponding to buckets */
    private Node[] listOfLeafNodesCorrespondingToBuckets;
   

    protected AbstractPivotChooser pivotChooser;
    
    /** Root node of the tree implementation of bucket directory */
    private Node root = null;
    
    
    /** Creates a new instance of ExtensibleHashing.
     *
     * @param disp instance of bucket dispatcher used to allocate new buckets
     * @param rho  value of the separation coefficient
     * @param chooser instance of a pivot chooser used to select new pivots
     * @throws messif.buckets.BucketStorageException
     * @throws java.lang.InstantiationException
     */
    public HashingTree(HdfsStorageBucketDispatcher disp, float rho, AbstractPivotChooser chooser) throws BucketStorageException, InstantiationException {
        super(disp, rho);

        globalLevel = 0;
        
        pivotChooser = chooser;

        // Initialize the addrTo* arrays
        listOfBucketIds = new HdfsStorageBucket[16];
        listOfLocalLevelValuesOfBuckets = new byte[16];
        listOfLeafNodesCorrespondingToBuckets = new Node[16];

        // Allocate the root of the tree directory
        root = new Node(0);
        
        // Allocate one bucket
        addrLen = 1;
        listOfBucketIds[0] = disp.createBucket(pathCounter++);
        pivotChooser.registerSampleProvider(listOfBucketIds[0]);
        listOfLocalLevelValuesOfBuckets[0] = 0;     // The lonely bucket has local level 0.
        listOfLeafNodesCorrespondingToBuckets[0] = root;           // For translation purposes, store pointer to the only leaf node.
        
    }

    static public PrecomputedDistancesByLevelFilter getFilter(LocalAbstractObject obj) {
        // Get the precomputed distances filter
        PrecomputedDistancesByLevelFilter flt = obj.getDistanceFilter(PrecomputedDistancesByLevelFilter.class);
        if (flt == null) {
            flt = new PrecomputedDistancesByLevelFilter();
            obj.chainFilter(flt, true);
        }
        return flt;
    }

    
    protected PrecomputedDistancesFixedArrayFilter getFilterOfLevel(LocalAbstractObject obj, int level) {
        return getFilter(obj).getPrecompDistFilterOfLevel(level);
    }
    
    
    @Override
    public void computeDistances(LocalAbstractObject obj, int hashLevel) {
        PrecomputedDistancesFixedArrayFilter flt = getFilterOfLevel(obj, hashLevel);
        
        // For each level (or pivot)
        for (int l = 0; l < globalLevel; l++) {
            // Compute the distance from the object to the pivot
            float dist = flt.getPrecompDist(l);
            if (dist == LocalAbstractObject.UNKNOWN_DISTANCE) {
                dist = obj.getDistance(pivots[l]);
                // Store the distance among precomputed distances
                flt.addPrecompDist(dist);
            }
        }
    }
    
    
    //*******************************************************************
    //************************* ADDRESSING METHODS **********************
    //*******************************************************************
    
    
    @Override
    public int computeAddress(LocalAbstractObject obj, float rho, boolean stopEarly, int hashLevel) {
        boolean excluded = false;
        Node n = root;

        if (rho < 0)
            rho = 0;
        
        counterAddressDistanceComputations.bindTo(counterDistanceComputations);
        
        // Get the precomputed distances filter
        PrecomputedDistancesByLevelFilter flt = getFilter(obj);
        
        // For each level (or pivot)
        for (int l = 0; l < globalLevel; l++) {
            // Compute the distance from the object to the pivot
            float dist = flt.getPrecompDist(hashLevel, l);
            if (dist == LocalAbstractObject.UNKNOWN_DISTANCE) {
                dist = obj.getDistance(pivots[l]);
                // Store the distance among precomputed distances
                flt.addPrecompDist(l, dist);
            }
            
            if (excluded)
                continue;       // compute the remaining distances only
            
            int zone = ExcludedMiddlePartitioning.matchDistance(dist, n.getDm(), rho);
            if (zone == ExcludedMiddlePartitioning.ZONE_EXCLUSION) {
                excluded = true;
                if (stopEarly)
                    break;
            } else {
                // Update the bucket's address and move to the next node
                if (zone == ExcludedMiddlePartitioning.ZONE_IN) {
                    n = n.getLeft();
                } else {
                    n = n.getRight();
                }
                if (n.isLeaf()) { // Leaf node, address is complete
                    if (l+1 != listOfLocalLevelValuesOfBuckets[n.getBucketIndex()])        // Correctness check
                        throw new RuntimeException("computeAddress: the local level of the bucket's address computed is incorrect!"+
                                                   " LocalLevel:" + listOfLocalLevelValuesOfBuckets[n.getBucketIndex()] + ", ExpectedLevel: "+(l+1));
                    break;
                }
            }
        }
        
        counterAddressDistanceComputations.unbind();
        
        if (excluded)
            return ExcludedMiddlePartitioning.ZONE_EXCLUSION;
        else 
            return n.getBucketIndex();
    }
    
    
    @Override
    public int[] computeAddressList(LocalAbstractObject obj, float rho, int hashLevel) {
        Node arr[] = new Node[64];
        int arrLen = 0;
        int first;
        
        int res[] = new int[16];       // Array of bucket addresses to be returned.
        int resLen = 0;

        if (rho < 0)
            rho = 0;
        
        counterAddressDistanceComputations.bindTo(counterDistanceComputations);
        
        // This method assumes that the passed object has distances to all pivots.
        PrecomputedDistancesByLevelFilter flt = getFilter(obj);
        
        // Initialize list of nodes
        arrLen++;
        arr[0] = root;
        first = 0;
        
        // For each level (or pivot)
        for (int l = 0; l < globalLevel; l++) {
            // Get the distance from the object to the pivot
            float dist = flt.getPrecompDist(hashLevel, l);
            if (dist == LocalAbstractObject.UNKNOWN_DISTANCE) {
                // Evaluate and store the distance if is is missing
                dist = obj.getDistance(pivots[l]);
                flt.addPrecompDist(l, dist);
            }
            
            // Check all current nodes (non-leaf nodes starting at 'first' index)
            for (int idx = arrLen - 1; idx >= first; idx--) {
                if (arr[idx] == null)
                    continue;
                
                // Get zone of the object
                int zone = ExcludedMiddlePartitioning.matchDistance(dist, arr[idx].getDm(), rho);
                
                Node left = arr[idx].getLeft();
                Node right = arr[idx].getRight();

                switch (zone) {
                    case ExcludedMiddlePartitioning.ZONE_IN:
                        // Left branch is followed (if not a leaf)
                        if (left.isLeaf()) {
                            // Add the left leaf to the response
                            if (resLen >= res.length)
                                res = Arrays.copyOf(res, resLen + 16);
                            res[resLen++] = left.getBucketIndex();
                            arr[idx] = null;
                            if (idx == first)
                                first++;
                            if (idx == arrLen-1)
                                arrLen--;
                        } else {
                            // Replace the current node with the left one
                            arr[idx] = left;
                        }
                        break;
                    case ExcludedMiddlePartitioning.ZONE_OUT:
                        // Right branch is followed (if not a leaf)
                        if (right.isLeaf()) {
                            // Add the rihgt leaf to the response
                            if (resLen >= res.length)
                                res = Arrays.copyOf(res, resLen + 16);
                            res[resLen++] = right.getBucketIndex();
                            arr[idx] = null;
                            if (idx == first)
                                first++;
                            if (idx == arrLen-1)
                                arrLen--;
                        } else {
                            // Replace the current node with the right one
                            arr[idx] = right;
                        }
                        break;
                    case ExcludedMiddlePartitioning.ZONE_EXCLUSION:
                        // Both the branches must be followed...
                        if (left.isLeaf()) {
                            // Add the left one to the response (it is a leaf)
                            if (resLen >= res.length)
                                res = Arrays.copyOf(res, resLen + 16);
                            res[resLen++] = left.getBucketIndex();

                            // The left one is leaf node, so replace the current position with the right node.
                            if (right.isLeaf()) {
                                // Right is the leaf as well, so set the current position to null
                                if (resLen >= res.length)
                                    res = Arrays.copyOf(res, resLen + 16);
                                res[resLen++] = right.getBucketIndex();
                                arr[idx] = null;
                                if (idx == first)
                                    first++;
                                if (idx == arrLen-1)
                                    arrLen--;
                            } else
                                arr[idx] = right;
                        } else {
                            // Replace the current node with the left one.
                            arr[idx] = left;

                            // Add the right node if it is not a leaf.
                            if (right.isLeaf()) {
                                if (resLen >= res.length)
                                    res = Arrays.copyOf(res, resLen + 16);
                                res[resLen++] = right.getBucketIndex();
                            } else {
                                // Add the right node to the queue of nodes waiting for processing
                                if (arrLen >= arr.length)
                                    arr = Arrays.copyOf(arr, arrLen + 16);
                                arr[arrLen++] = right;
                            }
                        }
                        break;
                }
            }
        }
        
        counterAddressDistanceComputations.unbind();
        
        // Return the addresses
        if (resLen == 0)
            res[resLen++] = root.getBucketIndex();
        return Arrays.copyOf(res, resLen);
    }
    
    
    @Override
    public LocalBucket getBucket(int addr) throws NoSuchElementException {
        if (addr >= addrLen)
            throw new NoSuchElementException();
        return listOfBucketIds[addr];
    }


    @Override
    public Iterator<Integer> getAllAddresses() {
        return new AddressIterator(addrLen-1);
    }
    
    //*******************************************************************
    //************************* SPLIT METHODS ***************************
    //*******************************************************************
    

    @Override
    public LocalAbstractObject getSplitPivot(int addr) throws NoSuchElementException {
        if (addr >= addrLen)
            throw new NoSuchElementException();
        
        if (pivots == null || listOfLocalLevelValuesOfBuckets[addr] >= pivots.length)
            return null;
        
        // Get the bucket's local level and return the corresponding pivot.
        return pivots[listOfLocalLevelValuesOfBuckets[addr]];
    }

    
    @Override
    public LocalAbstractObject selectNewPivot() {
        return pivotChooser.getNextPivot();
    }

    
    @Override
    public int split(int addr, LocalAbstractObject newPivot, float newDm) throws NoSuchElementException, BucketStorageException, InstantiationException {
        if (addr >= addrLen)
            throw new NoSuchElementException();

        // If the local level is the same as the global level, increment the global level and store the new pivot.
        if (listOfLocalLevelValuesOfBuckets[addr] == globalLevel) {
            // New pivot is required!!!
            if (newPivot == null)
                return -1;
            
            // Store the new pivot
            addPivot(newPivot);
            
            // Increase the global level
            globalLevel++;
        }
        
        // Resize arrays if no room.
        if (addrLen == listOfBucketIds.length) {
            listOfBucketIds = Arrays.copyOf(listOfBucketIds, addrLen+16);
            listOfLocalLevelValuesOfBuckets = Arrays.copyOf(listOfLocalLevelValuesOfBuckets, addrLen+16);
            listOfLeafNodesCorrespondingToBuckets = Arrays.copyOf(listOfLeafNodesCorrespondingToBuckets, addrLen+16);
        }

        // Create the new bucket
        listOfBucketIds[addrLen] = disp.createBucket(pathCounter++);
        pivotChooser.registerSampleProvider(listOfBucketIds[addrLen]);
        
        // Split the leaf node corresponding to the current bucket
        Node curr = listOfLeafNodesCorrespondingToBuckets[addr];        
        Node left = new Node(addr);
        Node right = new Node(addrLen);
        
        curr.changeToInternal(newDm, left, right);
        listOfLeafNodesCorrespondingToBuckets[addr] = left;
        listOfLeafNodesCorrespondingToBuckets[addrLen] = right;

        // Update the level of the bucket being split and the level of the new bucket
        listOfLocalLevelValuesOfBuckets[addr]++;
        listOfLocalLevelValuesOfBuckets[addrLen] = listOfLocalLevelValuesOfBuckets[addr];
        
        // Increment the size (we added a new element) and return the new bucket's address (last index)
        return addrLen++;
    }

    
    
    //*******************************************************************
    //************************* INFO METHODS ****************************
    //*******************************************************************
    

    @Override
    public String toString() {
        StringBuilder rtv = new StringBuilder();
        rtv.append("global level: ").append(globalLevel).append("\n");
        rtv.append(super.toString());

        // Info about buckets used
        rtv.append("object counts:");
        for (int i = 0; i < addrLen; i++) {
            rtv.append(String.format(" %d", listOfBucketIds[i].getObjectCount()));
        }
        rtv.append("\n");
        
        // Info about bucket levels
        rtv.append("bucket levels:");
        for (int i = 0; i < addrLen; i++) {
            rtv.append(" ").append(listOfLocalLevelValuesOfBuckets[i]);
        }
        rtv.append("\n");
        
        // Info about bucket IDs
        rtv.append("bucket ids:");
        for (int i = 0; i < addrLen; i++) {
            rtv.append(" ").append(listOfBucketIds[i].getBucketID());
        }
        rtv.append("\n");
        
        return rtv.toString();
    }

    @Override
    public String formatBucketAddress(int addr) {
        return String.format("'%d:%d'", listOfLocalLevelValuesOfBuckets[addr], addr);
    }

    @Override
    public int getObjectCount() {
        int cnt = 0;
        for (int i = 0; i < addrLen; i++)
            cnt += listOfBucketIds[i].getObjectCount();
        return cnt;
    }

    @Override
    public int getBucketCount() {
        return addrLen;
    }   

    //*******************************************************************
    //************************* INTERNAL METHODS ************************
    //*******************************************************************
    
    /** Internal iterator for returning all bucket addresses available */
    protected class AddressIterator implements Iterator<Integer> {
        /** Current address returned by the iterator */
        private int curr;
        /** Maximum address to be returned by the iterator */
        private int max;
        
        /** Constructor
         * @param max      maximum bucket address available
         */
        public AddressIterator(int max) {
            this.curr = 0;
            this.max = max;
        }

        @Override
        public boolean hasNext() {
            return (curr <= max);
        }

        @Override
        public Integer next() {
            return curr++;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove a bucket address!");
        }
        
    }
    
    
    /** This class encapsulates the directory of extensible hashing implemented as a tree */
    private class Node implements Serializable {
        /** Class serial id for serialization */
        private static final long serialVersionUID = 1L;

        /** Value of dm used to split */
        protected float dm;
        /** Index to the list of buckets */
        protected int idxBucket;
        
        /** Left successor (inner partition) */
        protected Node left;
        /** Right successor (outer partition) */
        protected Node right;
        
        /** Constructor of a list node (Internal nodes are created from a list node) */
        Node(int bucket) {
            dm = -1.0f;
            idxBucket = bucket;
            left = right = null;
        }
        
        /** Changes the list node to an internal node */
        public void changeToInternal(float dm, Node left, Node right) {
            this.dm = dm;
            this.left = left;
            this.right = right;
            this.idxBucket = -1;
        }
        
        /** Gets the bucket index */
        public int getBucketIndex() { return idxBucket; }
        /** Gets the left successor */
        public Node getLeft() { return left; }
        /** Gets the right successor */
        public Node getRight() { return right; }
        /** Returns dm value */
        public float getDm() { return dm; }
        
        /** Is the node leaf?  (Yes, if bucket index is assigned) */
        public boolean isLeaf() { return (idxBucket>=0); }
        
    }
}
