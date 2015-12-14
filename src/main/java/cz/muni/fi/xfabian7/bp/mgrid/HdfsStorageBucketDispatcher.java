/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import messif.buckets.BucketDispatcher;
import messif.buckets.BucketStorageException;
import messif.buckets.LocalBucket;

/**
 * This class is a Bucket Dipatcher for the HdfsStorageBucket class
 * Allows to create bucket using the String path prefix for the file and an int 
 * to specific the file 
 * 
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic\
 */
public class HdfsStorageBucketDispatcher extends BucketDispatcher{

    public HdfsStorageBucketDispatcher(int maxBuckets, long bucketCapacity, long bucketSoftCapacity, long bucketLowOccupation, boolean bucketOccupationAsBytes, Class<? extends LocalBucket> defaultBucketClass) {
        super(maxBuckets, bucketCapacity, bucketSoftCapacity, bucketLowOccupation, bucketOccupationAsBytes, defaultBucketClass);
    }
    
    public HdfsStorageBucket createBucket (Integer addr) throws BucketStorageException {
        Map<String, Object> addrMap = new HashMap<>();
        addrMap.put("HashMapAddress",addr);
        System.out.println("creating bucket");
        return (HdfsStorageBucket) createBucket(defaultBucketClass, bucketCapacity, bucketSoftCapacity, bucketLowOccupation, bucketOccupationAsBytes, addrMap);
    }   

    @Override
    public void removeBucket(int bucketID) throws NoSuchElementException {
        System.out.println("removing bucket");
        super.removeBucket(bucketID);
        
    }

    @Override
    public synchronized LocalBucket removeBucket(int bucketID, boolean destroyBucket) throws NoSuchElementException {
        System.out.println("removing bucket, destroy");
        return super.removeBucket(bucketID, destroyBucket);
    }

    @Override
    public synchronized LocalBucket addBucket(LocalBucket bucket) throws IllegalStateException, BucketStorageException {
        System.out.println("adding bucket");
        return super.addBucket(bucket);
    }
    
    
    
}
