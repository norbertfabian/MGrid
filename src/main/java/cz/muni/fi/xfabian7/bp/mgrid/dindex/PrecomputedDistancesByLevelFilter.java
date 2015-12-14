/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package cz.muni.fi.xfabian7.bp.mgrid.dindex;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import messif.objects.LocalAbstractObject;
import messif.objects.PrecomputedDistancesFilter;
import messif.objects.PrecomputedDistancesFixedArrayFilter;

/**
 * Organizes precomputed distances as a fixed array filter and 
 * separates them by the aD-index's level.
 * 
 * @author Vlastislav Dohnal, dohnal@fi.muni.cz, Faculty of Informatics, Masaryk University, Brno, Czech Republic
 * @since 2008-05-21
 */
public class PrecomputedDistancesByLevelFilter extends PrecomputedDistancesFilter {

    /** Class serial id for serialization */
    private static final long serialVersionUID = 1L;

    /** Array of filters, each separately for each aD-index level. */
    protected PrecomputedDistancesFixedArrayFilter levels[];

    //****************** Constructors ******************/

    /**
     * Creates a new instance of PrecomputedDistancesByLevelFilter
     */
    public PrecomputedDistancesByLevelFilter() {
        levels = null;
    }
    
    
    /**
     * Creates a new instance of PrecomputedDistancesByLevelFilter from a string.
     * The string must be of format "level1;level2;...", where each 'level' is
     * the string of format "dist1 dist2 dist3...". In other words, distances
     * are separated by space and levels by semicolon.
     * 
     * @param distancesString string to create the filter from
     * @throws java.lang.IllegalArgumentException if the string is of inappropriate format
     */
    public PrecomputedDistancesByLevelFilter(String distancesString) throws IllegalArgumentException {
//        String[] distStrings = distancesString.split(" ");
//        precompDist = new float[distStrings.length];
//        try {
//            for (String dist : distStrings) {
//                precompDist[actualSize++] = Float.valueOf(dist);
//            }
//        } catch (NumberFormatException e) {
//            throw new IllegalArgumentException("string must be of format 'dist1 dist2 dist3...': "+distancesString);
//        }
        throw new UnsupportedOperationException("Constructor from String is not supported yet.");
    }

    
    
    //****************** Manipulation methods ******************/

    /** Add the distance at the end of current precomputed distances of
     * the passed level.
     * 
     * @param level index of the level to which the precomputed distance is being added
     * @param  dist distance to append
     * @return The total number of precomputed distances stored as the level.
     */
    public synchronized int addPrecompDist(int level, float dist) {
        return getPrecompDistFilterOfLevel(level).addPrecompDist(dist);
    }

    
    /**
     * Returns the precomputed distance of a specific level by index.
     * 
     *      If there is no distance associated with the index <code>position</code>
     *      or the level filter does not exist, the function returns 
     *      LocalAbstractObject.UNKNOWN_DISTANCE.
     * @param level index of the level from which the distance will be retrieved
     * @param position the index to retrieve the distance from
     * @return 
     */
    public float getPrecompDist(int level, int position) {
        if (levels == null || levels.length <= level ||
            position < 0 || position >= levels[level].getPrecompDistSize())
            return LocalAbstractObject.UNKNOWN_DISTANCE;
        return levels[level].getPrecompDist(position);
    }

    /**
     * Returns an instance of PrecomputedDistancesFilter that carries distances to
     * pivots of the specific <code>level</code> of aD-index hashing.
     * 
     * @param level index of the level of hashing
     * @return instance encapsulating the precomputed distances.
     */
    public PrecomputedDistancesFixedArrayFilter getPrecompDistFilterOfLevel(int level) {
        if (levels == null) {
            levels = new PrecomputedDistancesFixedArrayFilter[1];
            levels[0] = new PrecomputedDistancesFixedArrayFilter();
        }
        if (levels.length <= level) {
            // Resize and allocate new filters
            int origsize = levels.length;
            levels = Arrays.copyOf(levels, level+1);
            for (int i = origsize; i < levels.length; i++)
                levels[i] = new PrecomputedDistancesFixedArrayFilter();
        }
        return levels[level];
    }

    
    //****************** Clonning ******************/

    @Override
    public Object clone() throws CloneNotSupportedException {
        PrecomputedDistancesByLevelFilter rtv = (PrecomputedDistancesByLevelFilter)super.clone();
        if (rtv.levels != null) {
            rtv.levels = new PrecomputedDistancesFixedArrayFilter[levels.length];
            for (int i = 0; i < levels.length; i++)
                rtv.levels[i] = (PrecomputedDistancesFixedArrayFilter)levels[i].clone();
        }
        return rtv;
    }

    //****************** Filtering methods ******************/

    
//    @Override
//    public boolean isGetterSupported() {
//        return false;
//    }
//
//    @Override
//    protected float getPrecomputedDistanceImpl(LocalAbstractObject obj) {
//        throw new UnsupportedOperationException("This precomputed distances filter does not support get precomputed distance by object");
//    }
//
//    @Override
//    protected boolean excludeUsingPrecompDistImpl(PrecomputedDistancesFilter targetFilter, float radius) {
//        try {
//            return excludeUsingPrecompDistImpl((PrecomputedDistancesByLevelFilter)targetFilter, radius);
//        } catch (ClassCastException e) {
//            return false;
//        }
//    }

    /**
     * Implementation of exclusion principle with an instance of this class.
     * 
     * @param targetFilter a filter of another object which might be excluded
     * @param radius       the value of query radius
     * @return <code>true</code> if the object can be excluded 
     * (it is farer than <code>radius</code>, otherwise <code>false</code>.
     */
    protected boolean excludeUsingPrecompDistImpl(PrecomputedDistancesByLevelFilter targetFilter, float radius) {
        if (levels == null || targetFilter.levels == null)
            return false;
        
        final int cnt = Math.min(levels.length, targetFilter.levels.length);
        for (int i = 0; i < cnt; i++) {
            if (levels[i].excludeUsingPrecompDist(targetFilter.levels[i], radius))
                return true;
        }
        return false;
    }

//    @Override
//    protected boolean includeUsingPrecompDistImpl(PrecomputedDistancesFilter targetFilter, float radius) {
//        try {
//            return includeUsingPrecompDistImpl((PrecomputedDistancesByLevelFilter)targetFilter, radius);
//        } catch (ClassCastException e) {
//            return false;
//        }
//    }

    /**
     * Implementation of inclusion principle with an instance of this class.
     * 
     * @param targetFilter a filter of another object which might be included
     * @param radius       the value of query radius
     * @return <code>true</code> if the object can be directly included 
     * (it is closer than <code>radius</code>, otherwise <code>false</code>.
     */
    protected boolean includeUsingPrecompDistImpl(PrecomputedDistancesByLevelFilter targetFilter, float radius) {
        if (levels == null || targetFilter.levels == null)
            return false;
        
        final int cnt = Math.min(levels.length, targetFilter.levels.length);
        for (int i = 0; i < cnt; i++) {
            if (levels[i].includeUsingPrecompDist(targetFilter.levels[i], radius))
                return true;
        }
        return false;
    }

    @Override
    public float getPrecomputedDistance(LocalAbstractObject obj, float[] metaDistances) {
        return LocalAbstractObject.UNKNOWN_DISTANCE;
    }

    @Override
    public boolean excludeUsingPrecompDist(PrecomputedDistancesFilter targetFilter, float radius) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean includeUsingPrecompDist(PrecomputedDistancesFilter targetFilter, float radius) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean addPrecomputedDistance(LocalAbstractObject obj, float distance, float[] metaDistances) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void writeData(OutputStream stream) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean isDataWritable() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
