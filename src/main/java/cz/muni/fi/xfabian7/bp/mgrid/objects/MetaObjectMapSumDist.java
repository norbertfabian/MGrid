package cz.muni.fi.xfabian7.bp.mgrid.objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import messif.objects.LocalAbstractObject;
import messif.objects.impl.MetaObjectMap;
import messif.objects.nio.BinaryInput;
import messif.objects.nio.BinarySerializator;

/**
 * Generic {@link MetaObjectMap} with a specific distance function implementation.
 * The distance function is implemented as a sum of all encapsulated objects with
 * the same names.
 *
 * @author xbatko
 */
public class MetaObjectMapSumDist extends MetaObjectMap {
    /** Serial version of this object used for {@link java.io.Serializable} */
    private final static long serialVersionUID = 1L;

    // All inherited constructors

    public MetaObjectMapSumDist(BinaryInput input, BinarySerializator serializator) throws IOException {
        super(input, serializator);
    }

    public MetaObjectMapSumDist(BufferedReader stream) throws IOException {
        super(stream);
    }

    public MetaObjectMapSumDist(BufferedReader stream, String[] restrictNames) throws IOException {
        super(stream, restrictNames);
    }

    public MetaObjectMapSumDist(BufferedReader stream, Set<String> restrictNames) throws IOException {
        super(stream, restrictNames);
    }

    public MetaObjectMapSumDist(String locatorURI, Map<String, LocalAbstractObject> objects) {
        super(locatorURI, objects);
    }

    public MetaObjectMapSumDist(String locatorURI, Map<String, LocalAbstractObject> objects, boolean cloneObjects) throws CloneNotSupportedException {
        super(locatorURI, objects, cloneObjects);
    }

    /**
     * Implementation of the distance function.
     * The sum of all encapsulated objects with the same names is returned.
     *
     * @param obj the respective metaobject for which to compute the distance
     * @param metaDistances array that contains the distances of respective
     *          encapsulated objects (in the order of {@link #getObjectNames()}
     * @param distThreshold threshold on the distance to avoid its computation
     * @return the sum of all encapsulated objects with the same names
     */
    @Override
    protected float getDistanceImpl(LocalAbstractObject obj, float[] metaDistances, float distThreshold) {
        MetaObjectMapSumDist object = (MetaObjectMapSumDist)obj;
        float dist = 0;
        int i = 0;
        for (String name : getObjectNames()) {
            LocalAbstractObject oLocal = getObject(name);
            LocalAbstractObject oOther = object.getObject(name);
            if (oLocal != null && oOther != null) {
                float d = oLocal.getDistance(oOther);
                dist += d;
                if (metaDistances != null)
                    metaDistances[i] = d;
            }
            i++;
        }
        return dist;
    }

}