package cz.muni.fi.xfabian7.bp.mgrid.objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import messif.objects.LocalAbstractObject;
import messif.objects.MetaObject;

/**
 * Specific implementation of the {@link MetaObject} that contains two
 * encapsulated descriptors: a number (represented by {@link MyObject}) and
 * a vector (represented by {@link MyObjectVect}).
 *
 * Distance function is defined as sum of the distance between the numbers and
 * the distance between the vectors.
 *
 * @author xbatko
 */
public class MyMetaObject extends MetaObject {
    /** Serial version of this object used for {@link java.io.Serializable} */
    private final static long serialVersionUID = 1L;

    /** Name of the encapsulated number descriptor */
    private static final String NUMBER_NAME = "number";
    /** Name of the encapsulated vector descriptor */
    private static final String VECTOR_NAME = "vector";
    /** Encapsulated number descriptor */
    private final MyObject number;
    /** Encapsulated vector descriptor */
    private final MyObjectVect vector;

    /**
     * Creates a new instance of MyMetaObject with the given number and vector descriptors
     * and a locator identifier.
     * @param locatorURI the identifier of this object
     * @param number the number descriptor to encapsulate
     * @param vector the vector descriptor to encapsulate
     */
    public MyMetaObject(String locatorURI, MyObject number, MyObjectVect vector) {
        super(locatorURI);
        this.number = number;
        this.vector = vector;
    }

    /**
     * Creates a new instance of MyMetaObject with the given number and vector descriptors.
     * @param number the number descriptor to encapsulate
     * @param vector the vector descriptor to encapsulate
     */
    public MyMetaObject(MyObject number, MyObjectVect vector) {
        this.number = number;
        this.vector = vector;
    }

    /**
     * Creates a new MyMetaObject instance by reading the data from a text stream.
     * @param stream the text stream from which to read the data
     * @throws IOException if there was an I/O error reading the data from the stream
     */
    public MyMetaObject(BufferedReader stream) throws IOException {
        String header = readObjectComments(stream);

        if (header.contains(NUMBER_NAME))
            this.number = new MyObject(stream);
        else
            this.number = null;

        if (header.contains(VECTOR_NAME))
            this.vector = new MyObjectVect(stream);
        else
            this.vector = null;
    }

    @Override
    protected void writeData(OutputStream stream) throws IOException {
        if (number != null)
            stream.write(NUMBER_NAME.getBytes());
        stream.write(',');
        if (vector != null)
            stream.write(VECTOR_NAME.getBytes());
        stream.write('\n');

        if (number != null)
            number.writeData(stream);
        if (vector != null)
            vector.writeData(stream);
    }

    @Override
    public int getObjectCount() {
        return 2;
    }

    @Override
    public LocalAbstractObject getObject(String name) {
        if (name.equals(NUMBER_NAME))
            return number;
        else if (name.equals(VECTOR_NAME))
            return vector;
        return null;
    }

    @Override
    public Collection<String> getObjectNames() {
        return Arrays.asList(NUMBER_NAME, VECTOR_NAME);
    }

    @Override
    public Collection<LocalAbstractObject> getObjects() {
        return Arrays.asList(number, vector);
    }

    @Override
    protected float getDistanceImpl(LocalAbstractObject obj, float[] metaDistances, float distThreshold) {
        MyMetaObject object = (MyMetaObject)obj;
        float dist = 0;

        if (number != null && object.number != null) {
            float d = number.getDistance(object.number);
            dist += d;
            if (metaDistances != null)
                metaDistances[0] = d;
        }

        if (vector != null && object.vector != null) {
            float d = vector.getDistance(object.vector);
            dist += d;
            if (metaDistances != null)
                metaDistances[1] = d;
        }

        return dist;
        
    }
    
    @Override
    public String toString() {
        return ("MyMetaOject ("+getLocatorURI()+") : "+number+" , "+vector);
    }

}