/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package cz.muni.fi.xfabian7.bp.mgrid.objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import messif.objects.LocalAbstractObject;

/**
 * Object that defines metric space domain with a vector of float numbers and
 * a Euclidean (L2) distance function.
 * @author xbatko
 */
public class MyObjectVect extends LocalAbstractObject {
    /** Serial version of this object used for {@link java.io.Serializable} */
    private final static long serialVersionUID = 1L;

    //****************** Attributes ******************//

    /** Data for this object */
    private final float[] values;


    //****************** Constructors ******************//

    /**
     * Creates a new MyObjectVect instance with the provided vector of values
     * and a locator identifier.
     * @param values the vector of float values that represents this object's data
     * @param locatorURI the identifier of this object
     */
    public MyObjectVect(String locatorURI, float... values) {
        super(locatorURI);
        this.values = values;
    }

    /**
     * Creates a new MyObjectVect instance with the provided vector of values.
     * No identifier is set.
     * @param values the vector of float values that represents this object's data
     */
    public MyObjectVect(float... values) {
        this.values = values;
    }

    /**
     * Creates a new MyObjectVect instance by reading the data from a text stream.
     * @param stream the text stream from which to read the data
     * @throws IOException if there was an I/O error reading the data from the stream
     */
    public MyObjectVect(BufferedReader stream) throws IOException {
        String line = readObjectComments(stream);
        // Parse the line by regular expression that takes comma or whitespace as separators
        String[] stringValues = line.split("\\s*[ ,]\\s*");
        this.values = new float[stringValues.length];
        for (int i = 0; i < this.values.length; i++)
            this.values[i] = Float.parseFloat(stringValues[i]);
    }


    //****************** Data access methods ******************//

    /**
     * Returns the data of this object represented by a vector of float numbers.
     * A copy of the internal data is returned, so it can be modified in any way.
     * @return the data of this object
     */
    public float[] getValues() {
        return values.clone();
    }

    @Override
    public int getSize() {
        return values.length * Float.SIZE / 8;
    }

    @Override
    public boolean dataEquals(Object obj) {
        if (obj instanceof MyObjectVect)
            return Arrays.equals(values, ((MyObjectVect)obj).values);
        else
            return false;
    }

    @Override
    public int dataHashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    protected void writeData(OutputStream stream) throws IOException {
        for (int i = 0; i < values.length; i++) {
            if (i > 0)
                stream.write(',');
            stream.write(Float.toString(values[i]).getBytes());
        }
        stream.write('\n');
    }


    //****************** Metric function ******************//

    @Override
    protected float getDistanceImpl(LocalAbstractObject obj, float distThreshold) {
        float[] objValues = ((MyObjectVect)obj).values;
        if (objValues.length != values.length)
            throw new IllegalArgumentException("Different number of dimensions");
        float dist = 0;
        for (int i = 0; i < values.length; i++) {
            float diff = (values[i] - objValues[i]);
            dist += diff*diff;
        }
        return (float)Math.sqrt(dist);
    }


    //****************** Human-readable description of this object ******************//

    @Override
    public String toString() {
        String locator = getLocatorURI();
        if (locator != null)
            return locator + ": " + Arrays.toString(values);
        else
            return Arrays.toString(values);
    }

}