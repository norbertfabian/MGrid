package cz.muni.fi.xfabian7.bp.mgrid.objects;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import messif.objects.LocalAbstractObject;

/**
 * Object that defines metric space domain with one float number and
 * distance based on absolute difference of the two numbers.
 * @author xbatko
 */
public class MyObject extends LocalAbstractObject {
    /** Serial version of this object used for {@link java.io.Serializable} */
    private final static long serialVersionUID = 1L;

    //****************** Attributes ******************//

    /** Data for this object */
    private final float value;


    //****************** Constructors ******************//

    /**
     * Creates a new MyObject instance with the provided value and a locator identifier.
     * @param locatorURI the identifier of this object
     * @param value the value of this object's data
     */
    public MyObject(String locatorURI, float value) {
        super(locatorURI);
        this.value = value;
    }

    /**
     * Creates a new MyObject instance with the provided value.
     * No identifier is set.
     * @param value the value of the number
     */
    public MyObject(float value) {
        this.value = value;
    }

    /**
     * Creates a new MyObject instance by reading the data from a text stream.
     * @param stream the text stream from which to read the data
     * @throws IOException if there was an I/O error reading the data from the stream
     */
    public MyObject(BufferedReader stream) throws IOException {
        String line = readObjectComments(stream);
        this.value = Float.parseFloat(line);
    }


    //****************** Data access methods ******************//

    /**
     * Returns the data of this object represented by a single float number.
     * @return the data of this object
     */
    public float getValue() {
        return value;
    }

    @Override
    public int getSize() {
        return Float.SIZE / 8;
    }

    @Override
    public boolean dataEquals(Object obj) {
        if (obj instanceof MyObject)
            return value == ((MyObject)obj).value;
        else
            return false;
    }

    @Override
    public int dataHashCode() {
        return (int)value;
    }

    @Override
    protected void writeData(OutputStream stream) throws IOException {
        stream.write(Float.toString(value).getBytes());
        stream.write('\n');
    }


    //****************** Metric function ******************//

    @Override
    protected float getDistanceImpl(LocalAbstractObject obj, float distThreshold) {
        return Math.abs(value - ((MyObject)obj).value);
    }


    //****************** Human-readable description of this object ******************//

    @Override
    public String toString() {
        String locator = getLocatorURI();
        if (locator != null)
            return locator + ": " + value;
        else
            return Float.toString(value);
    }

}