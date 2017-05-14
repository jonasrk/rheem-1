package org.qcri.rheem.profiler.data;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.stream.Collector;

/**
 * Created by migiwara on 10/05/17.
 */
public class DataGenaratorsTest {
    private static DataGenaratorsTest ourInstance = new DataGenaratorsTest();

    public static DataGenaratorsTest getInstance() {
        return ourInstance;
    }


    /**
     * Test the generation of the {@link DataGenerators}
     */
    @Test
    public void DataGenaratorsTest() {
        DataGenerators.Generator<String> dg = DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.0, new Random(), 4, 20);
        Collection<String> tmp = new ArrayList<String>();
        for(int i=0;i<100;i++)
            tmp.add(dg.get());
        System.out.println(tmp.toString());
    }

}
