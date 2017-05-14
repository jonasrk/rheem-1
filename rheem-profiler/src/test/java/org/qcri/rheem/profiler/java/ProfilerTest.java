package org.qcri.rheem.profiler.java;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for Java profiler
 */
public class ProfilerTest {

    @Test
    public void testJavaProfiler(){
        String operator = "reduce";
        String cardinalities = "100";
        String dataQuataSize = "1,100,1000,10000";
        String[] input = {operator, cardinalities, dataQuataSize};
        Profiler.main(input);
    }
}
