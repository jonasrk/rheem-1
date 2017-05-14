package org.qcri.rheem.profiler.java;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.java.operators.*;
import org.qcri.rheem.profiler.data.DataGenerators;

import java.util.*;
import java.util.function.Supplier;

/**
 * Utilities to create {@link OperatorProfiler} instances.
 */
public class OperatorProfilers {

    public static JavaTextFileSourceProfiler createJavaTextFileSourceProfiler(int dataQuantaScale) {
        Configuration configuration = new Configuration();
        return new JavaTextFileSourceProfiler(
                DataGenerators.createRandomStringSupplier(dataQuantaScale+20, dataQuantaScale+40, new Random(42)),
                configuration.getStringProperty("rheem.profiler.datagen.url")
        );
    }

    public static JavaCollectionSourceProfiler createJavaCollectionSourceProfiler(int dataQuantaScale) {
        return new JavaCollectionSourceProfiler(DataGenerators.createRandomIntegerSupplier(new Random(dataQuantaScale + 42)));
    }

    public static UnaryOperatorProfiler createJavaMapProfiler(int dataQuantaScale) {
        return createJavaMapProfiler(
                DataGenerators.createRandomIntegerSupplier(1+dataQuantaScale,50+dataQuantaScale,new Random(42)),
                i -> i,
                Integer.class, Integer.class
        );
    }

    public static <In, Out> UnaryOperatorProfiler createJavaMapProfiler(Supplier<In> dataGenerator,
                                                                        FunctionDescriptor.SerializableFunction<In, Out> udf,
                                                                        Class<In> inClass,
                                                                        Class<Out> outClass
                                                                        ) {
        return new UnaryOperatorProfiler(
                () -> new JavaMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createDefault(outClass),
                        new TransformationDescriptor<>(udf, inClass, outClass)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaFlatMapProfiler(int dataQuantaScale) {
        final Random random = new Random(42);
        return new UnaryOperatorProfiler(
                () -> new JavaFlatMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new FlatMapDescriptor<>(
                                RheemArrays::asList,
                                Integer.class,
                                Integer.class
                        )
                ),
                random::nextInt
        );
    }

    public static <In, Out> UnaryOperatorProfiler createJavaFlatMapProfiler(Supplier<In> dataGenerator,
                                                                            FunctionDescriptor.SerializableFunction<In, Iterable<Out>> udf,
                                                                            Class<In> inClass,
                                                                            Class<Out> outClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaFlatMapOperator<>(
                        DataSetType.createDefault(inClass),
                        DataSetType.createDefault(outClass),
                        new FlatMapDescriptor<>(udf, inClass, outClass)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaFilterProfiler(long dataQuantaSize) {
        final Random random = new Random(49);
        return new UnaryOperatorProfiler(
                () -> new JavaFilterOperator<>(
                        DataSetType.createDefault(Integer.class),
                        new PredicateDescriptor<>(i -> (i & 1) == 0, Integer.class)
                ),
                random::nextInt
        );
    }


    public static UnaryOperatorProfiler createJavaReduceByProfiler(int dataQuantaScale) {
        return createJavaReduceByProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.01, new Random(), 4 + dataQuantaScale, 20 + dataQuantaScale),
                String::new,
                (s1, s2) -> s1,
                String.class,
                String.class
        );
    }

    public static <In, Key> UnaryOperatorProfiler createJavaReduceByProfiler(Supplier<In> dataGenerator,
                                                                             FunctionDescriptor.SerializableFunction<In, Key> keyUdf,
                                                                             FunctionDescriptor.SerializableBinaryOperator<In> udf,
                                                                             Class<In> inOutClass,
                                                                             Class<Key> keyClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaReduceByOperator<>(
                        DataSetType.createDefault(inOutClass),
                        new TransformationDescriptor<>(keyUdf, inOutClass, keyClass),
                        new ReduceDescriptor<>(udf, inOutClass)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaGlobalReduceProfiler(int dataQuantaScale) {
        return createJavaGlobalReduceProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.0, new Random(), 4, 20),
                (s1, s2) -> s1,
                String.class
        );
    }

    public static <In> UnaryOperatorProfiler createJavaGlobalReduceProfiler(Supplier<In> dataGenerator,
                                                                             FunctionDescriptor.SerializableBinaryOperator<In> udf,
                                                                             Class<In> inOutClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaGlobalReduceOperator<>(
                        DataSetType.createDefault(inOutClass),
                        new ReduceDescriptor<>(udf, inOutClass)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaMaterializedGroupByProfiler(int dataQuantaScale) {
        return createJavaMaterializedGroupByProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.0, new Random(), 4, 20),
                String::new,
                String.class,
                String.class
        );
    }

    public static <In, Key> UnaryOperatorProfiler createJavaMaterializedGroupByProfiler(Supplier<In> dataGenerator,
                                                                             FunctionDescriptor.SerializableFunction<In, Key> keyUdf,
                                                                             Class<In> inOutClass,
                                                                             Class<Key> keyClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaMaterializedGroupByOperator<>(
                        new TransformationDescriptor<>(keyUdf, inOutClass, keyClass),
                        DataSetType.createDefault(inOutClass),
                        DataSetType.createDefaultUnchecked(Iterable.class)
                ),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaCountProfiler(int dataQuantaScale) {
        return createJavaCountProfiler(DataGenerators.createRandomIntegerSupplier(new Random()), Integer.class);
    }

    public static <T> UnaryOperatorProfiler createJavaCountProfiler(Supplier<T> dataGenerator,
                                                                    Class<T> inClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaCountOperator<>(DataSetType.createDefault(inClass)),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaDistinctProfiler(int dataQuantaScale) {
        return createJavaDistinctProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.0, new Random(), 4, 20),
                String.class
        );
    }

    public static <T> UnaryOperatorProfiler createJavaDistinctProfiler(Supplier<T> dataGenerator, Class<T> inClass) {
        return new UnaryOperatorProfiler(
                () -> new JavaDistinctOperator<>(DataSetType.createDefault(inClass)),
                dataGenerator
        );
    }

    public static UnaryOperatorProfiler createJavaSortProfiler(int dataQuantaScale) {
        return createJavaSortProfiler(
                DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.0, new Random(), 4, 20),
                String.class
        );
    }

    public static <T> UnaryOperatorProfiler createJavaSortProfiler(Supplier<T> dataGenerator, Class<T> inClass) {
        return new UnaryOperatorProfiler(() -> new JavaSortOperator<>(new TransformationDescriptor<>(in->in, inClass, inClass),
                DataSetType.createDefault(inClass)), dataGenerator);
    }

    public static BinaryOperatorProfiler createJavaJoinProfiler(int dataQuantaScale) {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.3;
        final Random random = new Random();
        final int minLen = 4, maxLen = 6;
        Supplier<String> reservoirStringSupplier = DataGenerators.createReservoirBasedStringSupplier(stringReservoir, reuseProbability, random, minLen, maxLen);
        return new BinaryOperatorProfiler(
                () -> new JavaJoinOperator<>(
                        DataSetType.createDefault(String.class),
                        DataSetType.createDefault(String.class),
                        new TransformationDescriptor<>(
                                String::new,
                                String.class,
                                String.class
                        ),
                        new TransformationDescriptor<>(
                                String::new,
                                String.class,
                                String.class
                        )
                ),
                reservoirStringSupplier,
                reservoirStringSupplier
        );
    }

    /**
     * Creates a {@link BinaryOperatorProfiler} for the {@link JavaCartesianOperator} with {@link Integer} data quanta.
     */
    public static BinaryOperatorProfiler createJavaCartesianProfiler(int dataQuantaScale) {
        return new BinaryOperatorProfiler(
                () -> new JavaCartesianOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class)
                ),
                DataGenerators.createRandomIntegerSupplier(new Random()),
                DataGenerators.createRandomIntegerSupplier(new Random())
        );
    }

    public static BinaryOperatorProfiler createJavaUnionProfiler(int dataQuantaScale) {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.0;
        final Random random = new Random();
        Supplier<String> reservoirStringSupplier = DataGenerators.createReservoirBasedStringSupplier(stringReservoir, reuseProbability, random, 4, 6);
        return new BinaryOperatorProfiler(
                () -> new JavaUnionAllOperator<>(DataSetType.createDefault(String.class)),
                reservoirStringSupplier,
                reservoirStringSupplier
        );
    }

    public static SinkProfiler createJavaLocalCallbackSinkProfiler(int dataQuantaScale) {
        return new SinkProfiler(
                () -> new JavaLocalCallbackSink<>(obj -> {
                }, DataSetType.createDefault(Integer.class)),
                DataGenerators.createRandomIntegerSupplier(new Random())
        );
    }

    public static <T> SinkProfiler createCollectingJavaLocalCallbackSinkProfiler(int dataQuantaScale) {
        Collection<T> collector = new LinkedList<>();
        return new SinkProfiler(
                () -> new JavaLocalCallbackSink<>(collector::add, DataSetType.createDefault(Integer.class)),
                DataGenerators.createRandomIntegerSupplier(new Random())
        );
    }

}
