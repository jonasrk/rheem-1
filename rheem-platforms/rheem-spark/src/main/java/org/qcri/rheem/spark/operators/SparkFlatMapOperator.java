package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Spark implementation of the {@link FlatMapOperator}.
 */
public class SparkFlatMapOperator<InputType, OutputType>
        extends FlatMapOperator<InputType, OutputType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param functionDescriptor
     */
    public SparkFlatMapOperator(DataSetType inputType, DataSetType outputType,
                                FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final FlatMapFunction<InputType, OutputType> flatMapFunction =
                compiler.compile(this.functionDescriptor, this, inputs);

        final JavaRDD<InputType> inputRdd = input.provideRdd();
        final JavaRDD<OutputType> outputRdd = inputRdd.flatMap(flatMapFunction);
        output.accept(outputRdd, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 600 * inputCards[0] + 600 * outputCards[0] + 1000000000L),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> Math.round(0.2d * inputCards[0] + 2000)),
                0.07d,
                420
        );

        return Optional.of(mainEstimator);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }
}
