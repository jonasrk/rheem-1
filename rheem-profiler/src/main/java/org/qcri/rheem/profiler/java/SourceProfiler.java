package org.qcri.rheem.profiler.java;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.operators.JavaCollectionSource;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.operators.JavaTextFileSource;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} implementation for sinks.
 */
public abstract class SourceProfiler extends OperatorProfiler {

    private JavaChannelInstance outputChannelInstance;

    public SourceProfiler(Supplier<JavaExecutionOperator> operatorGenerator, Supplier<?>... dataQuantumGenerators) {
        super(operatorGenerator, dataQuantumGenerators);
    }

    @Override
    public void prepare(long dataQuantaSize,long... inputCardinalities) {
        Validate.isTrue(inputCardinalities.length == 1);

        try {
            this.setUpSourceData(inputCardinalities[0]);
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).error(
                    String.format("Failed to set up source data for input cardinality %d.", inputCardinalities[0]),
                    e
            );
        }

        super.prepare(dataQuantaSize, inputCardinalities);

        // Channel creation separation between operators that requires Collection channels vs Stream channels for the evaluation.
        List operatorsWithCollectionInput = new ArrayList<Class<Operator>>();

        // List of operators requiring collection channels.
        operatorsWithCollectionInput.addAll(Arrays.asList(JavaTextFileSource.class));

        if (operatorsWithCollectionInput.contains(this.operator.getClass())) {
            this.outputChannelInstance = createChannelInstance();
        } else {
            this.outputChannelInstance = createCollectionChannelInstance();
        }
    }

    abstract void setUpSourceData(long cardinality) throws Exception;

    @Override
    protected long executeOperator() {
        this.evaluate(
                new JavaChannelInstance[]{},
                new JavaChannelInstance[]{this.outputChannelInstance}
        );
        return this.outputChannelInstance.provideStream().count();
    }

}
