package org.qcri.rheem.java.platform;

import org.qcri.rheem.basic.function.StringReverseDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.plugin.Activator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Dummy executor for the Java platform.
 */
public class JavaExecutor implements Executor {

    public static final Executor.Factory FACTORY = () -> new JavaExecutor();

    public FunctionCompiler compiler = new FunctionCompiler();

    public JavaExecutor() {
        this.compiler.registerMapFunction(
                StringReverseDescriptor.class,
                string -> {
                    StringBuilder sb = new StringBuilder();
                    for (int i = string.length() - 1; i >= 0; i--) {
                        sb.append(string.charAt(i));
                    }
                    return sb.toString();
                }
        );
    }

    @Override
    public void evaluate(ExecutionOperator executionOperator) {
        if (!executionOperator.isSink()) {
            throw new IllegalArgumentException("Cannot evaluate execution operator: it is not a sink");
        }

        if (!(executionOperator instanceof JavaExecutionOperator)) {
            throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                    "Execution plan contains non-Java operator %s.", executionOperator));
        }

        evaluate0((JavaExecutionOperator) executionOperator);
    }

    private Stream[] evaluate0(JavaExecutionOperator operator) {
        // Resolve all the input streams for this operator.
        Stream[] inputStreams = new Stream[operator.getNumInputs()];
        for (int i = 0; i < inputStreams.length; i++) {
            final OutputSlot outputSlot = operator.getInput(i).getOccupant();
            if (outputSlot == null) {
                throw new IllegalStateException("Cannot evaluate execution operator: There is an unsatisfied input.");
            }

            final Operator inputOperator = outputSlot.getOwner();
            if (!(inputOperator instanceof JavaExecutionOperator)) {
                throw new IllegalStateException(String.format("Cannot evaluate execution operator: " +
                        "Execution plan contains non-Java operator %s.", inputOperator));
            }

            Stream[] outputStreams = evaluate0((JavaExecutionOperator) inputOperator);
            int outputSlotIndex = 0;
            for (; outputSlot != inputOperator.getOutput(outputSlotIndex); outputSlotIndex++) ;
            inputStreams[i] = outputStreams[outputSlotIndex];
        }

        return operator.evaluate(inputStreams, this.compiler);
    }

}
