package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Version of {@link AbstractKeyedCEPPatternMixedTimeApproachOperator} that
 * applies given {@link PatternSelectFunction} to fully matched event
 * patterns.
 *
 * @param <IN>  Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
public class SelectCepOperatorMixedTimeApproach<IN, KEY, OUT>
        extends AbstractKeyedCEPPatternMixedTimeApproachOperator
                        <IN, KEY, OUT, PatternSelectFunction<IN, OUT>> {

    public SelectCepOperatorMixedTimeApproach
            (TypeSerializer inputSerializer, boolean isProcessingTime,
             NFACompiler.NFAFactory nfaFactory, EventComparator comparator,
             AfterMatchSkipStrategy afterMatchSkipStrategy,
             PatternSelectFunction<IN, OUT> function) {
        super(inputSerializer, isProcessingTime, nfaFactory, comparator,
                afterMatchSkipStrategy, function);
        Logger.getRootLogger().info("Creating " +
                "SelectCepOperatorMixedTimeApproach operator.");
    }

    //Copied from SelectCepOperator
    @Override
    protected void processMatchedSequences(Iterable<Map<String, List<IN>>>
                                                       matchingSequences,
                                           long timestamp) throws
            Exception {
        for (Map<String, List<IN>> match : matchingSequences) {
            output.collect(new StreamRecord<>(getUserFunction().select(match), timestamp));
        }
    }
}
