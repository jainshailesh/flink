package org.apache.flink.cep.operator;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.nfa.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.log4j.Logger;

/**
 * Abstract CEP pattern operator for a keyed input stream, this operator when
 * running in ProcessingTime, passes the timestamps extracted from the events
 * (similar to EventTime) to the NFA (as opposed to passing the current
 * processing time). This is to overcome the limitation that we cannot generate
 * a watermark per key.
 * <p>
 * To leverage this mixed approach, the job needs to run in ProcessingTime,
 * there should be a timestamp assigner upstream, and the elements have to come
 * in ascending order of the timestamps.
 */
public abstract class AbstractKeyedCEPPatternMixedTimeApproachOperator<IN,
        KEY, OUT, F extends Function>
        extends AbstractKeyedCEPPatternOperator<IN,
        KEY, OUT, F> {
    public AbstractKeyedCEPPatternMixedTimeApproachOperator(TypeSerializer
                                                                    inputSerializer, boolean isProcessingTime, NFACompiler.NFAFactory nfaFactory, EventComparator comparator, AfterMatchSkipStrategy afterMatchSkipStrategy, F function) {
        super(inputSerializer, isProcessingTime, nfaFactory, comparator,
                afterMatchSkipStrategy, function);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (isProcessingTime && element.hasTimestamp()) {
            if (comparator == null) {
                NFA<IN> nfa = getNFA();
                //We'll assume for now that the elements will always come in
                // order
                Logger.getRootLogger().debug("Processing element " +
                        element.toString() + "using mixed approach");
                processEvent(nfa, element.getValue(), element.getTimestamp());
                updateNFA(nfa);
            } else {
                //We'll call the parent method for now
                super.processElement(element);
            }
        } else {
            //We'll call the parent method for now
            super.processElement(element);
        }
    }
}
