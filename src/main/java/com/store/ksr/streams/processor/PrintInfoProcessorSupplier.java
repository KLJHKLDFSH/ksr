package com.store.ksr.streams.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PrintInfoProcessorSupplier implements ProcessorSupplier<Integer,String> {

    private static Logger log = LoggerFactory.getLogger(PrintInfoProcessorSupplier.class);

    @Override
    public Processor<Integer, String> get() {
        return new PrintInfoProcessor();
    }

    static class PrintInfoProcessor extends AbstractProcessor<Integer,String>{

        @Override
        public void process(Integer key, String value) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            log.info("print Info : taskId:{} ,topic:{}, timestamp:{}, partition:{}, key:{}, value:{}",
                    super.context().taskId(),super.context().topic(),sdf.format(new Date(super.context().timestamp())),super.context().partition(), key, value);
        }
    }
}
