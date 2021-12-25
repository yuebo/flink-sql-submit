package com.eappcat.flink.reporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogJobReporter implements JobReporter {
    private static final Logger logger=LoggerFactory.getLogger(LogJobReporter.class.getName());
    @Override
    public void report(JobReport jobReport) {
        logger.info("reporter: {}",jobReport);
    }
}
