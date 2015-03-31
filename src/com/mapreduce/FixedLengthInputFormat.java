package com.mapreduce;

/**
 * Created by gurdit on 29/3/15.
 */
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class FixedLengthInputFormat extends FileInputFormat {

  
    public static final String FIXED_RECORD_LENGTH = "mapreduce.input.fixedlengthinputformat.record.length";


    private static final Log LOG = LogFactory.getLog(FixedLengthInputFormat.class);

 
    private int recordLength = -1;

    public static int getRecordLength(Configuration config) throws IOException {
        int recordLength = config.getInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 0);

        // this would be an error
        if (recordLength == 0) {
            throw new IOException("FixedLengthInputFormat requires the Configuration property:" + FIXED_RECORD_LENGTH + " to" +
                    " be set to something > 0. Currently the value is 0 (zero)");
        }

        return recordLength;
    }


  
    @Override
    public RecordReader createRecordReader(InputSplit split,
                                           TaskAttemptContext context) throws IOException, InterruptedException {
        return new FixedLengthRecordReader();
    }



}
