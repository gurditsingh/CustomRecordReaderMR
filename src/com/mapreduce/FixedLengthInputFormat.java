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

/**
 * FixedLengthInputFormat is an input format which can be used
 * for input files which contain fixed length records with NO
 * delimiters and NO carriage returns (CR, LF, CRLF) etc. Such
 * files typically only have one gigantic line and each "record"
 * is of a fixed length, and padded with spaces if the record's actual
 * value is shorter than the fixed length.


 *
 * Users must configure the record length property before submitting
 * any jobs which use FixedLengthInputFormat.


 *
 * myJobConf.setInt("mapreduce.input.fixedlengthinputformat.record.length",[myFixedRecordLength]);


 *
 * This input format overrides <code>computeSplitSize()</code> in order to ensure
 * that InputSplits do not contain any partial records since with fixed records
 * there is no way to determine where a record begins if that were to occur.
 * Each InputSplit passed to the FixedLengthRecordReader will start at the beginning
 * of a record, and the last byte in the InputSplit will be the last byte of a record.
 * The override of <code>computeSplitSize()</code> delegates to FileInputFormat's
 * compute method, and then adjusts the returned split size by doing the following:
 * <code>(Math.floor(fileInputFormatsComputedSplitSize / fixedRecordLength) * fixedRecordLength)</code>
 *
 *


 * This InputFormat returns a FixedLengthRecordReader.


 *
 * Compressed files currently are not supported.
 *
 * @see FixedLengthRecordReader
 *
 * @author bitsofinfo.g (AT) gmail.com
 *
 */
public class FixedLengthInputFormat extends FileInputFormat {

    /**
     * When using FixedLengthInputFormat you MUST set this
     * property in your job configuration to specify the fixed
     * record length.
     *


     *
     * i.e. myJobConf.setInt("mapreduce.input.fixedlengthinputformat.record.length",[myFixedRecordLength]);
     */
    public static final String FIXED_RECORD_LENGTH = "mapreduce.input.fixedlengthinputformat.record.length";

    // our logger reference
    private static final Log LOG = LogFactory.getLog(FixedLengthInputFormat.class);

    // the default fixed record length (-1), error if this does not change
    private int recordLength = -1;

    /**
     * Return the int value from the given Configuration found
     * by the FIXED_RECORD_LENGTH property.
     *
     * @param config
     * @return  int record length value
     * @throws IOException if the record length found is 0 (non-existant, not set etc)
     */
    public static int getRecordLength(Configuration config) throws IOException {
        int recordLength = config.getInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 0);

        // this would be an error
        if (recordLength == 0) {
            throw new IOException("FixedLengthInputFormat requires the Configuration property:" + FIXED_RECORD_LENGTH + " to" +
                    " be set to something > 0. Currently the value is 0 (zero)");
        }

        return recordLength;
    }


    /**
     * Returns a FixedLengthRecordReader instance
     *
     * @inheritDoc
     */
    @Override
    public RecordReader createRecordReader(InputSplit split,
                                           TaskAttemptContext context) throws IOException, InterruptedException {
        return new FixedLengthRecordReader();
    }

    /**
     * @inheritDoc
     */


}
