package com.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class FixedLengthRecordReader extends RecordReader {


    private static final Log LOG = LogFactory.getLog(FixedLengthRecordReader.class);


    private long splitStart;


    private long splitEnd;

    // our current position in the split
    private long currentPosition;

    // the length of a record
    private int recordLength;

    // reference to the input stream
    private FSDataInputStream fileInputStream;

    // the input byte counter
    private Counter inputByteCounter;

    // reference to our FileSplit
    private FileSplit fileSplit;

    // our record key (byte position)
    private LongWritable recordKey = null;

    // the record value
    private Text recordValue = null;

    @Override
    public void close() throws IOException {
        if (fileInputStream != null) {
            fileInputStream.close();
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return recordKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return recordValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (splitStart == splitEnd) {
            return (float)0;
        } else {
            return Math.min((float)1.0, (currentPosition - splitStart) / (float)(splitEnd - splitStart));
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {


        this.fileSplit = (FileSplit)inputSplit;


        splitStart = fileSplit.getStart();

        splitEnd = splitStart + fileSplit.getLength();


        LOG.info("FixedLengthRecordReader: SPLIT START="+splitStart + " SPLIT END=" +splitEnd + " SPLIT LENGTH="+fileSplit.getLength() );

        Path file = fileSplit.getPath();

        Configuration job = context.getConfiguration();

        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        if (codec != null) {
            throw new IOException("FixedLengthRecordReader does not support reading compressed files");
        }

        inputByteCounter = ((MapContext)context).getCounter("FileInputFormatCounters", "BYTES_READ");

      
        this.recordLength = FixedLengthInputFormat.getRecordLength(job);


        final FileSystem fs = file.getFileSystem(job);


        fileInputStream = fs.open(file,(64 * 1024));

        fileInputStream.seek(splitStart);

        this.currentPosition = splitStart;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (recordKey == null) {
            recordKey = new LongWritable();
        }

        // the Key is always the position the record starts at
        recordKey.set(currentPosition);

        // the recordValue to place the record text in
        if (recordValue == null) {
            recordValue = new Text();
        } else {
            recordValue.clear();
        }

        // if the currentPosition is less than the split end..
        if (currentPosition < splitEnd) {

            // setup a buffer to store the record
            byte[] buffer = new byte[this.recordLength];
            int totalRead = 0; // total bytes read
            int totalToRead = recordLength; // total bytes we need to read

            // while we still have record bytes to read
            while(totalRead != recordLength) {
                // read in what we need
                int read = this.fileInputStream.read(buffer, 0, totalToRead);

                // append to the buffer
                recordValue.append(buffer,0,read);

                // update our markers
                totalRead += read;
                totalToRead -= read;
             
            }


            currentPosition = currentPosition +recordLength;
            inputByteCounter.increment(recordLength);

        
            return true;
        }

        return false;
    }

}
