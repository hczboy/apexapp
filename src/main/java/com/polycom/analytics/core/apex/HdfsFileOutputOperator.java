package com.polycom.analytics.core.apex;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.util.KeyValPair;

public class HdfsFileOutputOperator extends AbstractFileOutputOperator<KeyValPair<String, byte[]>>
{
    private transient byte[] tupleSeparatorBytes;

    private String tupleSeparator;

    private static final Logger log = LoggerFactory.getLogger(HdfsFileOutputOperator.class);

    @Override
    public void setup(OperatorContext context)
    {
        super.setup(context);
        log.info("HdfsFileOutputOperator is setup");

    }

    public HdfsFileOutputOperator()
    {
        setTupleSeparator(System.getProperty("line.separator"));
    }

    public String getTupleSeparator()
    {
        return tupleSeparator;
    }

    public void setTupleSeparator(String separator)
    {
        this.tupleSeparator = separator;
        this.tupleSeparatorBytes = separator.getBytes();
    }

    @Override
    protected String getFileName(KeyValPair<String, byte[]> tuple)
    {
        return tuple.getKey();
    }

    @Override
    protected byte[] getBytesForTuple(KeyValPair<String, byte[]> tuple)
    {

        ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();

        try
        {
            bytesOutStream.write(tuple.getValue());
            bytesOutStream.write(tupleSeparatorBytes);

            return bytesOutStream.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            try
            {
                bytesOutStream.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

    }

}
