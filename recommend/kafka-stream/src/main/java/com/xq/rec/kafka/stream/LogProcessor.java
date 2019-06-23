package com.xq.rec.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author xiaoqiang
 * @date 2019/6/24 0:12
 */
public class LogProcessor implements Processor<byte[], byte[]>
{
    private static final String MOVIE_RATING_PREFIX ="MOVIE_RATING_PREFIX:";

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext)
    {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line)
    {
        // 收集到的日志信息
        String input = new String(line);
        // 根据前缀过滤日志信息
        if (input.contains(MOVIE_RATING_PREFIX))
        {
            System.out.println("movie rating data coming>>>>" + input);
            input = input.split(MOVIE_RATING_PREFIX)[1].trim();
        }
        context.forward("logProcessor".getBytes(), input.getBytes());
    }

    @Override
    public void punctuate(long l)
    {

    }

    @Override
    public void close()
    {

    }
}
