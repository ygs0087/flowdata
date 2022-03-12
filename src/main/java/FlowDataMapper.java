import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable 相当于 Long；Text 相当于 String
 * LongWriteable, Text > 偏移量 手机号
 * Text FlowDataEntity > (手机号, 流量对象）
 */
public class FlowDataMapper extends Mapper<LongWritable, Text, Text, FlowDataEntity> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowDataEntity>.Context context) throws IOException, InterruptedException {
        if (value == null) return;
        String[] fields = value.toString().split("\t");
        String phoneNum = fields[1];

        Long upFlow = Long.parseLong(fields[fields.length - 3]);
        Long downFlow = Long.parseLong(fields[fields.length - 2]);
        Text keyOut = new Text();
        keyOut.set(phoneNum);

        context.write(keyOut, new FlowDataEntity(upFlow, downFlow));
    }
}
