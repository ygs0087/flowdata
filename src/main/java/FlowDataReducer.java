import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowDataReducer extends Reducer<Text, FlowDataEntity, Text, FlowDataEntity> {

    @Override
    protected void reduce(Text key, Iterable<FlowDataEntity> values, Reducer<Text, FlowDataEntity, Text, FlowDataEntity>.Context context) throws IOException, InterruptedException {
        Long sumUpFlow = 0L, sumDownFlow = 0L;
        for (FlowDataEntity value : values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        context.write(key, new FlowDataEntity(sumUpFlow, sumDownFlow));
    }

}


