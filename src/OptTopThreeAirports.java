import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;

public class OptTopThreeAirports {
    public static void main(String[] args) throws Exception {
        if(args.length < 1) return;
        // execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // take in csv files for usage
        DataSet<Tuple2<String, String>> flights =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_flights_medium.csv").includeFields("000110000000").ignoreFirstLine().ignoreInvalidLines().types(String.class,String.class);
        // filtering to get the year
        DataSet<Tuple1<String>> filtflights;
        filtflights = ((DataSource<Tuple2<String,String>>) flights).setParallelism(2).filter(new FilterFunction<Tuple2<String, String>>(){
            @Override
            public boolean filter(Tuple2<String, String> t){
                return t.f0.contains(args[0]);
            }
        })
                // projecting only the airport codes
                .project(1);

        // sorting whilst summing groups
        filtflights.groupBy(0)
                .reduceGroup(new FlightCounter())
                .setParallelism(1).sortPartition(1,  Order.DESCENDING).first(3)
                .writeAsCsv("/home/storm/data3404/assignment_data_files/t1output_mediumOpt.csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        filtflights.print();


    }
    public static class FlightCounter implements GroupReduceFunction<Tuple1<String>,
            Tuple2<String, Integer>> {


        @Override
        public void reduce(Iterable<Tuple1<String>> flights, Collector <Tuple2<String, Integer>> out) {

            String flight=null;
            int iter = 0;
            for(Tuple1<String> tup : flights) {
                flight = tup.f0;
                iter++;
            }

            out.collect(new Tuple2<>(flight, iter));
        }
    }
}
