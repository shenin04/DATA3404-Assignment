import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import java.util.HashMap;

public class OptPopularAircrafts {
    public static void main(String[] args) throws Exception {
        // execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // take in csv files for usage
        DataSet<Tuple2<String, String>> flights =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_flights_tiny.csv")
                        .includeFields("010000100000").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class);
        DataSet<Tuple3<String, String, String>> airlines =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_airlines.csv")
                        .includeFields("111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
        DataSet<Tuple3<String, String, String>> aircrafts =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_aircrafts.csv")
                        .includeFields("101010000").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);


        /////////////////////////////////////
        // Implementation #2 OPTIMISED
        // Less naive
        // Functional Flink notations
        // Join hints since we know second set is smaller
        // Flights Join Airlines
        // Filter United States
        // Join Aircrafts
        // Group by and rank
        /////////////////////////////////////

        // Flights join Airlines
        DataSet<Tuple3<String, String, String>> flightlines =
                flights.join(airlines, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo(0)
                        .with(new JoinFA());

        DataSet<Tuple2<String, String>> resultsOpt = flightlines.filter(new FilterFunction<Tuple3<String, String, String>>(){
            @Override
            public boolean filter(Tuple3<String, String, String> t){
                return t.f2.contains("United States");
            }
        })
                .project(0, 1);


        // Flights join Aircrafts
        DataSet<Tuple2<String, String>> results =
                resultsOpt.join(aircrafts, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(1).equalTo(0)
                        .with(new JoinFAA2());

        /////////////////////////////////////
        // Common step
        /////////////////////////////////////

        // Calculate sum, group by airline and sort it according to the sum
        // Select 5, re sort and format tuples
        results.reduceGroup(new PopularityCounter()).groupBy(0)
                .sortGroup(2, Order.DESCENDING)
                .first(5)
                .groupBy(0).sortGroup(2, Order.DESCENDING)
                .reduceGroup(new Concatenate()).sortPartition(0, Order.ASCENDING).setParallelism(1)
                .writeAsCsv("/home/storm/data3404/assignment_data_files/t3output_tinyOpt.csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        airlines.print();




    }

    private static class Concatenate implements GroupReduceFunction<Tuple3<String, String, Integer>, Tuple2 <String, String>> {


        @Override
        public void reduce(Iterable<Tuple3<String, String, Integer>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
            String first = null;
            String conc = null;
            for(Tuple3<String, String, Integer> i : iterable){
                if(first == null){
                    first = i.f0;
                    conc = "[";
                }
                if(i.f0.equals(first)){
                    if(conc.length() != 1) conc += ", ";
                    conc += i.f1;
                }
            }
            conc += "]";
            collector.collect(new Tuple2<>(first, conc));
        }
    }

    private static class PopularityCounter implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> faa, Collector<Tuple3<String, String, Integer>> out) {
            String airline = null;
            String aircraft = null;
            HashMap<String, Integer> tracker = new HashMap<String, Integer>();
            for(Tuple2<String, String> tup: faa) {
                String conc = tup.f0 + "%" + tup.f1;
                if(tracker.get(conc) == null){
                    tracker.put(conc, 1);
                } else {
                    tracker.put(conc,
                            tracker.get(conc) + 1);
                }

            }
            for(String ans : tracker.keySet()){
                String [] fin = ans.split("%");
                out.collect(new Tuple3<>(fin[0], fin[1], tracker.get(ans)));
            }

        }
    }

    // Implementation # 2
    @FunctionAnnotation.ForwardedFieldsFirst("1")
    @FunctionAnnotation.ForwardedFieldsSecond("1->0; 2")
    private static class JoinFA implements JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple3<String, String, String>> {
        @Override
        public Tuple3<String, String, String> join(Tuple2<String, String> flights,
                                                   Tuple3<String, String, String> airlines){

            return new Tuple3<>(airlines.f1, flights.f1, airlines.f2);
        }
    }

    @FunctionAnnotation.ForwardedFieldsFirst("0")
    private static class JoinFAA2 implements JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple2< String, String>> {
        @Override
        public Tuple2<String, String> join(Tuple2< String, String> flightlines,
                                           Tuple3<String, String, String> aircrafts){

            String conc = aircrafts.f1 + " " + aircrafts.f2;
            return new Tuple2<>(flightlines.f0, conc);
        }
    }

}
