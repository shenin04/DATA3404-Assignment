import org.apache.camel.util.Time;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import java.util.Date;
import java.text.SimpleDateFormat;


public class OptAvgDelay {

    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Define a data set from the flights.csv file , include the required fields for the task, in this case carrier code, scheduled dept time and actual dept time
        DataSet<Tuple6<String, String, String, String, String, String>> flights =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_flights_medium.csv").
                        includeFields("010100011110").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class, String.class, String.class, String.class);


        //Define a data set from the data.csv file, include the required fields for the task, in this case
        DataSet<Tuple3<String, String, String>> airline =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_airlines.csv")
                        .includeFields("111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);


        //Join to data sets to create a new one tuple data set with region name
        DataSet<Tuple7<String, String, String, String, String, String, String>> result = flights.join(airline, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo(0)//joining two data sets using the equality condition flights.carrier_code = airlines.carrier_code
                .with(new JoinUR()).filter(new FilterFunction<Tuple7<String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple7<String, String, String, String, String, String, String> t) {
                return t.f1.contains("United States") && t.f2.contains(args[0]);
            }
        });


        DataSet<Tuple2<String, String>> finalResult = result.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple7<String, String, String, String, String, String, String>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple7<String, String, String, String, String, String, String>> filteredResult, Collector<Tuple2<String, String>> out) throws Exception {
                Date scheduledDept = null;
                Date actualDept = null;
                Date scheduledArri = null;
                Date actualArri = null;
                String airportName = null;
                long totalDelay = 0;
                long avgDelay = 0;
                int cnt = 0;
                String secs = null;
                String mins = null;
                String hrs = null;

                try {
                    SimpleDateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");

                    //Format - airline_name, country, date, scheduled dept, scheduled arrival, actual dept, actual arri
                    for (Tuple7<String, String, String, String, String, String, String> data : filteredResult) {
                        airportName = data.f0;

                        if(data.f5.equals("") || data.f6.equals("")) continue;

                        scheduledDept = timeFormat.parse(data.f3);
                        scheduledArri = timeFormat.parse(data.f4);
                        actualDept = timeFormat.parse(data.f5);
                        actualArri = timeFormat.parse(data.f6);

                        long diffDept = actualDept.getTime() - scheduledDept.getTime();
                        long diffArrival = actualArri.getTime() - scheduledArri.getTime();

                        if(diffDept > 0 && diffArrival < 0) continue;

                        totalDelay += diffArrival + diffDept;
                        cnt++;

                    }
                } catch (Exception e) {
                    System.out.println("ERROR");
                }

                avgDelay = totalDelay / cnt;
                secs = String.format("%02d", avgDelay / 1000 % 60);
                mins = String.format("%02d", avgDelay / (60 * 1000) % 60);
                hrs = String.format("%02d", avgDelay / (60 * 60 * 1000) % 60);

                if (secs.contains("-") || mins.contains("-") || hrs.contains("-")) {
                    out.collect(new Tuple2<>(airportName, "00:00:00"));
                } else
                    out.collect(new Tuple2<>(airlineName, hrs + ":" + mins + ":" + secs));
            }
        });

        finalResult.sortPartition(0, Order.ASCENDING).setParallelism(1).writeAsCsv("/home/storm/data3404/assignment_data_files/t2output_mediumOpt.csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        finalResult.print();
    }

    //Joins the two tables for flights and airlines
    private static class JoinUR implements JoinFunction<Tuple6<String, String, String, String, String, String>,
            Tuple3<String, String, String>,
            Tuple7<String, String, String, String, String, String, String>> {

        @Override
        public Tuple7<String, String, String, String, String, String, String> join(Tuple6<String, String, String, String, String, String> flights,
                                                                           Tuple3<String, String, String> airline) {

            //Format is airline_name, country, date, scheduled dept, scheduled arrival, actual dept, actual arri
            return new Tuple7<>(airline.f1, airline.f2, flights.f1, flights.f2, flights.f3, flights.f4, flights.f5);
        }
    }
}
