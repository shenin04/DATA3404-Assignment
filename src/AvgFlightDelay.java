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
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.deser.DataFormatReaders;
import org.apache.flink.util.Collector;
import java.util.Date;
import java.text.SimpleDateFormat;


public class AvgFlightDelay {

    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Define a data set from the flights.csv file , include the required fields for the task, in this case carrier code, scheduled dept time and actual dept time
        DataSet<Tuple6<String, String, String, String, String, String>> flights =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_flights_medium.csv")
                        .includeFields("010100011110").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class, String.class, String.class, String.class);

        //   flights.print();

        //Define a data set from the data.csv file, include the required fields for the task, in this case
        DataSet<Tuple3<String, String, String>> airline =
                env.readCsvFile("/home/storm/data3404/assignment_data_files/ontimeperformance_airlines.csv")
                        .includeFields("111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);

        // airline.print();

        //Join to data sets to create a new one tuple data set with region name
        DataSet<Tuple7<String, String, String, Long, Long, Long, Long>> result = flights.join(airline).where(0).equalTo(0)//joining two data sets using the equality condition flights.carrier_code = airlines.carrier_code
                .with(new JoinUR()).filter(new FilterFunction<Tuple7<String, String, String, Long, Long, Long, Long>>() {
            @Override
            public boolean filter(Tuple7<String, String, String, Long, Long, Long, Long> t) {
                return t.f1.contains("United States") && t.f2.contains(args[0]);
            }
        });

  //     result.print();

        DataSet<Tuple2<String, String>> finalResult = result.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple7<String, String, String, Long , Long, Long, Long>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple7<String, String, String, Long, Long, Long, Long>> filteredResult, Collector<Tuple2<String, String>> out) throws Exception {
                String airportName = null;
                long totalDelay = 0;
                long avgDelay = 0;
                int cnt = 0;
                String secs = null;
                String mins = null;
                String hrs = null;

                //Format - airline_name, country, date, scheduled dept, scheduled arrival, actual dept, actual arri
                for (Tuple7<String, String, String, Long, Long, Long, Long> data : filteredResult) {
                    airportName = data.f0;

                    long diffDept = data.f5 - data.f3;
                    long diffArrival = data.f6 - data.f4;

                    if(diffDept > 0 && diffArrival < 0) continue;

                    totalDelay += diffArrival + diffDept;
                    cnt++;
                }

                avgDelay = totalDelay / cnt;
                secs = String.format("%02d", avgDelay / 1000 % 60);
                mins = String.format("%02d", avgDelay / (60 * 1000) % 60);
                hrs = String.format("%02d", avgDelay / (60 * 60 * 1000) % 60);

                if (secs.contains("-") || mins.contains("-") || hrs.contains("-")) {
                    out.collect(new Tuple2<>(airportName, "00:00:00"));
                } else
                    out.collect(new Tuple2<>(airportName, hrs + ":" + mins + ":" + secs));
            }
        });

        finalResult.sortPartition(0, Order.ASCENDING).setParallelism(1).writeAsCsv("/home/storm/data3404/assignment_data_files/t2output_medium.csv", "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        finalResult.print();
    }

    //Joins the two tables for flights and airlines
    private static class JoinUR implements JoinFunction<Tuple6<String, String, String, String, String, String>,
            Tuple3<String, String, String>,
            Tuple7<String, String, String, Long, Long, Long, Long>> {

        @Override
        public Tuple7<String, String, String, Long, Long, Long, Long> join(Tuple6<String, String, String, String, String, String> flights,
                                                                           Tuple3<String, String, String> airline) {

            Date scheduledDept = null;
            Date actualDept = null;
            Date scheduledArri = null;
            Date actualArri = null;
            try {
                SimpleDateFormat timeFormat = new SimpleDateFormat("hh:mm:ss");

                if (flights.f5.equals("")) {
                    flights.f5 = flights.f3;
                }
                if (flights.f4.equals("")) {
                    flights.f4 = flights.f2;
                }

                // System.out.println(flights.f2 + " + " + flights.f3);
                scheduledDept = timeFormat.parse(flights.f2);
                scheduledArri = timeFormat.parse(flights.f3);
                actualDept = timeFormat.parse(flights.f4);
                actualArri = timeFormat.parse(flights.f5);


            } catch (Exception e) {
                System.out.println("ERROR");
            }

            //Format is airline_name, country, date, scheduled dept, scheduled arrival, actual dept, actual arri
            return new Tuple7<String, String, String, Long, Long, Long, Long>(airline.f1, airline.f2, flights.f1, scheduledDept.getTime(), scheduledArri.getTime(), actualDept.getTime(), actualArri.getTime());
        }
    }
}