package no.kantega.mlworkshop.task3;

import no.kantega.mlworkshop.AbstractTaskApp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App extends AbstractTaskApp
{
    private App(int taskId) {
        super(taskId);
    }

    public static void main(String[] args )
    {
        new App(3).run();
    }



    private void run(){
        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("Bike").config(conf).getOrCreate();

        // TODO: Last ned csv-filer om bysykkelturer fra https://developer.oslobysykkel.no/data og legg de i resources/

        // TODO: Les inn filene i ett datasett og undersøk hvordan datasettet ser ut
        Dataset<Row> trips;

        // TODO Transformer datasettet slik at det inneholder separate kolonner for år, måned, dag, time og ukedag
        // Kan få bruk for functions.unix_timestamp(bikeTrips.col(<kolonnenavn>), "yyyy-MM-dd HH:mm:ss").cast("timestamp")


        // TODO Gjør en groupBy-operasjon sånn at du får antallet turer pr time

    }

    private Dataset<Row> readCsvFile(SparkSession spark, String path) {
        createTempFile(path);
        return spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ",")
                .load(path);
    }
}
