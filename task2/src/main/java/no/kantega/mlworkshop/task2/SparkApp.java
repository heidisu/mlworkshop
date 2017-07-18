package no.kantega.mlworkshop.task2;

import no.kantega.mlworkshop.AbstractTaskApp;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

import static java.nio.file.StandardOpenOption.APPEND;

class SparkApp extends AbstractTaskApp {
    private SparkSession sparkSession;
    private PipelineModel model;
    private File file;
    static final int IMAGE_SIDE = 50;
    private static final String FILE = "numbers.csv";

    SparkApp() {
        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        sparkSession = SparkSession.builder().appName("ML numbers").config(conf).getOrCreate();
        file = new File(FILE);
        if(!file.exists()){
            try {
                Files.createFile(Paths.get(FILE));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    String trainModel() {
        Dataset<Row> numbers = readCsvFile(sparkSession);


        // TODO Transformer datasettet til å inneholde en kolonne med features og en kolonne med label

        // TODO Sett opp et nevralt nett
        MultilayerPerceptronClassifier classifier;

        // TODO Lag en pipeline med transformer og classifier
        Pipeline pipeline;

        // TODO Lag en validator som validerer pipelinen og finner de beste parameterne for MultiLayerPerceptronClassifier
        ParamMap[] paramGrid;
        MulticlassClassificationEvaluator evaluator;

        // TODO Kjør evaluatoren og finn den beste modellen
        // model = ?

        // TODO Returner et tall som sier noen om hvor bra modellen var på treningsdataene
        double accuracy = 0.0;

        return String.format("Score: %f", accuracy);
    }

    private Dataset<Row> readCsvFile(SparkSession spark) {
        return spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "false")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(FILE);
    }

    int predict(List<Double> numbers) {

        // TODO Gjør om listen av tall til Dataset<Row> med samme form som treningssettet
        Dataset<Row> image;

        // TODO Bruk den trente modellen til å predikere hva slags tall det er bilde av
        Double prediction = new Random().nextDouble()* 10;
        return prediction.intValue();
    }

    void addTrainingSample(String line) throws IOException {
        Files.write(file.toPath(), line.getBytes(), APPEND);
    }

    void evaluateModel() {
        // get dataset
        // map verder
        //submit();
    }
}
