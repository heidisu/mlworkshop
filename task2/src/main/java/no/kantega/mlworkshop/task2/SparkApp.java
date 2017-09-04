package no.kantega.mlworkshop.task2;

import no.kantega.mlworkshop.AbstractTaskApp;
import no.kantega.mlworkshop.submission.Prediction;
import no.kantega.mlworkshop.submission.PredictionData;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.APPEND;

class SparkApp extends AbstractTaskApp {
    private SparkSession sparkSession;
    private PipelineModel model;
    private File file;
    static final int IMAGE_SIDE = 25;
    private static final String FILE = "numbers.csv";
    private static final StructType IMAGE_SCHEMA;
    private static final StructType IMAGE_WITH_LABEL_SCHEMA;

    static {
        IMAGE_SCHEMA = DataTypes.createStructType(imageSchemaFields());
        List<StructField> imageWithLabelFields = imageSchemaFields();
        imageWithLabelFields.add(DataTypes.createStructField("label", DataTypes.DoubleType, true));
        IMAGE_WITH_LABEL_SCHEMA = DataTypes.createStructType(imageWithLabelFields);
    }


    SparkApp(int taskId) {
        super(taskId);
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

    private static List<StructField> imageSchemaFields() {
        List<StructField> fields = new ArrayList<>();
        for(int i = 0; i < IMAGE_SIDE * IMAGE_SIDE; i++){
            StructField field = DataTypes.createStructField("pxl_" + i, DataTypes.DoubleType, true);
            fields.add(field);
        }
        return fields;
    }

    /**
     * Trener modell fra fila med tall.
     *
     * @return Streng som beskriver nøyaktigheten til modellen
     */
    String trainModel() {
        Dataset<Row> numbers = readFile(sparkSession);

        // TODO 2.1 Sett opp RFormula så datasettet får en kolonne med features og en kolonne med label
        RFormula formula;

        // TODO 2.2 Sett opp en maskinlæringsalgoritme, LogisticRegression eller  MultilayerPerceptronClassifier (dvs nevralt nett)
        // Bytt ut "Predictor" med den med konkrete klassen du bruker
        Predictor classifier;

        // TODO 2.3 Lag en pipeline med RFormula og modell
        Pipeline pipeline;

        // TODO 2.4 Del datasettet i treningssett og testsett.
        Dataset<Row>[] splits; // = ??
        Dataset<Row> training; // = ??
        Dataset<Row> test; // = ??

        // TODO 2.5 Tren pipelinen med treningssettet
        // model = ?

        // TODO 2.6 Returner et tall som sier noen om hvor bra modellen var på testsdataene.
        // Modellen vår gjør multi-klassifikasjon så evaluatoren vi vil bruke er MulticlassClassificationEvaluator.
        Dataset<Row> predictions; // = ??
        Evaluator evaluator; // = ??
        double accuracy = 0.0;

        return String.format("Score: %f", accuracy);
    }

    /**
     * Metode som tar inn en liste av tallbilder, der et tall er representert som en lista av 0 og 1, og predikerer hva
     * slags siffer som er på bildet. Metoden returnerer en liste av prediksjoner, i samme rekkefølge som bildene.
     *
     * @param numbers En liste av tallbilder
     * @return en liste av prediksjoner
     */
    List<Double> predict(List<List<Double>> numbers) {

        // TODO 3.1 Gjør om listen av tall til Dataset<Row> med samme form som treningssettet
        // Først må List<List<Double>> konverteres til List<Row>.
        // Bruk funksjonen RowFactory.create() med List.toArray() for å konvertere ett bilde List<Double> til Row
        // Loop eller bruk stream og map for å konvertere hele settet
        List<Row> rows; // = ??

        // For å lage dataset må vi i tillegg til rows fortelle spark hva slags skjema datasettet skal ha
        // Du kan du bruke sparkSession.sqlContext().createDataFrame() med rows og konstanten IMAGE_SCHEMA
        Dataset<Row> images;

        // TODO 3.2 Bruk den trente modellen til å predikere hvilke tall det er bilde av
        Dataset<Row> predictions; // = ??

        // TODO 3.3 Returner en liste av prediksjoner
        // return predictions.select("prediction").collectAsList().stream().map(row -> row.getDouble(0)).collect(Collectors.toList());

        Double prediction = new Random().nextDouble()* 10;
        return Collections.singletonList(prediction);
    }

    void addTrainingSample(String line) throws IOException {
        Files.write(file.toPath(), line.getBytes(), APPEND);
    }

    private Dataset<Row> readFile(SparkSession spark) {
        return spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "false")
                .option("delimiter", ";")
                .schema(IMAGE_WITH_LABEL_SCHEMA)
                .load(FILE);
    }

    void evaluateModel() {
        List<PredictionData> predictionData = getData();
        List<List<Double>> data = predictionData.stream().map(PredictionData::getData).collect(Collectors.toList());
        List<Double> predictions = predict(data);
        List<Prediction> submissionData = new ArrayList<>();
        for(int i = 0; i < predictions.size(); i++){
            submissionData.add(new Prediction(predictionData.get(i).getId(), predictions.get(i)));
        }
        submit(submissionData);
    }
}
