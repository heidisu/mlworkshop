package no.kantega.mlworkshop.task1;

import no.kantega.mlworkshop.AbstractTaskApp;
import no.kantega.mlworkshop.Prediction;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class App extends AbstractTaskApp {

    private static final String STUDENT_DATA_PATH = "student.csv";
    private static final String STUDENT_SUBMISSION_PATH = "student-submission.csv";

    public static void main(String[] args) {
        App app = new App();
        app.run();
    }

    private void run() {
        // starter lokal spark
        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("ML student").config(conf).getOrCreate();

        // TODO 1.1: les inn csv fil som dataset, se på metoden readFile
        Dataset<Row> students; // = ??


        // TODO 1.2: Se på dataene, tegn diagrammer. Hvilke egenskaper har betydning for om man består eksamen,
        // hvilke ser ikke ut til å ha noe å si for resultatet
        // plotHistogram(students, "age", "pass");


        // TODO 1.3: Del datasettet inn i "label" med "pass"-verdien og vektor "features" med de egenskapene du vil ha med
        // Pass på at "id" ikke blir med i features
        RFormula formula; // = new RFormula().setFormula(/* fyll ut her */);


        // TODO 2.1: Sett opp en classifer. For eksempel RandomForestClassifer eller LogisticRegressionClassifier
        Classifier classifier;

        // TODO 2.2 Sett formula og classifier sammen i en pipeline
        Pipeline pipeline;

        // TODO 2.3. Del datasettet i treningsdata og testdata. Modellen trenes med treningssettet og testes med testsettet
        TrainValidationSplit trainValidationSplit; // = ??

        // TODO 2.4 Tren validatoren med treningssettet
        TrainValidationSplitModel model; // = ??

        // TODO 2.5 Få modellen til å predikere bestått/ikke bestått
        Dataset<Row> predictions ; // ??

        // TODO 2.5 Undersøk resultatet av prediksjonene for testdataene
        Evaluator evaluator; // = model.getEvaluator();
        double accuracy;  // =

        // TODO 3.1 Oppdater verdiene i klassen SubmissionProperties i common-modulen
        // TODO 3.2 Send inn resultatet av modellen din
        //submitTask(spark, model);

        // TODO 4 Forbedre resultatet!
    }

    private void submitTask(SparkSession spark, Model model) {
        Dataset<Row> testStudents = readFile(spark, STUDENT_SUBMISSION_PATH);
        Dataset<Row> predictionSet = model.transform(testStudents);
        predictionSet.select("id", "prediction").show();
        List<Prediction> predictions = predictionSet.collectAsList().stream()
                .map(row -> new Prediction(row.<Integer>getAs("id").toString(), row.<Double>getAs("prediction")))
                .collect(Collectors.toList());
        submit(predictions);

    }

    private Dataset<Row> readFile(SparkSession spark, String path) {
        createTempFile(path);
        return spark.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(path);
    }
}
