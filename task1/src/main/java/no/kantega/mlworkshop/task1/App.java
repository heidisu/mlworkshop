package no.kantega.mlworkshop.task1;

import no.kantega.mlworkshop.AbstractTaskApp;
import no.kantega.mlworkshop.submission.Prediction;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class App extends AbstractTaskApp {

    private static final String STUDENT_DATA_PATH = "student.csv";

    public App(int taskId) {
        super(taskId);
    }

    public static void main(String[] args) {
        App app = new App(1);
        app.run();
    }

    private void run() {
        // starter lokal spark
        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("ML student").config(conf).getOrCreate();

        // TODO 1.1: les inn csv-fila student.csv som dataset, se på metoden readFile()
        Dataset<Row> students = readFile(spark, STUDENT_DATA_PATH);

        // TODO 1.2: Se på dataene, tegn diagrammer. Hvilke egenskaper har betydning for om man består eksamen,
        // hvilke ser ikke ut til å ha noe å si for resultatet?
        // plotHistogram(students, "age", "pass");
        plotHistogram(students, "age", "pass");

        // TODO 1.3: Del datasettet inn i kolonne "label" med "pass"-verdien
        // og kolonne "features" med vektorform av de egenskapene du vil ha med.
        // Pass på at "id" ikke blir med i features.
        // For å ha alle feltene utenom id som features, bruk "pass ~ . - id" (. betyr alle)
        // for å f.eks ha feltene age og g1, bruk "pass ~ age + g1".
        // Når rformula brukes på et datasett vil datasettet få en ny kolonne label som inneholder feltet før ~ og
        // en kolonne features som inneholder en vektor med feltene bestemt av uttrykket etter ~.
        // For å teste resultatet kan du gjøre formula.fit(students).transform(students).show().
        RFormula formula = new RFormula().setFormula("pass ~ . - id");
        formula.fit(students).transform(students).show();

        // TODO 2.1: Sett opp en classifer (= maskinlæringsalgoritme). Bruk RandomForestClassifer eller LogisticRegression
        Classifier classifier = new RandomForestClassifier();

        // TODO 2.2 Sett formula og classifier sammen i en pipeline. Bruk pipeline.setStages for å sette at trinnene skal
        // være formula først og classifier etterpå
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{formula, classifier});

        // TODO 2.3. Del datasettet i treningssett og testsett. Modellen trenes med treningssettet og testes til slutt med testsettet
        // bruk dataset.randomSplit()
        Dataset<Row>[] splits = students.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        // TODO 2.4 Tren pipelinen med treningssettet ved å bruke metoden pipeline.fit
        PipelineModel model = pipeline.fit(training);

        // TODO 2.5 Få modellen til å predikere bestått/ikke bestått ved å bruke metoden model.transform
        Dataset<Row> predictions = model.transform(test);

        // TODO 2.6 Undersøk resultatet av prediksjonene for testdataene ved å lage en evaluator. Vi har en binær klassifikasjon, så
        // lag en BinaryClassificationEvaluator, og få ut et tall på nøyaktigheten ved å bruke metoden evaluate
        Evaluator evaluator = new BinaryClassificationEvaluator();
        double accuracy = evaluator.evaluate(predictions);

        System.out.println("Accuracy: " + accuracy);

        // TODO 3.1 Oppdater verdiene i klassen SubmissionProperties i common-modulen
        // TODO 3.2 Send inn resultatet av modellen din
        //submitTask(spark, model);

        // TODO 4 Forbedre resultatet!
        // Se på hvilke felter du valgte i featurevektoren din, er det bedre med andre eller færre felter?
        // Les om hvordan ChiSqSelector kan brukes til å finne de viktigste feltene
        // https://spark.apache.org/docs/latest/ml-features.html#chisqselector
        // Du kan også teste å bytte ut maskinlæringsmodellen med den du ikke brukte i 2.1, blir det forskjell på resultatet?
        ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(4).setOutputCol("selFeatures");
        Classifier classifier2 = new RandomForestClassifier().setFeaturesCol("selFeatures");
        Pipeline pipeline2 = new Pipeline().setStages(new PipelineStage[]{formula, selector, classifier2});
        Model model2 = pipeline2.fit(training);
        System.out.println("Accuracy: " + new BinaryClassificationEvaluator().evaluate(model2.transform(test)));
    }

    private void submitTask(SparkSession spark, Model model) {
        Dataset<Row> testStudents = readFile(spark, "student-submission.csv");
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
