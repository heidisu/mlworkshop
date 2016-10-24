package no.kantega.mlworkshop.task1;

import no.kantega.mlworkshop.AbstractTaskApp;
import no.kantega.mlworkshop.Prediction;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.stream.Collectors;

public class App extends AbstractTaskApp{

    private static final String STUDENT_DATA_PATH = "student.csv";
    private static final String STUDENT_SUBMISSION_PATH = "student-submission.csv";

    public static void main(String[] args) throws FileNotFoundException, IllegalAccessException, InstantiationException {
        App app = new App();
        app.run();
    }

    private void run() {

        // les inn fil
       //File file = readData(STUDENT_DATA_PATH);

        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("ML student").config(conf).getOrCreate();

        //System.out.println("datapath: " + dataPath);
        Dataset<Row> students = readFile(spark, STUDENT_DATA_PATH);
        students.show();

//        plotFeatures(students, "pass", "age");
//        plotHistogram(students, "age", "pass");
//        plotHistogram(students, "alc", "pass");
//        plotHistogram(students, "address", "pass");
//        plotHistogram(students, "internet", "pass");

        Dataset<Row> transformed = transformToFeatures(students);
        transformed.show();

        transformed.groupBy("pass").count().show();

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(6)
                .fit(transformed);

        ChiSqSelectorModel chiSqSelectorModel = new ChiSqSelector()
                .setNumTopFeatures(18)
                .setFeaturesCol("features")
                .setLabelCol("label")
                .setOutputCol("selectedFeatures")
                .fit(transformed);


        Dataset<Row>[] split = transformed.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingSet = split[0];
        Dataset<Row> testSet = split[1];

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("label")
                .setFeaturesCol("selectedFeatures");

        // Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{chiSqSelectorModel, rf});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingSet);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testSet);
        predictions.show();

        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Area under ROC: " + accuracy);
        System.out.println("Test Error : " + (1 - accuracy));

       //submitTask(spark, model);
    }

    private void submitTask(SparkSession spark, Model model) {
        Dataset<Row> testStudents = readFile(spark, STUDENT_SUBMISSION_PATH);
        Dataset<Row> testTransformed = transformToFeatures(testStudents);
        Dataset<Row> predictionSet = model.transform(testTransformed);
        predictionSet.select("id", "prediction").show();
        List<Prediction> predictions = predictionSet.collectAsList().stream()
                .map(row -> new Prediction(row.<Integer>getAs("id").toString(), row.<Double>getAs("prediction")))
                .collect(Collectors.toList());
        submit(predictions);

    }

    protected Dataset<Row> transformToFeatures(Dataset<Row> data) {
        RFormula formula = new RFormula().setFormula("pass ~ . - id");
        return formula.fit(data).transform(data);
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
