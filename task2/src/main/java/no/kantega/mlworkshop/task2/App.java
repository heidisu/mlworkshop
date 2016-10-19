package no.kantega.mlworkshop.task2;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws FileNotFoundException, IllegalAccessException, InstantiationException {
        String dataPath = App.class.getClassLoader().getResource("trips-09-2016.csv").getPath();
        List<Trip> trips = new ArrayList<>();
        System.out.println(trips.size());

        SparkConf conf = new SparkConf().setAppName("ML").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("ML Wine").config(conf).getOrCreate();

        String jsonPath = App.class.getClassLoader().getResource("person.json").getPath();
        Dataset<Row> people = spark.read().json(jsonPath);

        people.printSchema();
        people.show();

        String studentPath = App.class.getClassLoader().getResource("student.csv").getPath();
        Dataset<Row> students = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(studentPath);

        students.show();

        RFormula formula = new RFormula().setFormula("pass ~ .");

        Dataset<Row> transformed = formula.fit(students).transform(students);
        transformed.select("features", "label").show();

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(6)
                .fit(transformed);

        String mathStudentPath = App.class.getClassLoader().getResource("student-math.csv").getPath();
        Dataset<Row> mathStudents = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(mathStudentPath);

        Dataset<Row> mathStudentsTest = featureIndexer.transform(formula.fit(mathStudents).transform(mathStudents));

        Dataset<Row>[] split = transformed.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingSet = split[0];
        Dataset<Row> testSet = split[1];

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("label")
                .setFeaturesCol("indexedFeatures");




        // Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {featureIndexer, rf});

        // Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingSet);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testSet);

        for(Row r : predictions.collectAsList()){
            int pass = r.getInt(r.fieldIndex("pass"));
            double label = r.getDouble(r.fieldIndex("label"));
            double prediction = r.getDouble(r.fieldIndex("prediction"));
            if(prediction != label) {
                System.out.println("Pass: " + pass + " label: " + label + " prediction: " + prediction);
            }

        }
        // Select example rows to display.
        predictions.show();
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC");
//
       double accuracy = evaluator.evaluate(predictions);
        System.out.println("Area under ROC: " + accuracy);
      System.out.println("Test Error : " + (1 - accuracy));

        //RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[1]);
        //System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());

        Dataset<Row> mathPredictions = model.transform(mathStudentsTest);
        accuracy = evaluator.evaluate(mathPredictions);
        System.out.println("Area under ROC: " + accuracy);
        System.out.println("Test Error : " + (1 - accuracy));

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

// Fit the model
        LogisticRegressionModel lrModel = lr.fit(trainingSet);

// Print the coefficients and intercept for logistic regression
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();

// Obtain the loss per iteration.
        double[] objectiveHistory = trainingSummary.objectiveHistory();
        for (double lossPerIteration : objectiveHistory) {
            System.out.println(lossPerIteration);
        }

// Obtain the metrics useful to judge performance on test data.
// We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
// classification problem.
        BinaryLogisticRegressionSummary binarySummary =
                (BinaryLogisticRegressionSummary) trainingSummary;


 //       ChiSqSelector selector = new ChiSqSelector()
//                .setNumTopFeatures(5)
//                .setFeaturesCol("features")
//                .setLabelCol("pass")
//                .setOutputCol("selectedFeatures");
//
//        Dataset<Row> result = selector.fit(transformed).transform(transformed);
//        result.show();


// Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
        Dataset<Row> roc = binarySummary.roc();
        roc.show();
        roc.select("FPR").show();
        System.out.println(binarySummary.areaUnderROC());
//        JavaRDD<LabeledPoint> data = transformed.javaRDD().map(row -> new LabeledPoint(row.<Double>getAs("label"), row.<Vector>getAs("features")));
//        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
//        JavaRDD<LabeledPoint> training = splits[0].cache();
//        JavaRDD<LabeledPoint> test = splits[1];
//
//        LogisticRegressionModel logisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training.rdd());
//
//        // Compute raw scores on the test set.
//        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
//                new Function<LabeledPoint, Tuple2<Object, Object>>() {
//                    public Tuple2<Object, Object> call(LabeledPoint p) {
//                        Double prediction = logisticRegressionModel.predict(p.features());
//                        return new Tuple2<Object, Object>(prediction, p.label());
//                    }
//                }
//        );
//
//// Get evaluation metrics.
//        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
//        double accuracy2 = metrics.accuracy();
//        System.out.println("Accuracy = " + accuracy2);



//        LogisticRegression logisticRegression = new LogisticRegression()
//                .setMaxIter(10)
//                .setRegParam(0.3)
//                .setElasticNetParam(0.8);
//
//        LogisticRegressionModel model = logisticRegression.fit(trainingSet);
//
//        Dataset<Row> predictions = model.transform(testSet);
//
//        predictions.show();
//
//        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC");
//
//        double accuracy = evaluator.evaluate(predictions);
//        System.out.println("Test Error : " + (1 - accuracy));
//
//        LogisticRegressionTrainingSummary trainingSummary = model.summary();
//
//        // Obtain the loss per iteration.
//        double[] objectiveHistory = trainingSummary.objectiveHistory();
//        for (double lossPerIteration : objectiveHistory) {
//            System.out.println(lossPerIteration);
//        }

        // Obtain the metrics useful to judge performance on test data.
        // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
        // classification problem.
//        BinaryLogisticRegressionSummary binarySummary =
//                (BinaryLogisticRegressionSummary) trainingSummary;
//
//        // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
//        Dataset<Row> roc = binarySummary.roc();
//        roc.show();
//        roc.select("FPR").show();
//        System.out.println(binarySummary.areaUnderROC());
//
//        // Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
//        // this selected threshold.
//        Dataset<Row> fMeasure = binarySummary.fMeasureByThreshold();

        String porStudentPath = App.class.getClassLoader().getResource("student-por.csv").getPath();
        Dataset<Row> porStudents = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", ";")
                .load(porStudentPath);
        List<Student> studentList = new ArrayList<>();

        for (Row r : porStudents.collectAsList()) {
            Student s = new Student();
            s.school = fromSchool((String) r.get(0));
            s.gender = fromGender((String) r.get(1));
            s.age = (int) r.get(2);
            s.address = fromAddress((String) r.get(3));
            s.pStatus = fromPstatus((String) r.get(5));
            s.mEdu = (int) r.get(6);
            s.fEdu = (int) r.get(7);
            s.mJob = fromJob((String) r.get(8));
            s.fJob = fromJob((String) r.get(9));
            s.reason = fromReason((String) r.get(10));
            s.traveltime = (int) r.get(12);
            s.studytime = (int) r.get(13);
            s.failures = (int) r.get(14);
            s.schoolsup = fromYesNo((String) r.get(15));
            s.famsup = fromYesNo((String) r.get(16));
            s.paid = fromYesNo((String) r.get(17));
            s.activities = fromYesNo((String) r.get(18));
            s.nursery = fromYesNo((String) r.get(19));
            s.higher = fromYesNo((String) r.get(20));
            s.internet = fromYesNo((String) r.get(21));
            s.romantic = fromYesNo((String) r.get(22));
            s.famrel = (int) r.get(23);
            s.freetime = (int) r.get(24);
            s.goout = (int) r.get(25);
            int dAlc = (int) r.get(26);
            int wAlc = (int) r.get(27);
            s.alc = getAlc(wAlc, dAlc);
            s.health = (int) r.get(28);
            s.absences = (int) r.get(29);
            s.g1 = (int) r.get(30);
            s.g2 = (int) r.get(31);
            s.g3 = (int) r.get(32);
            s.pass = s.g3 >= 10 ? 1 : 0;
            studentList.add(s);
        }

        for(int i = 1; i<= studentList.size(); i++) {
            Student s = studentList.get(i);
            System.out.println(
                    i + ";" +
                    s.school + ";" +
                            s.gender + ";" +
                            s.age + ";" +
                            s.address + ";" +
                            s.famsize + ";" +
                            s.pStatus + ";" +
                            s.mEdu + ";" +
                            s.fEdu + ";" +
                            s.mJob + ";" +
                            s.fJob + ";" +
                            s.reason + ";" +
                            s.traveltime + ";" +
                            s.studytime + ";" +
                            s.failures + ";" +
                            s.schoolsup + ";" +
                            s.famsup + ";" +
                            s.paid + ";" +
                            s.activities + ";" +
                            s.nursery + ";" +
                            s.higher + ";" +
                            s.internet + ";" +
                            s.romantic + ";" +
                            s.famrel + ";" +
                            s.goout + ";" +
                            s.alc + ";" +
                            s.health + ";" +
                            s.g1 + ";" +
                            s.g2 + ";" +
                            s.pass);
        }
    }

    public static int fromSchool(String school) {
        if ("GP".equalsIgnoreCase(school)) {
            return 0;
        }
        if ("MS".equalsIgnoreCase(school)) {
            return 1;
        }
        return -1;
    }

    public static int fromGender(String gender) {
        if ("F".equalsIgnoreCase(gender)) {
            return 0;
        }
        if ("M".equalsIgnoreCase(gender)) {
            return 1;
        }
        return -1;
    }

    public static int fromYesNo(String bool) {
        if ("yes".equalsIgnoreCase(bool)) {
            return 1;
        }
        if ("no".equalsIgnoreCase(bool)) {
            return 0;
        }
        return -1;
    }

    public static int fromAddress(String address) {
        if ("U".equalsIgnoreCase(address)) {
            return 0;
        }
        if ("R".equalsIgnoreCase(address)) {
            return 1;
        }
        return -1;
    }

    public static int fromPstatus(String pstatus) {
        if ("A".equalsIgnoreCase(pstatus)) {
            return 0;
        }
        if ("T".equalsIgnoreCase(pstatus)) {
            return 1;
        }
        return -1;
    }

    public static int fromJob(String job) {
        switch (job) {
            case "at_home":
                return 0;
            case "teacher":
                return 1;
            case "services":
                return 2;
            case "health":
                return 3;
            case "other":
                return 4;
            default:
                return -1;
        }
    }

    public static int fromReason(String reason) {
        switch (reason) {
            case "course":
                return 0;
            case "home":
                return 1;
            case "reputation":
                return 2;
            case "other":
                return 3;
            default:
                return -1;
        }
    }

    public static int getAlc(int wAlc, int dAlc) {
        double alc = (wAlc * 2 + dAlc * 5) / (double) 7;

        return alc < 3 ? 0 : 1;
    }
}
