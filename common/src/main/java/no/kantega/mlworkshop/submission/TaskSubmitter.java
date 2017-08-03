package no.kantega.mlworkshop.submission;

import com.google.gson.Gson;
import no.kantega.mlworkshop.SubmissionProperties;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TaskSubmitter {
    private final HttpPost httpPost;
    private final HttpGet httpGet;
    private final Gson gson = new Gson();

    public TaskSubmitter(int taskId) {
        String servicePath = String.format("%s/submissions/%d", SubmissionProperties.SERVICE_ROOT, taskId);
        httpPost = new HttpPost(servicePath);
        httpPost.setHeader("Content-Type", "application/json");
        httpGet  = new HttpGet(servicePath);
        httpGet.addHeader("accept", "application/json");
    }

    public void submit(List<Prediction> predictions) {
        TaskSubmission taskSubmission = new TaskSubmission();
        taskSubmission.setName(SubmissionProperties.NAME);
        taskSubmission.setPredictions(predictions);
        String json = gson.toJson(taskSubmission);

        try {
            httpPost.setEntity(new StringEntity(json));
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpPost);
            int responseCode = response.getStatusLine().getStatusCode();
            String content = IOUtils.toString(response.getEntity().getContent());
            System.out.println("Task Submission:");
            System.out.println(String.format("HTTP STATUS: %d", responseCode));
            System.out.println(String.format("RESPONSE: %s", content));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<PredictionData> getPredictionData() {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpGet);
            int responseCode = response.getStatusLine().getStatusCode();
            System.out.println(String.format("HTTP STATUS: %d", responseCode));
            String json = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
            PredictionData[] predictionData = gson.fromJson(json, PredictionData[].class);
            return Arrays.asList(predictionData);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }
}
