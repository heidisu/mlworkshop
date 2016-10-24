package no.kantega.mlworkshop;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.List;

public class TaskSubmitter {
    private String servicePath = String.format("http://%s:3000/application/submissions/1", SubmissionProperties.SERVER);
    private HttpClient httpClient;
    private HttpPost task1Post = new HttpPost(servicePath);
    private Gson gson = new Gson();

    public TaskSubmitter() {
        httpClient = HttpClientBuilder.create().build();
    }

    public void submit(List<Prediction> predictions) {
        TaskSubmission taskSubmission = new TaskSubmission();
        taskSubmission.setName(SubmissionProperties.NAME);
        taskSubmission.setPredictions(predictions);
        String json = gson.toJson(taskSubmission);
        task1Post.setHeader("Content-Type", "application/json");

        try {
            task1Post.setEntity(new StringEntity(json));
            HttpResponse response = httpClient.execute(task1Post);
            int responseCode = response.getStatusLine().getStatusCode();
            String content = IOUtils.toString(response.getEntity().getContent());
            System.out.println("Task Submission:");
            System.out.println(String.format("HTTP STATUS: %d", responseCode));
            System.out.println(String.format("RESPONSE: %s", content));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
