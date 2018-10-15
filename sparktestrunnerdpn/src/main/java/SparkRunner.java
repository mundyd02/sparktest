import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;

public class SparkRunner {

    public static void main(String[] args) throws Exception {

        AmazonElasticMapReduce emr = new AmazonElasticMapReduceClient(new DefaultAWSCredentialsProviderChain()).withRegion(Regions.EU_WEST_1);

        StepFactory stepFactory = new StepFactory();
        AddJobFlowStepsRequest req = new AddJobFlowStepsRequest();
        req.withJobFlowId("j-3VXFPO2J7DILG");

        List<StepConfig> stepConfigs = new ArrayList<StepConfig>();

        HadoopJarStepConfig sparkStepConf = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit","--executor-memory","1g","--class","com.baeldung.WordCount","s3://dpm-test-emr/sparktest-dpm-1.0-SNAPSHOT.jar", "10");
        // above removed ,"10" version of something?????
        StepConfig sparkStep = new StepConfig()
                .withName("Spark Step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(sparkStepConf);

        stepConfigs.add(sparkStep);
        req.withSteps(stepConfigs);
        AddJobFlowStepsResult result = emr.addJobFlowSteps(req);

    }

    public static void getJar() {
        String clientRegion = "eu-west-1";
        String bucketName = "dpm-test-emr";
        String key = "sparktest-dpm-1.0-SNAPSHOT.jar";

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .withRegion(clientRegion)
                    .build();
            File fileDest = new File("/tmp/" + key);
            fileDest.delete();
            s3Client.getObject(new GetObjectRequest(bucketName, key), fileDest );

        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }



}

