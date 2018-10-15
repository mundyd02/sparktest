package com.baeldung;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.SparkConf;

import java.io.IOException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;

import scala.Tuple2;

public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
       /* if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }*/

        //getFile();
        //String fileName = "/tmp/words.txt";

    }

    public static void countWordsExample() {
        String fileName = "s3a://dpm-test-emr/words.txt";
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
                .setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        //JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<String> lines = ctx.textFile(fileName, 1);

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> wordAsTuple = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordWithCount = wordAsTuple.reduceByKey((Integer i1, Integer i2)->i1 + i2);
        List<Tuple2<String, Integer>> output = wordWithCount.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();

    }

    public static void getFile() {
        String clientRegion = "eu-west-1";
        String bucketName = "dpm-test-emr";
        String key = "words.txt";

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
