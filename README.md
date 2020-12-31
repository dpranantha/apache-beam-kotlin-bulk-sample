1. Running example using Direct Runner via Apache Maven:
    
   a. With public dataset in GCS
    
        mvn compile exec:java -Dexec.mainClass=com.dpranantha.WordCount \
        -Dexec.args="--inputFile=gs://apache-beam-samples/shakespeare/kinglear.txt --output=results/counts" -Pdirect-runner

   b. With default input 

        mvn compile exec:java -Dexec.mainClass=com.dpranantha.WordCount \
        -Dexec.args="--output=results/counts" -Pdirect-runner

2. Running example on top of Google Dataflow via Apache Maven

        mvn compile exec:java -Dexec.mainClass=com.dpranantha.WordCount \
        -Dexec.args="--runner=DataflowRunner --gcpTempLocation=gs://YOUR_GCS_BUCKET/tmp \
                      --project=YOUR_PROJECT --region=GCE_REGION \
                      --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://YOUR_GCS_BUCKET/counts" \
        -Pdataflow-runner
   
   To run in GCP, ideally we need to publish docker image (dockerize the fat jar) to Google container registry and add either kubernetes deployment or
   kubernetes batch job.

3. Running integration tests via Apache Maven:
      
      mvn clean verify -Dskip.failsafe.tests=false
