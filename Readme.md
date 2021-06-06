# Batch ETL with Cloud Flower

Cloud Flower is a project which empowers Cloud environment (in this case Im using Google Cloud Platform) to perform Batch Extract Transform Load (ETL)

Cloud Flower stands for:
- **Cloud** which is the production environment
- **Flower** which is Dataflow + Composer

## Data Sources:
1. CSV stored at GCS
2. BigQuery Table

## Tech Stack
1. Cloud Composer
2. Google Cloud Storage
3. BigQuery
4. Dataflow
5. Cloud Build

## Setup
### Composer Environment
Composer is managed Airflow environment on Google Cloud Platform. With Composer you don't need to spend more time on managing your Airflow, you can just focus on writing the DAGs.

Here is how to setup the environment:
1. Activate Composer API if you haven't. You can refer to this link on how to: https://cloud.google.com/endpoints/docs/openapi/enable-api#console
2. Open https://console.cloud.google.com/
3. On Navigation Menu, go to **Big Data > Composer**. Wait until Composer page shown
4. Click **CREATE** button to create new Composer environment
5. Fill some required fields there. This projects use this machine config:
   - Name: cloud-flower
   - Location: asia-south1
   - Node count: 3
   - Zone: asia-south1-a
   - Machine Type: n1-standard-1
   - Disk size (GB): 20 GB
   - Service account: choose your Compute Engine service account.
   - Image Version: composer-1.17.0-preview.1-airflow-2.0.1
   - Python Version: 3
   - Cloud SQL machine type: db-n1-standard-2
   - Web server machine type: db-n1-standard-2
6. Fill the Environment Variable with
   - ENVIRONMENT: production
7. Then, click the **CREATE** button
8. Note that you will need to wait for some minutes until the environment successfully created.
![composer-env](/images/Composer%20Env%20Config.png)

After the environment has been created successfully you will see like this:
![composer-env-success](/images/After%20Success%20Create%20Env.png)

### Airflow webserver on Composer
After the environment successfully created from the previous steps. Now its time to access the airflow web page. Here are the steps:
1. Click the **Airflow webserver link** in Composer Environments list. This will open airflow web page that same as we opened http://localhost:8080 if we using Local Airflow.
2. Create `json` file on your project that stores Airflow variables. Lets call it `variables.json`. The minimum key-value of its file must contains this:
   ```js
    {
      "PROJECT_ID": "",
      "GCS_TEMP_LOCATION": "",
      "BUCKET_NAME": "",
      "ALL_KEYWORDS_BQ_OUTPUT_TABLE": "",
      "GCS_STG_LOCATION": "",
      "MOST_SEARCHED_KEYWORDS_BQ_OUTPUT_TABLE": "",
      "EVENTS_BQ_TABLE": ""
    }
   ```
   - `PROJECT_ID` : your GCP project ID
   - `GCS_TEMP_LOCATION`: temp location to store data before loaded to BigQuery. The format is `gs://` followed by your bucket and object name
   - `BUCKET_NAME` : name of your GCS bucket
   - `ALL_KEYWORDS_BQ_OUTPUT_TABLE` : output table in BigQuery to store keyword search data. The value format is `dataset_id.table_id`
   - `GCS_STG_LOCATION`: staging location to store data before loaded to BigQuery. The format is `gs://` followed by your bucket and object name
   - `MOST_SEARCHED_KEYWORDS_BQ_OUTPUT_TABLE`: output table in BigQuery to store most searched keyword data. The value format is `dataset_id.table_id`
   - `EVENTS_BQ_TABLE`: output table in BigQuery to store event data from reverse engineering result. The value format is `dataset_id.table_id`
   
3. Go to **Admin > Variables**
4. **Choose File** then click **Import Variables**
![airflow-variables](images/Import%20Variable.png)

### BigQuery
1. On your GCP console, go to BigQuery. You can find it on **Big Data > BigQuery**
2. Then create your Dataset ![create-dataset](/images/Create%20Dataset%20menu.png)
3. Fill the Dataset field such as. In this project we only need to set:
   - **Data set ID** (example: my_first_bigquery)
   - **Data location**. Choose the nearest one from your location.
  ![fill-dataset](/images/Create%20BigQuery%20Dataset.png)
4. Click `CREATE DATA SET`
5. Ensure that your dataset has been created
![ensure-dataset-created](/images/Ensure%20Created.png)

### Google Cloud Storage
1. Back to your GCP console, choose Cloud Storage. You can find it on **Storage > Cloud Storage**
2. Click `CREATE BUCKET` button. Then fill some fields such as:
   - Name your bucket (example: blank-space-de-batch1)
   - Choose where to store your data
     - I would suggest to choose **Region** option because it offers lowest latency and single region. But if you want to get high availability you may consider to choose other location type options.
     - Then choose nearest location from you
   - Leave default for the rest of fields.
3. Click `CREATE`
4. Your bucket will be created and showed on GCS Browser
![success-create-bucket](/images/Success%20Create%20Bucket.png)

### Dataflow
From [Google Dataflow Docs](https://cloud.google.com/dataflow). The definition is:
> *Fully managed streaming analytics service that minimizes latency, processing time, and cost through autoscaling and batch processing.*

We can run Dataflow Jobs using our defined Apache Beam script or by using templated jobs which you can see more here: https://cloud.google.com/dataflow/docs/concepts/dataflow-templates . In this case, I created Apache Beam script then it triggered by `BeamRunPythonPipelineOperator` from Airflow

Dataflow Jobs:
![dataflow-job-list](images/Dataflow%20Jobs.png)

One of Dataflow Pipeline:
![dataflow-job-pipeline](images/Dataflow%20Jobs%20Pipeline.png)

### Cloud Build
Cloud Build is a CI/CD pipeline services in Google Cloud Platform. I used Cloud Build to auto synchronize between my Github Repository with GCS Bucket that stores Cloud Composer data. Thus I dont need to copy each files to the bucket manually everytime I did some changes. Here is how to setup it:
1. On GCP console, go to **Tools > Cloud Tasks**
2. Click **CREATE TRIGGER** button
3. Fill some required fields.
   - Name: your trigger name. This will be visible to your repository
   - Description: your trigger description 
   - Event: in this project I used **Push new tag**
   - Source: 
     - Connect to repository
![connect-repo](images/Connect%20Repository%20Cloud%20Build.png)
     - Tag: what tag pattern to be recognized to trigger the build
   - Configuration
     - Type: Cloud build configuration (YAML or JSON)
     - Location: Repository
     - Cloud Build configuration file location: cloudbuild.yaml
4. Dont forget to fill Variable on **Advanced** section. In this project, I set `_GCS_BUCKET` variable with Composer's GCS Bucket name which can be accessed on `cloudbuild.yaml` by `${_GCS_BUCKET}`
![cloud-build-variable](images/Add%20Cloud%20Build%20Variable.png)
5. Click **CREATE** after all needed fields filled 

![cloud-build-config](images/Cloud%20Build%20Config.png)

If your Cloud Build runs successfully, you will see **green mark** and when you click those icon. You will see this:
![success-cloud-build](images/Google%20Cloud%20Build%20Run%20Successfully.png) 

## BigQuery Output Table
This ETL process will produces 3 tables in BigQuery, which is:
1. event
   - This table contains event table as result from reverse engineering process to backfill/restore some missing data at transactional table from data warehouse table. 
2. keyword_searches
   - This table contains information about user's keyword search on platform from 10 March until 15 March 2021. I used partitioned table on this table with `Day` as the partition type.
3. most_searched_keywords
   - This table contains most searched keyword from keyword_searches table 

Output: <br>
![bq-output](images/BigQuery%20Output%20Table.png)