# BCG Case Study
This case study was executed on a local spark cluster. 

The main etl job can be executed using the below spark submit command.

$SPARK_HOME/bin/spark-submit --py-files dependencies.zip \
--files configs/file_configs.json jobs/job.py

The project has been divided into three components:
1. Extract -> Reading all the datasets required for analysing the data
2. Transfom -> All the analysis/transformations are done once the data is read.
3. Load -> Writing the processed data back as CSV

All the configs resides under configs/file_configs.json.

Input Data Location: "Data/IP_Data"
Output Data Location: "Data/OP_Data"

The main job definition/flow has been defined in the file present at location "jobs/job.py".


The case study code has also been solved using using ipython notebook with the derived results. 

