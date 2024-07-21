

# FH TAKE HOME TEST

This project is to build a data pipeline using Python with Pandas or PySpark or SparkSQL to model a users referral program and implement basic business logic to identify and avoid potential fraud. The pipeline will extract 7 csv files of raw data, transform the data, and upload them to a bucket on Google Cloud Storage. The pipeline and the data are containerized using Docker.

<img width="955" alt="Screenshot 2024-07-21 at 17 00 52" src="https://github.com/user-attachments/assets/e0e55edf-1304-44ba-a7e4-0629bf45caea">


  
#### Prerequisites  
- Docker
  
#### Get the Source  
  

    git clone https://github.com/faishalfaye/fh_test.git



#### Build & Run the Container
Make sure your Docker engine is running and your current directory is the same directory as the Dockerfile.
   

    
    # step 1: build a container
    # docker build -t <your container name> .
    docker build -t containerfaishal .

    # step 2: run the container
    # docker run <your container name>
    docker run containerfaishal
	
#### View the Output
Go to the link below to see the output
https://console.cloud.google.com/storage/browser/faishal_bucket

The output will look like this
<img width="1440" alt="Screenshot 2024-07-21 at 17 25 47" src="https://github.com/user-attachments/assets/e5b31168-e4d7-4e01-ac33-c003a7d20d15">