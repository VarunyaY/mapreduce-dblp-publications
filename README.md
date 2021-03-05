Varunya Yanamadala
Masters Student in Computer Science
10/22/2020

Introduction
The homework consists of creating map reduce tasks in order to produce a high level understanding on the dblp dataset:https://dblp.uni-trier.de. usign hadoop framework.

Setup Requirements
1. To check the logic the mapreduce jobs could be first run in IntelliJ by setting up program parameters to the imput_dir and out_dir. Also, make sure the library dependecies in build.sbt has the hadoop.core in it.
2. Once, it runs with out any errors, thid could be deployed to the Hortonworks Sandbox Virtual Machine. The HDP could be setup in vmWare Fusion or Workstation. Make sure atleast 10gb RAM is allocated to the VM.
3. The last step is to host the jar file in a cluster in Amazon EMR. 

Each of the above is discussed in the next steps.

Setup of the repo in IntelliJ IDE
Clone the git repo using: git clone https://varunya@bitbucket.org/cs441-fall2020/varunya_yanamadala_hw2.git onto IntelliJ IDE with scala plugin installed. 1. In IntelliJ this can be done through File-> New-> Project from Version Control. 2. Select Version control as Git, provide the above git URL and the directory path to clone it. 3. Click clone.

Deploying on vmware
1. Once the HDP for vmWare is downloaded and vmWare Fusion is installed in the system, import HDP VM into Fusion.
2. Once HDP VM is started, URLs are displayed to access the dashboard through the browser.

In the browser:
1. CLick on advanced links and the Web Shell Client.
2. On the screen to enter the username and password - provide root and hadoop respectively. After first time login password has to be reset.
3. Now, give ambari-server start to start the ambari server.
4. Now, you could access the Ambari server dashboard from the HortonWorks Sandbox homepage.
5. Start the required services - mainly YARN, MapReduces2 and HDFS for the tasks discussed further.

MapReduce Jobs
There are total of 5 tasks in the homework and the output format for each of them is as follows:
(To check for the outputs each of the tasks was performed on smaller dataset of dblp.xml)
Task1 : To find the top ten published authors at each venue. Output: Venue,(Author,Number of publications) 
Task2: To find the list of the authors without any interruption for N years. Output: Name1, Name2...
Task3: To find the list of publications with only one author. Output: Venue, Publication1, Publications2...
Task4: To find the list of publications for each venue that contain highest number of authors. Output: Venue, Publication, Number of Authors
Task5a: To get the top 100 list of authors with most number of co-authors. Output: For this the output is the last line in file which of form: Top 100 authors, List((Author1, Number of co-authors),(Author2, Number of co-authors)....)
Task5b: To get the top 100 list of authors without any co-author. Output: The last line in the output file is the required output which is of the form: Top 100 Authors with no Co-Authors, List((Author1,1), (Author2,1).....)

The output corresponding to each of the above tasks are in the folder job1, job2, job3,job4 for task1, task2, task3, task4 respectively.
For task5a and task5b the output files are in job6 and job 7 folders.

The output folders are in output_dir folder. The final output files are the part files for the corresponding tasks.

Deploying into HDP:

Now, when the code is ready to be deployed onto VM follow the below steps.
1. Start the HDP Sandbox.
2. Copy the jar file to the Sandbox by issuing the following command: "scp -P 2222 target/scala-2.13/varunya\_yanamadala\_hw2.jar root@172.16.7.129:/root" (the IP address corresponds to the sandbox IP. This would vary for each)
3. Copy the dblp.xml to the Sandbox: "scp -P 2222 dblp.xml root@172.16.7.129:/root"
4. Login into the Sandbox: ssh -p 2222 root@172.16.7.129. The default password is: hadoop. Provide the recent password if you have changed in the before steps while starting the ambari-server.
5. Create the input directory on HDFS: hdfs dfs -mkdir input_dir
6. Load the dataset on HDFS: hdfs dfs -put dblp.xml input_dir/
7. You can now launch the job: hadoop jar varunya_yanamadala_hw2.jar
8. After completion the results are saved in a folder named output_dir on HDFS, you can copy them to local storage by issuing the following command: "hdfs dfs -get output_dir output"
9. Exit from the SSH terminal, "exit"

While running the jar file I faced issues writing the data to the output_dir folder. So, I modified the access permissions of the root by issuing 
sudo su hdfs
hdfs dfs -chown root /user
hdfs dfs -chmod 777 /user/root


Test cases:
Testing was performed on UninterruptedAuthorsReducer and VenuePublicationsHighestAuthorsReducerTest.

XML Parsing:
XML Parsing of the input dataset was done using an implementation putforward by Apache Mahout at the following URL: https://github.com/apache/mahout/blob/758cfada62556d679c445416dff9d9fb2a3c4e59/community/mahout-mr/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java

AWS EMR:
Steps to be followed while deploying into AWS EMR:
(I ran a job using my Amazon educate account.)
1. Under services select EMR and click on create a cluster.
2. Select advanced options and provide the name for the cluster.
3. Click next provide 20 GB(so that the gz file can be unzipped) EBS root volume. Click next until you get an option to select EC2 key-pair.
4. Click create cluster.
5. Now select S3 under services and upload the jar file.
6. Now, in the clusted select security group for master and add SSH anywhere and TCP 8088 anywhere in the inbound rules.
7. Once this is complete, SSH to the aws server from local using the master public DNS available under summary tab.
8. Upload the input file using "wget <URL of the input file>" and unzip it using "gunzip dblp.xml".
9. Now, move the input file to the input_dir by creating one using the below commands:
hdfs dfs -mkdir input_dir
hdfs dfs -put dblp.xml input_dir/
10. Now, create a step by going to the steps tab and add step by selecting the appropriate jar file from the location. The job should be running now and once it is complete 
the results can be checked by running the command hdfs dfs -ls output_dir and traversing to a particular folder and running the command:
hdfs dfs -cat output_dir/job1/part-r-00000.



