#########################     README    ############################# 

-------------------------------------------------------
Project: Implement MapReduce Framework
- By Kamlendra, Mayuri, Sri and Kavyashree 
-------------------------------------------------------

#NOTE: MAKE SURE YOU ARE IN THE “Cluster” FOLDER WHEN YOU RUN THE COMMANDS FROM TERMINAL

-------------------------------------------------------
1. Software
-------------------------------------------------------
 
* JDK version 1.8
* AWS Command Line Interface 
* Maven
* JQ Tool

-------------------------------------------------------
2. Configuration for AWS
-------------------------------------------------------
 
* Configure the authentication keys using 'aws configure' command. 
* Make sure that the AWS credential file is present at ~/.aws/credentials'

-------------------------------------------------------
3. Environment variables
-------------------------------------------------------
 
* Check that the following environment variables are set:$JAVA_HOME, $MAVEN_HOME are set

-------------------------------------------------------
4. Project
-------------------------------------------------------

The project file contains the following:
1. start-cluster.sh, sort.sh, stop-cluster.sh : Shell scripts which have commands to create and destroy EC2 instances on AWS.
2. Cluster : A maven project which contains the code and jar file that gets sent to all the instances.
3. Report.pdf : A detailed report of the project which includes the Approach used, Implementation logic, and function of each module.

-------------------------------------------------------
5. Shell Scripts
------------------------------------------------------- 
1. start-cluster.sh : This script takes the total number of nodes as the input.
	Ex : sh start-cluster.sh 3  (This will create one master node and two sort nodes)
2. sort.sh : This script will take mode, <input bucket name> and <output bucket name> 
	Ex : sh sort.sh "aws" s3://cs6240sp16/climate s3://your-bucket/output
	Mode can be "aws" or "local"
3. stop-cluster.sh : This script will stop the instances after the execution is completed.
	Ex : sh stop-cluster.sh 
 
-------------------------------------------------------
6. Requirements:
-------------------------------------------------------
+ Make sure you are running this assignment on a Linux Machine
+ Input files have to be present in “input” folder and output files have to be present in “output” folder.
+ Only String type is expected(input) and produced(output) in map and reduce tasks.
+ Terminate all the active instances before running this Assignment.
+ Make sure you are in the Code folder :
  a. Create a keypair using the folowing command which generates a A9KeyPair.pem file:

	aws ec2 create-key-pair --key-name A9KeyPair --query 'KeyMaterial' --output 	text > A9KeyPair.pem

	#Modify the permissions to make it publicly viewable
	chmod 400 A9KeyPair.pem
  b. Create a security group:
	aws ec2 create-security-group --group-name A9SecurityGroup --description "A9 security group" --output text > A9SecurityGroup
  c. Assign rules to your security groups:
	aws ec2 authorize-security-group-ingress --group-name A9SecurityGroup --protocol tcp --port 22 --cidr 0.0.0.0/0

	aws ec2 authorize-security-group-ingress --group-name A9SecurityGroup --protocol tcp --port 21 --cidr 0.0.0.0/0

	aws ec2 authorize-security-group-ingress --group-name A9SecurityGroup --protocol udp --port 1210 --cidr 0.0.0.0/0

	aws ec2 authorize-security-group-ingress --group-name A9SecurityGroup --protocol tcp --port 1210 --cidr 0.0.0.0/0

Also, make sure you have one rule that allows all traffic.

   d. In the start-cluster.sh, sort.sh and stop-cluster.sh files, change the variable names according to your configuration.
 
-------------------------------------------------------
7. Execution:
-------------------------------------------------------

Steps to run the project:

1. Run start-cluster.sh 
2. Run sort.sh file
3. Run stop-cluster.sh