#!/bin/bash
# Authors: Sri Annapurna, Mayuri, Kavyashree, Kamlendra
# Fill in all the variables according to your configurations

imageid="ami-fce3c696" # this is an Ubuntu AMI, you can change it to whatever you want
instance_type="t2.micro"
key_name="A9KeyPair" # your keypair name -- http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html
security_group="A9SecurityGroup" # your security group id -- http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html
wait_seconds="5" # seconds between polls for the public IP to populate (keeps it from hammering their API)
#port="5222" # the SSH tunnel port you want
key_location="A9KeyPair.pem" # your private key -- http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair
user="ubuntu" # the EC2 linux user name
nodeCount=$1

# maven cleaning
mvn clean compile assembly:single

# moving jar to current location
cp target/Project.jar .

echo "Node Count: " $1

# Removing the existing publicDnsFile
rm publicIp.txt
rm privateIp.txt
rm masterInstance.txt

#Installation commands for Java and AWS CLI4
installJava="mkdir Project; mkdir ~/.aws; sudo apt-get update; sudo apt-get install python-software-properties; sudo add-apt-repository ppa:webupd8team/java; sudo apt-get update; yes | sudo apt-get install oracle-java8-installer; which java"
installAwsCli="sudo apt-get install -y awscli; ls"

# Create instances
aws ec2 run-instances --image-id $imageid --count $nodeCount --instance-type $instance_type --key-name $key_name --security-groups $security_group;

# Get the ip addresses for the active instances
publicIps=`aws ec2 describe-instances | grep PublicIpAddress | grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}"`

# Use an array to reference the instance ids
publicIpList=($publicIps) #PUBLIC

length=${#publicIpList[@]}
echo "Number of instances: "$length

for (( i=0; i<${length}; i++ ));
do
		if [ $i -eq 0 ]; then
			echo "Master Instance running on: " ${publicIpList[i]}
			echo ${publicIpList[i]} >> masterInstance.txt
		fi
			echo ${publicIpList[i]} >> publicIp.txt
done


# SSH into each active instance and install the respective tools
for (( i=0; i<${length}; i++ ));
	do
		echo "Current Instance: " ${publicIpList[i]}
		state=`aws ec2 describe-instances --filters "Name=ip-address,Values=${publicIpList[i]}" | jq -r ".Reservations[].Instances[].State.Name"`
		echo "State: "${state}
		while [ $state != 'running' ]
		do
			echo "Pinging the instance"
			sleep $wait_seconds
			state=`aws ec2 describe-instances --filters "Name=ip-address,Values=${publicIpList[i]}" | jq -r ".Reservations[].Instances[].State.Name"`
		done

		echo "Instance is up and running"
		echo "SSHing into each instance and installing the jdk and awscli"

		ssh -i $key_location ubuntu@${publicIpList[i]} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "${installJava}; ${installAwsCli}"

		# Transferring the credentials, publicDns.txt, A9KeyPair.pem and jar to each instance
		scp -i $key_location -o stricthostkeychecking=no $key_location ubuntu@${publicIpList[i]}:~/Project
		scp -i $key_location -o stricthostkeychecking=no masterInstance.txt ubuntu@${publicIpList[i]}:~/Project
		scp -i $key_location -o stricthostkeychecking=no ~/.aws/credentials ubuntu@${publicIpList[i]}:~/.aws
		scp -i $key_location -o stricthostkeychecking=no Project.jar ubuntu@${publicIpList[i]}:~/Project
done

echo "Finished with SSH Transfer of Files"
