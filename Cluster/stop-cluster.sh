#!/bin/bash
# Authors: Sri Annapurna Bhupatiraju
# Script to terminate running ec2 instances

instance_type="t2.micro"

echo "Terminating the ec2 instances that are currently active"

# Retrieve the instance ids of the current running ec2 instances
instanceIds=`aws ec2 describe-instances --filters "Name=instance-type,Values=$instance_type" | jq -r ".Reservations[].Instances[].InstanceId"`

# Putting the instanceIds into an array
instanceIdList=($instanceIds)

length=${#instanceIdList[@]}

for (( i=0; i<${length}; i++ ));
do
		echo "Terminating Instance: "${instanceIdList[i]}
		aws ec2 terminate-instances --instance-ids ${instanceIdList[i]}
done

echo "All active instances have been terminated successfully"
