#!/bin/bash
# Arguments: mode
mode=$1

echo "Mode: "$mode

# the file that contains the public DNS addresses of the linux boxes that we just started
globalIpFile=publicIp.txt

startMaster="cd ~/Project; java -cp Project.jar com.mr.hw.cluster.master.Master $mode > log.txt 2>&1"
#startClient="cd ~/Project; java -cp Cluster-0.0.1-SNAPSHOT.jar TestClient > log.txt 2>&1"
startClient="java -cp Project.jar com.mr.hw.client.TestClient"
startSlave="cd ~/Project; java -cp Project.jar com.mr.hw.cluster.slave.Slave $mode > log.txt 2>&1"

i=0

# Reads each line from the publicDns.txt file
while read line;do
        if [ $i -eq 0 ]; then
        	server=$line
          echo "node id : "$i
          echo "Starting the Master Instance"
          `chmod 600 A9KeyPair.pem`
          `ssh -i A9KeyPair.pem ubuntu@$line -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "${startMaster}"` < /dev/null &
        else
        	echo "Starting the Slave Instance"
          echo "node id : "$i
          `chmod 600 A9KeyPair.pem`
          `ssh -i A9KeyPair.pem ubuntu@$line -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "${startSlave}"` < /dev/null &
        fi
        i=$((i+1))
done < $globalIpFile

echo "Submitting the job via the client"
echo "Master Server: "$server

sleep 10s

$startClient

wait

exit
