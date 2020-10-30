
        Copyright 2020 Google LLC

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.


#Demo - Apache Flink on Google Cloud Dataproc

##Description
In this demo, we set up a Dataproc Cluster with Flink enabled on it. We also set up a single node Apache Kafka cluster 
for demo purpose. Functionally, we read a real time (rate-restricted) market data feed for BTC - USD trading. From the 
real time stream, we plot the average traded volumes and trade sizes on windows of 5 mins sliding every 5 secs. We use 
Redis to store the data and visualise on Grafana. Hence, we also set up Redis and Grafana for this demo.

##Setup
###Flink on Dataproc
TODO Enable Dataproc API

```
PROJECT=my-test-project
CLUSTER_NAME=demo-flink-cluster-1
REGION=us-central1
ZONE=us-central1-a

gcloud dataproc clusters create ${CLUSTER_NAME} 
 --region ${REGION} \ 
 --zone ${ZONE} \
 --master-machine-type n1-standard-4 \ 
 --num-workers 2 \
 --worker-machine-type n1-standard-4 \ 
 --scopes 'https://www.googleapis.com/auth/cloud-platform' \ 
 --image-version 1.5-debian10 \
 --optional-components=FLINK \
 --metadata flink-start-yarn-session=true \ 
 --enable-component-gateway \
 --project ${PROJECT}
```
 
###Single node Kafka
Follow the instruction at [this blog post](https://medium.com/google-cloud/setting-up-a-small-kafka-server-on-google-cloud-platform-for-testing-purposes-9958a47ea8b9)

###Redis
TODO Enable Redis API

Create a managed Redis instance on Cloud Memorystore with 5GB storage 
```
REDIS_INSTANCE_ID=flink-demo
REGION=us-central1
ZONE=us-central1-a

gcloud redis instances create ${REDIS_INSTANCE_ID} \
  --size=5 --region=${REGION} --zone=${ZONE} --redis-version=redis_5_0
```

###Grafana
gcloud compute instances create grafana-7 --description="Grafana 7 VM" --zone $ZONE --tags http-server,https-server
gcloud compute ssh grafana-7 --zone=$ZONE
sudo apt-get install -y adduser libfontconfig1
curl -o grafana_7.3.1_amd64.deb https://dl.grafana.com/oss/release/grafana_7.3.1_amd64.deb
sudo dpkg -i grafana_7.3.1_amd64.deb
sudo /bin/systemctl start grafana-server

sudo grafana-cli plugins install redis-datasource
sudo /bin/systemctl restart grafana-server

SOCKS5 Proxy
gcloud compute ssh --zone $ZONE grafana-7 -- -N -p 22 -D localhost:1080

Set up SOCKS5 proxy on browser (host: localhost, port: 1080)
Go to http://grafana-7:3000
Add data source -> Redis
Name: Redis-flink-demo
Address: redis://<Cloud Memorystore Redis instance internal IP>:6379
Save and Test


##Demo
###Enable real time streaming into Kafka



Open Cloud Shell


###Start Flink application with Kafka source and Redis sink

###Start Redis monitor

###Set up Grafana dashboard



------Remove later
Go to https://pantheon.corp.google.com/marketplace/details/google/grafana
Select your project at the top of the page
Clink on the `Configure` button

Next screen:
Select the zone (us-central1-a)
Click `Create Cluster` to create the underlying GKE cluster
Wait for the cluster creation to be complete

Then provide a suitable app instance name e.g. grafana-flink-demo
Select `Create a new service account` for "Grafana Service Account"
Select `Create a new storage class` for StorageClass
Grafana application storage size: `2Gi`

Click `Deploy`

Configure local access to Grafana UI (for demo, alternatively external access can be enabled)
gcloud container clusters get-credentials cluster-1 --zone us-central1-a

APP_INSTANCE_NAME=grafana-1
NAMESPACE=default
To expose Grafana with a publicly available IP address, run the following command:
kubectl patch svc "${APP_INSTANCE_NAME}-grafana" \
  --namespace "${NAMESPACE}" \
  -p '{"spec": {"type": "LoadBalancer"}}'
  
It might take a while for the service to be publicly available. After the process is finished, get the public IP address with:

SERVICE_IP=$(kubectl get svc ${APP_INSTANCE_NAME}-grafana \
  --namespace ${NAMESPACE} \
  --output jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "http://${SERVICE_IP}:3000/"


Install redis datasource plugin
kubectl exec -it grafana-1-grafana-0 -n default -- grafana-cli --pluginUrl https://grafana.com/api/plugins/redis-datasource/versions/1.1.2/download plugins install redis-datasource

Delete pod to restart
kubectl delete pod grafana-1-grafana-0 -n default

