# High Throughput Event Driven Registration Pipeline with Async rust(tokio) and Observability  
This is an asynchronous rust application build using tokio. It cnosumes real time registration event streams from redpanda topics and persists batch records to a PostgreSQL database with strict consistency guarantees. 
The platform features fullstack observability with instrumented telemetry using tracing crate for structured logging ans span logging across asynchronous tasks. 
Realtime metrics are collected via prometheus scarpers leveraging actix-web endpoint to monitor the system's health, from grafana dashboard. The whole system is containerized in docker.

### Starting the application
The application process is managed by docker container where we build and run the application as;
```docker build -t landlord-service:latest .```  
We run the container as;  
```docker run -d -p 8180:8180 --name landlord-app landlord-service:latest```
