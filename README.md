Fetch Rewards Data Engineering Take Home: ETL off an SQS Queue
Overview
This project demonstrates an ETL (Extract, Transform, Load) process where JSON data containing user login behavior is read from an AWS SQS queue, transformed to mask Personally Identifiable Information (PII), and then written to a PostgreSQL database.
Project Structure
- masking.py: The main script containing the ETL logic.
- docker-compose.yml: Docker Compose file to set up LocalStack and PostgreSQL services.
- requirements.txt: List of Python dependencies.
Prerequisites
- Docker
- Docker Compose
- awscli-local (for local AWS CLI commands)
- psql (PostgreSQL command-line tool)
- Python 3.8+
Setup
1. Clone the repository:
   git clone <repository-url>
   cd <repository-directory>
2. Install dependencies:
   pip install -r requirements.txt
3. Start Docker services:
   docker-compose up -d
4. Verify services are running:
   - LocalStack: http://localhost:4566
   - PostgreSQL:
     psql -d postgres -U postgres -p 5432 -h localhost -W
5. Fetch messages from SQS queue:
   awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue
6. Verify PostgreSQL table:
   psql -d postgres -U postgres -p 5432 -h localhost -W
   SELECT * FROM user_logins;
Running the ETL Process
Run the ETL script:
python masking.py
This will start reading messages from the SQS queue, transform the data to mask PII, and write the records to the PostgreSQL database.
Further Steps for Production
- Deployment: For production, the application can be containerized and deployed on Kubernetes or any container orchestration service. Use AWS SQS and RDS for managed services.
- Monitoring and Logging: Implement logging using tools like AWS CloudWatch, ELK stack, or Prometheus to monitor application performance and errors.
- Scalability: Use AWS Lambda for consuming messages from SQS and process them in parallel to handle a growing dataset efficiently.
- Security: Store PII in a separate, secure database if needed. Use AWS KMS to encrypt data at rest and in transit.
- Data Recovery: Keep a reference table of hashed PII to original values, secured and accessible only by authorized personnel, to recover PII if needed.
Assumptions
- The SQS queue and PostgreSQL table schemas are predefined and do not change frequently.
- Data volume is manageable within the constraints of the local setup for testing.
