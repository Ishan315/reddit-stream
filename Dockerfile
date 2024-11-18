FROM python:3.9-slim

# Install Java and netcat (for health checks)
RUN apt-get update && \
    apt-get install -y default-jdk netcat-traditional curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME correctly for Debian
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install spaCy model
RUN python -m spacy download en_core_web_sm

# Copy the application
COPY src/ src/
COPY .env .

# Create script to check Kafka
# RUN echo '#!/bin/bash\n\
# while ! nc -z kafka 9093; do\n\
#   echo "Waiting for Kafka..."\n\
#   sleep 1\n\
# done\n\
# echo "Kafka is up - executing command"\n\
# exec "$@"' > /wait-for-kafka.sh && chmod +x /wait-for-kafka.sh
COPY wait-for-kafka.sh /wait-for-kafka.sh
RUN chmod +x /wait-for-kafka.sh

# Verify Java installation
RUN java -version && \
    echo "JAVA_HOME=$JAVA_HOME" && \
    ls -l $JAVA_HOME/bin/java

# Set the entrypoint
ENTRYPOINT ["/wait-for-kafka.sh"]
# After the Kafka wait script
COPY wait-for-elasticsearch.sh /wait-for-elasticsearch.sh
RUN chmod +x /wait-for-elasticsearch.sh 
