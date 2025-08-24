FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    git \
    wget \
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN TERRAFORM_VERSION=1.12.2 \
    && curl -fsSL "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" -o terraform.zip \
    && unzip terraform.zip \
    && chmod +x terraform \
    && mv terraform /usr/local/bin/terraform \
    && rm terraform.zip \
    && ls -la /usr/local/bin/terraform \
    && terraform version

RUN curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-482.0.0-linux-x86_64.tar.gz -o gcloud.tar.gz \
    && tar -xzf gcloud.tar.gz \
    && ./google-cloud-sdk/install.sh --quiet --path-update=true \
    && rm gcloud.tar.gz

ENV PATH="/google-cloud-sdk/bin:/usr/local/bin:${PATH}"

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir dbt-core>=1.7.19 dbt-bigquery==1.7.*

ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app/dbt

RUN echo "=== Installed Tools Check ===" \
    && which terraform && terraform version \
    && which gcloud && gcloud version --quiet \
    && which python && python --version \
    && which dbt && dbt --version