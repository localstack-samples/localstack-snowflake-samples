FROM python:3.8.19-bullseye
LABEL maintainer="LocalStack <info@localstack.cloud>"
LABEL application="localstack-credit-card-fraud-detection"

# Set build arguments.
ARG DEBIAN_FRONTEND=noninteractive

# Get package lists, important for getting 'curl' and such.
RUN apt-get -y update

# Install Python dependencies.
RUN pip3 install pip-tools

# Install all requirements, and only update when the requirements.txt updates.
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Setup workdirectory.
RUN mkdir /app
WORKDIR /app

# Run commands.
CMD [ "exec", "\"@\"" ]
