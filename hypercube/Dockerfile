FROM jupyter/pyspark-notebook:latest

ARG appdir=hypercube
WORKDIR /app

COPY ${appdir}/app/requirements.txt .
RUN pip install -r requirements.txt

COPY ${appdir}/app .

# Create kafka user and add the user to sudoers
USER root
RUN useradd -ms /bin/bash hypercube
RUN adduser hypercube sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN apt update
RUN apt install -y redis-tools

# Create kafkahub logs directory
RUN mkdir -p /logs/hypercube
RUN chmod -R 775 /logs/hypercube
RUN mkdir -p /opt/app
RUN chown -R hypercube:hypercube /opt/app
RUN mkdir -p /temp/hypercube/
RUN chmod -R 777 /temp/hypercube/

CMD ["/app/entrypoint.sh"]
