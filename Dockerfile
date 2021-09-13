FROM debian:stable

# Identify the maintainer of an image
LABEL maintainer="contact@openchia.io"

# Update the image to the latest packages
RUN apt-get update && apt-get upgrade -y

RUN apt-get install python3-virtualenv python3-yaml python3-aiohttp libpq-dev git vim procps net-tools iputils-ping apache2-utils simpleproxy -y

EXPOSE 8088

RUN mkdir -p /root/pool

WORKDIR /root/pool

COPY ./requirements.txt .
RUN virtualenv -p python3 venv
RUN ./venv/bin/pip install -r requirements.txt

COPY ./pool /root/pool/pool/
COPY ./hooks /root/pool/hooks/

COPY ./docker/start.sh /root/

CMD ["bash", "/root/start.sh"]
