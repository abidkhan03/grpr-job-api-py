FROM python:3.8

RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY ./requirements.txt .
RUN pip3 install --upgrade pip
RUN pip install -r requirements.txt
COPY . .
RUN chmod +x /usr/src/app/scripts/meta-docker.sh
RUN /usr/src/app/scripts/meta-docker.sh
WORKDIR /usr/src/app/server
RUN python manage.py migrate
ENTRYPOINT ["/usr/src/app/scripts/docker-entrypoint.sh"]
Expose 8000
