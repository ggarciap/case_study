FROM postgres:latest

# install Python 3
RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get -y install python3.7-dev
RUN apt-get install postgresql-server-dev-10 gcc python3-dev musl-dev

RUN pip3 install psycopg2
RUN pip3 install pandas 
RUN pip3 install numpy 
RUN pip3 install sqlalchemy

USER postgres

# expose Postgres port
EXPOSE 5432

# bind mount Postgres volumes for persistent data
VOLUME ["/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql"]