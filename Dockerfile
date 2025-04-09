FROM mysql:8.0

ENV MYSQL_ROOT_PASSWORD=password
ENV MYSQL_DATABASE=employees

RUN microdnf install -y wget unzip

WORKDIR /tmp

# download data repository 
RUN wget https://github.com/datacharmer/test_db/archive/master.zip && \
    unzip master.zip && \
    mkdir -p /docker-entrypoint-initdb.d/test_db && \
    cp -r /tmp/test_db-master/* /docker-entrypoint-initdb.d/test_db/ && \
    rm -rf /tmp/master.zip /tmp/test_db-master

# script to load data
COPY <<-"EOT" /docker-entrypoint-initdb.d/01-load-employees.sh
#!/bin/bash
while ! mysqladmin ping -h"localhost" -u"root" -p"$MYSQL_ROOT_PASSWORD" --silent; do
    sleep 1
done

echo "Charging database employees"
cd /docker-entrypoint-initdb.d/test_db
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < employees.sql

echo "Test data"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -t < test_employees_md5.sql

mkdir -p /tmp/test_db_backup
mv *.sql /tmp/test_db_backup/ 2>/dev/null || true
mv *.sh /tmp/test_db_backup/ 2>/dev/null || true
mv *.dump /tmp/test_db_backup/ 2>/dev/null || true
EOT

#  delete adicional scripts and dependencies
RUN chmod +x /docker-entrypoint-initdb.d/01-load-employees.sh && \
    microdnf remove -y wget unzip && microdnf clean all

EXPOSE 3306
