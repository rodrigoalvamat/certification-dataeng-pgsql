#!/bin/bash

# init jupyter server
sudo -b -u "$USER" -s /opt/app/bin/init-jupyter.sh

# start postgres server
sudo -u postgres -s /usr/lib/postgresql/14/bin/postgres -D /var/lib/postgresql/14/main --config-file=/etc/postgresql/14/main/postgresql.conf
