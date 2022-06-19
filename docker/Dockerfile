# debina image
FROM debian:bullseye-slim

# about the custom image
LABEL maintainer="datadiver.dev"
LABEL version="0.1"
LABEL description="Postgres, Python, Jupyter Lab and Streamlit"

# root user
USER root:root

# install deb essential packages and repositories
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates wget curl gnupg2 lsb-release sudo && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -c -s)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y postgresql postgresql-client postgresql-contrib python3-pip python3-dev python3-setuptools
    
# create user
ENV USER=developer
ENV HOME=/home/$USER
ENV PATH=$PATH:$HOME/.local/bin
RUN groupadd -g 1000 $USER && useradd -u 1000 -g $USER -G sudo -m -s /bin/bash $USER && \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "${USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# set init script
RUN mkdir -p /opt/app/bin
COPY ./docker/init.sh ./docker/init-jupyter.sh ./docker/init-streamlit.sh /opt/app/bin/
RUN chmod +x /opt/app/bin/*.sh && \
    chown -R $USER:$USER /opt/app/bin

# set user environment
USER $USER
WORKDIR $HOME
SHELL ["/bin/bash", "--login", "-c"]
RUN echo "alias python='python3'" >> $HOME/.bash_aliases

# install python libraries
COPY ./docker/requirements.txt .
RUN pip3 install -r requirements.txt

# create project workspace
RUN mkdir -p $HOME/.jupyter $HOME/workspace
COPY ./docker/jupyter_notebook_config.py $HOME/.jupyter/jupyter_notebook_config.py
WORKDIR $HOME/workspace

# copy project files
RUN mkdir -p ./.streamlit/ ./data/ ./docs/ ./images/ ./src
COPY ./.streamlit/ ./.streamlit/
COPY ./data/ ./data/
COPY ./docs/ ./docs/
COPY ./images/ ./images/
COPY ./src/ ./src/
COPY ./README.md ./
RUN sudo chown -R $USER:$USER $HOME


# set postgres environment variables
ENV POSTGRES_DB=studentdb
ENV POSTGRES_USER=student
ENV POSTGRES_PASSWORD=student

# set postgres user
USER postgres

# create default postgres user and database
RUN /etc/init.d/postgresql start && \
    psql --command "CREATE USER ${POSTGRES_USER} WITH SUPERUSER PASSWORD '${POSTGRES_PASSWORD}';" && \
    createdb -O $POSTGRES_USER $POSTGRES_DB

# set postgres configuration
RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/14/main/pg_hba.conf && \
    echo "listen_addresses='*'" >> /etc/postgresql/14/main/postgresql.conf

# server ports
EXPOSE 5432
EXPOSE 8080
EXPOSE 80

# add volumes to allow backup of config, logs and databases
VOLUME  [ "/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql" ]

USER $USER 

# container init script
CMD [ "/opt/app/bin/init.sh" ]
