# debina image
FROM debian:bullseye-slim

# about the custom image
LABEL maintainer="datadiver.dev"
LABEL version="0.1"
LABEL description="PostgreSQL, Conda Environment and Jupyter Lab"

# root user
USER root:root

# install deb essential packages
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates wget curl gnupg2 lsb-release sudo

# install public keys
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    curl https://repo.anaconda.com/pkgs/misc/gpgkeys/anaconda.asc | gpg --dearmor > conda.gpg && \
    install -o root -g root -m 644 conda.gpg /usr/share/keyrings/conda-archive-keyring.gpg && \
    gpg --keyring /usr/share/keyrings/conda-archive-keyring.gpg --no-default-keyring --fingerprint 34161F5BF5EB1D4BFBBB8F0A8AEB4F8B29D82806

# add debian repositories
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/conda-archive-keyring.gpg] https://repo.anaconda.com/pkgs/misc/debrepo/conda stable main" > /etc/apt/sources.list.d/conda.list && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -c -s)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list

# install postgres and conda
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update && apt-get install -y conda postgresql postgresql-client postgresql-contrib

# create user
ENV USER=student
ENV HOME=/home/$USER
RUN groupadd -g 999 $USER && useradd -u 999 -g $USER -G sudo -m -s /bin/bash $USER && \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "${USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# set init script
RUN mkdir -p /opt/app/bin
COPY ./docker/init.sh ./docker/init-jupyter.sh /opt/app/bin/
RUN chmod +x /opt/app/bin/*.sh && \
    chown -R $USER:$USER /opt/app/bin

# set user environment
USER $USER
WORKDIR $HOME
SHELL ["/bin/bash", "--login", "-c"]

# create conda environment
COPY ./docker/environment.yml .
RUN . /opt/conda/etc/profile.d/conda.sh && \
    conda env create -f environment.yml

# set user .bashrc
RUN echo "source /opt/conda/etc/profile.d/conda.sh" >> $HOME/.bashrc && \
    echo "conda activate py310" >> $HOME/.bashrc

# create project workspace
RUN mkdir -p $HOME/.jupyter $HOME/workspace
COPY ./docker/jupyter_notebook_config.py $HOME/.jupyter/jupyter_notebook_config.py
WORKDIR $HOME/workspace

# copy project files
RUN mkdir -p ./data/ ./docs/ ./images/
COPY ./data/ ./data/
COPY ./docs/ ./docs/
COPY ./images/ ./images/
COPY ./src/etl ./
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
EXPOSE 80

# add volumes to allow backup of config, logs and databases
VOLUME  [ "/etc/postgresql", "/var/log/postgresql", "/var/lib/postgresql" ]

USER $USER 

# container init script
CMD [ "/opt/app/bin/init.sh" ]
