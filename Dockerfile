FROM ubuntu:18.04

# Note: Latest version of kubectl may be found at:
# https://aur.archlinux.org/packages/kubectl-bin/
ENV KUBE_LATEST_VERSION="v1.8.3"
# Note: Latest version of helm may be found at:
# https://github.com/kubernetes/helm/releases
ENV HELM_VERSION="v2.13.1"
ENV FILENAME="helm-${HELM_VERSION}-linux-amd64.tar.gz"

RUN apt-get update

RUN apt-get install -y --no-install-recommends apt-utils \
    && apt-get install -y ca-certificates \
    && apt-get install -y curl \
    && apt-get install -y bash \
    && curl -L https://storage.googleapis.com/kubernetes-release/release/${KUBE_LATEST_VERSION}/bin/linux/amd64/kubectl -o /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && curl -L http://storage.googleapis.com/kubernetes-helm/${FILENAME} -o /tmp/${FILENAME} \
    && tar -zxvf /tmp/${FILENAME} -C /tmp \
    && mv /tmp/linux-amd64/helm /bin/helm \
    && rm -rf /tmp/*

# apt-get and system utilities
RUN apt-get install -y \
    curl apt-utils apt-transport-https debconf-utils gcc build-essential g++-5\
    && rm -rf /var/lib/apt/lists/*

# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update

# install libssl - required for sqlcmd to work on Ubuntu 18.04
RUN apt-get install -y libssl1.0.0 libssl-dev

# install sasl - required for python-sasl, a requirement of databricks-dbapi
RUN apt-get install -y libsasl2-dev

# install SQL Server drivers
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

# install SQL Server tools
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"

# python libraries
RUN apt-get install -y \
    python3-pip python3-dev python3-setuptools \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# install necessary locales
RUN apt-get update && apt-get install -y locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen
RUN pip3 install --upgrade pip

# install SQL Server Python SQL Server connector module - pyodbc
RUN pip3 install pyodbc

# install keyring prerequisites
RUN pip3 install keyring keyrings.cryptfile

# install additional utilities
RUN apt-get install gettext nano vim -y

RUN apt-get install -y git

ADD ./jetavator /src/jetavator
ADD ./jetavator_cli /src/jetavator_cli

WORKDIR /src/jetavator
RUN python3 /src/jetavator/setup.py install

WORKDIR /src/jetavator_cli
RUN python3 /src/jetavator_cli/setup.py install

WORKDIR /
RUN rm -rf /src/*

CMD /bin/bash ./entrypoint.sh
