# Get base image
FROM ubuntu:20.04

USER root
ARG UID=1000
ARG GID=1000
ARG TZ=America/New_York

## Change group settings ##
RUN groupadd -r --gid $GID user \
    && useradd -r --uid $UID -g user user \ 
    ## Set timezone (avoids hangup on gfortran install) ##
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \ 
    ## Install FORTRAN and Netcdf dependencies ##
    && apt-get update && apt-get install -y gcc gfortran libnetcdf-dev libnetcdff-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

## Install pip and python dependencies ##
RUN apt update --fix-missing && apt install -y p7zip-full python3-pip python-is-python3 \
    && pip3 install numpy Cython pandas geopandas xarray netcdf4 pyyaml toolz \ 
        joblib seaborn matplotlib fsspec dask s3fs zarr \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

## Compile t-route when container is run (default is no compile) ##
COPY ./entrypoint.sh /
ENV COMPILE=0
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]
