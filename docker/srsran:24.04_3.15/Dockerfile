# ettusresearch/ubuntu-uhd:18.04

# Provides a base Ubuntu (18.04) image with latest UHD installed

FROM        lkschu/uhd:3.15
MAINTAINER  Lukas K. Schumann

# Last build date - this can be updated whenever there are security updates so
# that everything is rebuilt
ENV         security_updates_as_of 2024-06-27

# This will make apt-get install without question
ARG         DEBIAN_FRONTEND=noninteractive
ARG         SRS_VERSION=24.04
ARG         SRS_VERSION_COMMIT=c33cacba7d940e734ac7bad08935cbc35578fad9
ARG         MAKEWIDTH=8

# Install srsRAN dependencies
RUN         apt-get update
RUN         apt-get -y install -q \
                    cmake \
                    make \
                    gcc \
                    g++ \
                    pkg-config \
                    libfftw3-dev \
                    libmbedtls-dev \
                    libsctp-dev \
                    libyaml-cpp-dev \
                    libgtest-dev

RUN          rm -rf /var/lib/apt/lists/*

RUN          mkdir -p /usr/local/src
RUN          git clone https://github.com/srsran/srsRAN_Project.git /usr/local/src/srsRAN
RUN          cd /usr/local/src/srsRAN/ && git checkout $SRS_VERSION_COMMIT
RUN          mkdir -p /usr/local/src/srsRAN/build
WORKDIR      /usr/local/src/srsRAN/build
RUN          cmake ../
RUN          make -j $MAKEWIDTH
# RUN          make test -j $MAKEWIDTH
RUN          make install
WORKDIR      /

