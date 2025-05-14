# ettusresearch/ubuntu-uhd:18.04

# Provides a base Ubuntu (18.04) image with latest UHD installed

FROM        ubuntu:22.04
MAINTAINER  Lukas K. Schumann

# Last build date - this can be updated whenever there are security updates so
# that everything is rebuilt
ENV         security_updates_as_of=2024-05-13
ENV         UHD_IMAGES_DIR=/usr/share/uhd/images/

# This will make apt-get install without question
ARG         DEBIAN_FRONTEND=noninteractive
# ARG         UHD_TAG=v4.0.0.0
ARG         UHD_TAG=UHD-4.0
ARG         MAKEWIDTH=8

# Install security updates and required packages
RUN         apt-get update
RUN         apt-get -y install -q \
                build-essential \
                ccache \
                git \
                python3-dev \
                python3-pip \
                curl
# Install UHD dependencies
RUN         apt-get -y install -q \
                    autoconf \
                    automake \
                    build-essential \
                    ccache \
                    cmake \
                    cpufrequtils \
                    doxygen \
                    ethtool \
                    g++ \
                    git \
                    inetutils-tools \
                    libboost-all-dev \
                    libncurses5 \
                    libncurses5-dev \
                    libusb-1.0-0 \
                    libusb-1.0-0-dev \
                    libusb-dev \
                    python3-dev \
                    python3-mako \
                    python3-numpy \
                    python3-requests \
                    python3-scipy \
                    python3-setuptools \
                    python3-ruamel.yaml

# RUN         apt-get -y install -q \
#                 libboost-all-dev \
#                 libusb-1.0-0-dev \
#                 libudev-dev \
#                 python3-mako \
#                 doxygen \
#                 python3-docutils \
#                 cmake \
#                 python3-requests \
#                 python3-numpy \
#                 dpdk \
#                 libdpdk-dev



RUN          rm -rf /var/lib/apt/lists/*

RUN          mkdir -p /usr/local/src
RUN          git clone https://github.com/EttusResearch/uhd.git /usr/local/src/uhd
RUN          cd /usr/local/src/uhd/ && git checkout $UHD_TAG
RUN          mkdir -p /usr/local/src/uhd/host/build
WORKDIR      /usr/local/src/uhd/host/build
RUN          cmake .. -DENABLE_PYTHON3=ON -DUHD_RELEASE_MODE=release -DCMAKE_INSTALL_PREFIX=/usr
RUN          make -j $MAKEWIDTH
RUN          make install
RUN          uhd_images_downloader
WORKDIR      /

