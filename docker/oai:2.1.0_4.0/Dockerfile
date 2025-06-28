# ettusresearch/ubuntu-uhd:18.04

# Provides a base Ubuntu (18.04) image with latest UHD installed

FROM        lkschu/uhd:4.0
MAINTAINER  Lukas K. Schumann

# Last build date - this can be updated whenever there are security updates so
# that everything is rebuilt
ENV         security_updates_as_of 2024-05-13

# This will make apt-get install without question
ARG         DEBIAN_FRONTEND=noninteractive
ARG         OAI_VERSION=v2.1.0
ARG         OAI_VERSION_COMMIT=9fab2124417cfe67fe09b1eab5e377e26c5cf3a5
ARG         MAKEWIDTH=8

# Install srsRAN dependencies
RUN         apt-get update
RUN          mkdir -p /usr/local/src
RUN          git clone https://gitlab.eurecom.fr/oai/openairinterface5g.git /usr/local/src/OAI
RUN          cd /usr/local/src/OAI/ && git checkout $SRS_VERSION_COMMIT
WORKDIR      /usr/local/src/OAI/cmake_targets
RUN          ./build_oai -I
RUN          rm -rf /var/lib/apt/lists/*
RUN          ./build_oai --ninja --gNB -C -w USRP
WORKDIR      /

ENTRYPOINT ["/usr/local/src/OAI/cmake_targets/ran_build/build/nr-softmodem", "-O", "/oai_config.conf", "--sa", "--continous-tx"]
