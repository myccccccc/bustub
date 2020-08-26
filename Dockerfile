FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN sed -i s@/archive.ubuntu.com/@/mirrors.cloud.tencent.com/@g /etc/apt/sources.list && \
    apt-get clean
RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      clang-8 \
      clang-format-8 \
      clang-tidy-8 \
      cmake \
      doxygen \
      git \
      g++-7 \
      pkg-config \
      valgrind \
      zlib1g-dev
