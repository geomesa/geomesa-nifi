FROM apache/nifi:1.28.1

USER root

# from https://adoptium.net/installation/linux/
RUN apt update && apt install -y wget apt-transport-https gpg && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt update && apt install -y temurin-21-jre

ENV JAVA_HOME=/usr/lib/jvm/temurin-21-jre-amd64
ENV PATH=/usr/lib/jvm/temurin-21-jre-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

USER nifi
