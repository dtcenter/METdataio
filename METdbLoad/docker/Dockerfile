FROM centos:8

MAINTAINER Venita Hagerty <venita.hagerty@noaa.gov>

#
# This Dockerfile checks out METdbload from GitHub and runs the specified branch or tag.
#

#
# Install system updates
#
RUN yum -y update \
 && yum -y install epel-release \
 && yum -y install 'dnf-command(config-manager)' \
 && yum config-manager --set-enabled powertools

#
# Install required packages
#
RUN yum -y install wget tar git ksh \
 && rm -rf /var/cache/yum/* \
 && yum clean all

 #
 # Install Python 3.6
 #
 RUN yum install -y python3 python3-devel python3-pip

 #
 # Install Python packages
 #
 RUN pip-3 install pandas \
  && pip-3 install numpy \
  && pip-3 install lxml \
  && pip-3 install pymysql \
  && pip-3 install retrying

 #
 # Create a link for python3
 #
 RUN ln -s /usr/bin/python3 /usr/local/bin/python

 ENV METDBLOAD_GIT_NAME develop

 ENV METDBLOAD_GIT_URL  https://github.com/dtcenter/METdatadb

#
# Check code out from GitHub
#
 RUN mkdir /METdbload-python \
  && echo "Checking out METdbload ${METDBLOAD_GIT_NAME} from ${METDBLOAD_GIT_URL}" \
  && git clone ${METDBLOAD_GIT_URL} /METdbload-python/METdatadb \
  && cd /METdbload-python/METdatadb \
  && git checkout ${METDBLOAD_GIT_NAME}

 #
 # set env vars
 #
 ENV PYTHONPATH "${PYTHONPATH}:/METdbload-python/METdatadb/METdbLoad/:/METdbload-python/METdatadb/METdbLoad/ush/"
 WORKDIR /METdbload-python/METdatadb/METdbLoad/ush

 EXPOSE 3306

 # use volume command to mount XML file
 # also include xml file name (with docker path) after image name in run command
 # use second volume command right after first one to specify top level data directory
 ENTRYPOINT ["python", "met_db_load.py"]