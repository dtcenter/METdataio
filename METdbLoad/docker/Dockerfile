FROM centos:7

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
 && yum-config-manager --enable PowerTools

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
  && pip-3 install pymysql

 #
 # Create a link for python3
 #
 RUN ln -s /usr/bin/python3 /usr/local/bin/python

 ENV METDBLOAD_GIT_NAME develop

 ENV METDBLOAD_GIT_URL  https://github.com/dtcenter/METdataio

#
# Check code out from GitHub
#
 RUN mkdir /METdbload-python \
  && echo "Checking out METdbload ${METDBLOAD_GIT_NAME} from ${METDBLOAD_GIT_URL}" \
  && git clone ${METDBLOAD_GIT_URL} /METdbload-python/METdataio \
  && cd /METdbload-python/METdataio \
  && git checkout ${METDBLOAD_GIT_NAME}

 #
 # set env vars
 #
 ENV PYTHONPATH "${PYTHONPATH}:/METdbload-python/METdataio/METdbLoad/:/METdbload-python/METdataio/METdbLoad/ush/"
 WORKDIR /METdbload-python/METdataio/METdbLoad/ush

 EXPOSE 3306

 # use volume command to mount XML file
 # also include xml file name (with docker path) after image name in run command
 # use second volume command right after first one to specify top level data directory
 ENTRYPOINT ["python", "met_db_load.py"]
