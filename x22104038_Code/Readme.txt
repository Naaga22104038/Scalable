# Scalable Cloud Programming

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)

## Introduction

The main objective of this project is to gather, save, pre-process data, and perform computation processes on the BGL dataset using the MapReduce and Spark frameworks.

## Prerequisites

In this project the BGL log file will be processed and examined, using an Oracle virtual machine and a technologically advanced scalable cloud computing solution. Install Ubuntu Virtual Machine using VirtualBox. This virtual machine should have a minimum of 4GB RAM allocated to it. Start your virtual machine.


## Setup

-minimum of 8GB RAM and 100GB free disk space
-hardware virtualisation enabled
-Install Ubuntu Server

-Install java
sudo apt install openjdk-8-jdk openjdk-8-jre

-Download Hadoop Archive 
curl -O http://ftp.heanet.ie/mirrors/www.apache.org/dist/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz

-Download Spark Archive
curl -O https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz

-Install Python 3 and PIP
sudo apt install python3
sudo apt install python3-pip

-Install Jupyter Notebook
sudo pip3 install notebook

## Usage
Copy BGL.tar.gz file to your Ubuntu Server using the below command.
sftp -i <path to private key> <user>@<host>

-To run the python file using MapReduce(MRJob)
python3 <filename> BGL.log

-To run the python file using SparkRdd
python3 <filename> BGL.log

-To run the SparkSQl
type pyspark in command prompt




