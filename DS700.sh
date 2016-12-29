################ Not needed for exam ################
cd Documents/Knowledge/Training/Cloudera/Advanced\ Analytics\ with\ Spark/
ssh -i SparkAA.pem hadoop@ec2-13-54-105-39.ap-southeast-2.compute.amazonaws.com
sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven

mkdir medline_data
cd medline_data
wget ftp://ftp.nlm.nih.gov/nlmdata/sample/medline/*.gz
gunzip *.gz
ls -ltr
hadoop fs -mkdir -p medline
hadoop fs -put *.xml medline
#####################################################

# Get xml jar and start spark
cd ..
wget https://github.com/sryza/aas/archive/1st-edition.zip
unzip 1st-edition.zip
cd aas-1st-edition/common/
mvn package
spark-shell --master yarn --deploy-mode client --jars target/common-1.0.2.jar

# For Q1c, get files
hdfs dfs -getmerge q1c q1c.txt   # 8637 lines
mkdir check
python DS700Q1ab.py  # answers different but ballpark - likely due to encoding ***(check)***
hdfs dfs -getmerge check/authsCS check/authsCS.txt
python DS700Q1c.py   # 8608 lines - difference is due to Python struggling with encoding

# Q2a file - note I got the response "Records contain incorrect data" last time so double check wording
cd ..
hdfs dfs -getmerge q2a q2a.txt

# For Q2b and Q2c checks, get file
hdfs dfs -getmerge check/degrees check/degrees.txt