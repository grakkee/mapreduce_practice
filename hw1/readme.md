Grace Meredith
CS433 HW1
9 March 2023

*NOTE* this assignment is incomplete and does not fully run on Hadoop. I did not give myself enough time for this assignment, so I'm just submitting what I have.

*Theoretically if this ran without errors, here are the commands to give it:

Q1:

compile java code
`javac -classpath hadoop-core-1.2.1.jar Q1.java`
create jar file
`jar cf Q1.jar *.class`
sub your actual path of jar file
`export HADOOP_CLASSPATH=home/meredith_grace_hw1/Q1/Q1.jar`
run jobs
`hadoop jar Q1.jar Q1 <path to training_set_tweets.txt> <path to output directory>`

Q2:

complie java code
`javac -classpath hadoop-core-1.2.1.jar Q2.java`
create jar file
`jar cf Q2.jar *.class`
sub path of jar file
`export HADOOP_CLASSPATH=home/meredith_grace_hw1/Q2/Q2.jar`
run jobs
`hadoop jar Q2.jar Q2 <path to training_sets_tweets.txt> <path to training_sets_users.txt> <path to output directory>`