#Intro

This is a Impala thrift client example, cause user can access impala use Hive jdbc / Impala jdbc or thrift API.
This client example contain all three methods, cause we need kerberos in our env, We need create another SASL Trsnaport in thrift examples.

In normal condition, I access impala with impala-shell and HUE, they both use thrift api implements with Python, I some confition, we need access it with Java, this is where the project comes from.

Besides, you can access impala with jdbc, I think thrift api can do more and jdbc client is easier.

#Usage:

* 1. before compale and package, you need modify pom.xml to set thrift path in your local env.(If linux, you can run 'which thrift' to find the path, In Windows, you can find thrift.exe in bin directory)
* 2. run 'mvn install'
* 3. cd target
* 4. $ java  -cp ./Impala-Client-0.0.1-SNAPSHOT.jar:./lib/* -Djavax.security.auth.useSubjectCredsOnly=false com.terry.impala.ImpalaConnectTest  hostname thrift_port command_sql [server_principal(if kerberos enable]


#Warning
If you want to use thrift client, you need confirm all .thrift files in src/main/thrift is the same version with server, If not, try to copy thrift files from server. Currently, My Impala server version is 2.6.0.

#Connect

Email : fengyuatad[AT]126[DOT]com