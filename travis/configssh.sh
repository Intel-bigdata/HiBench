#restart ssh service

ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
echo "    StrictHostKeyChecking no                     " >> /etc/ssh/ssh_config
#echo "    UserKnownHostsFile /dev/null                 " >> /etc/ssh/ssh_config
echo $JAVA_HOME
echo 1234567890
service ssh restart
