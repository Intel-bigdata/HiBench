#restart ssh service

ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
echo "    StrictHostKeyChecking no                     " >> /etc/ssh/ssh_config
service ssh restart
