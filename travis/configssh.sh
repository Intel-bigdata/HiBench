#restart ssh service

ssh-keygen -q -N "" -t rsa -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
echo "    StrictHostKeyChecking no                     " >> /etc/ssh/ssh_config
service ssh restart
