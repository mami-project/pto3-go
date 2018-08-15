HOSTNAME="fqdn.of.instance.goes.here"
PTO_DB_PASS="PTO database password goes here"

# start with upgraded system packages
apt update
apt upgrade

# install Debian package prerequisites 
apt install git postgresql-9.6 libcap2-bin certbot

# install golang manually for unclear and probably incredibly stupid reasons
wget https://dl.google.com/go/go1.10.3.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.10.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# get a certificate
certbot certonly --standalone -d $HOSTNAME

# create PTO Linux user
adduser pto --disbaled-password --gecos PTO,,,, 

# fixup letsencrypt perms so pto can use them
chgrp pto /etc/letsencrypt/live
chmod 750 /etc/letsencrypt/live
chgrp pto /etc/letsencrypt/archive
chmod 750 /etc/letsencrypt/archive

# create PTO PostgreSQL user and database
pushd /
runuser -u postgres createuser pto  
runuser -u postgres createdb pto
echo "grant all on database pto to pto;" | runuser -u postgres psql pto #FIXME permissive
echo "alter role pto with password `$PTO_DB_PASS`" | runuser -u postgres psql pto

# create PTO raw and query cache stores
mkdir -p /home/pto/data/raw
chown pto:pto /home/pto/data/raw 
mkdir -p /home/pto/data/qcache
chown pto:pto /home/pto/data/qcache 

# create configuration and systemd service
mkdir -p /etc/pto
cp ptoconfig.json /etc/pto
cp apikeys.json /etc/pto
cp ptosrv.service /etc/systemd/system

# install and build PTO in goroot
export GOPATH=/home/pto/go
mkdir $GOPATH
chown pto:pto $GOPATH
bash -x debian_install_pto3go.sh
setcap 'cap_net_bind_service=+ep' $GOPATH/bin/ptosrv

# initialize database
runuser -u pto -- $GOPATH/bin/ptosrv -initdb

# enable and start PTO
systemctl enable ptosrv
systemctl start ptosrv