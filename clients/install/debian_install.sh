# start with upgraded system packages
apt update
apt upgrade

# install prerequisites 
apt install git golang-1.8 postgresql-9.6

# create PTO Linux user
adduser --system pto

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
cp ptosrv.service /etc/systemd/system

# install and build PTO in goroot
export GOROOT=/home/pto/go
mkdir $GOROOT
runuser -u pto bash debian_install_pto3go.sh
setcap 'cap_net_bind_service=+ep' $GOROOT/bin/ptosrv

# initialize database
runuser -u pto $GOROOT/bin/ptosrv -initdb

