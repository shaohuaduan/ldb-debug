+++++++++++++++++++++++++ install GO in Ubuntu 18 +++++++++++++++++++++++++

wget -c https://dl.google.com/go/go1.14.2.linux-amd64.tar.gz -O - | sudo tar -xz -C /usr/local
export PATH=$PATH:/usr/local/go/bin
source ~/.profile
go version

+++++++++++++++++++++++++ install badgerDB +++++++++++++++++++++++++
go get github.com/dgraph-io/badger
cd badger-master/badger
go install

+++++++++++++++++++++++++ install/run ldb +++++++++++++++++++++++++
go build ldb.go
./ldb