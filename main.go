package main

import (
	"flag"
	"log"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/mountkin/etcd-fs/etcdfs"
)

func main() {
	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatal("Usage:\n  etcd-fs MOUNTPOINT ETCDENDPOINT")
	}
	etcdFs := etcdfs.New(flag.Arg(1))
	nfs := pathfs.NewPathNodeFs(etcdFs, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
