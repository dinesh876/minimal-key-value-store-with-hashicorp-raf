package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
    "github.com/dinesh876/kvstore/util"
)

// Finite state machine

type kvFsm struct{
    db *sync.Map
}

/*

The Apply operation is sent to basically-up-to-date nodes to keep them up to date.
An Apply call is made for each new log the leader commits.

Each log message will contain a key and value to store in the in-memory key-value store.

*/

type setPayload struct {
    Key string
    Value string
}

func (kf *kvFsm) Apply(log *raft.Log) any {
    switch log.Type{
        case raft.LogCommand:
            var sp setPayload
            err := json.Unmarshal(log.Data,&sp)
            if err != nil {
                return fmt.Errorf("Could not parse payload: %s",err)
            }
            kf.db.Store(sp.Key,sp.Value)
        default:
            return fmt.Errorf("Unkown raft log type: %#v",log.Type)
             
    }
    return nil
}

/*

The Restore operation reads all logs and applies them to the state machine.
It looks very similar to the Apply function we just wrote except for that
this operates on an io.ReadCloser of serialized log data rather than the high-level raft.Log struct.
And most importantly, and unlike the Apply function, Restore must reset all local state.

The io.ReadCloser represents the latest snapshot or beginning of time if there are
no snapshots
*/


func (kf *kvFsm) Restore(rc io.ReadCloser) error {
    // Must always restore from a clean state !!
    kf.db.Range(func(key any,_ any) bool{
        kf.db.Delete(key)
        return true
    })
    // convert json to go objects
    decoder := json.NewDecoder(rc)
    
    for decoder.More(){
        var sp setPayload
        err := decoder.Decode(&sp)
        if err != nil {
            return fmt.Errorf("could not decode payload : %s",err)
        }
        kf.db.Store(sp.Key,sp.Value)
    }

    return rc.Close()
}


type snapshotNoop struct {}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error {return nil}
func (sn snapshotNoop) Release() {}

func (kf *kvFsm) Snapshot() (raft.FSMSnapshot,error){
    return snapshotNoop{},nil
}


/*
Raft node initialization

In order to start the Raft library behavior for each node, we need a whole bunch of boilerplate for Raft libraryinitialization.
Each Raft node needs a TCP port that it uses to communicate with other nodes in the same cluster.
Each node starts out in a single-node cluster where it is the leader. 
Only when told to (and given the address of other nodes) does there become a multi-node cluster.
Each node also needs a permanent store for the append-only log. The Hashicorp Raft library suggests boltdb.

*/


func setupRaft(dir,nodeId,raftAddress string,kf *kvFsm) (*raft.Raft,error){
    os.MkdirAll(dir,os.ModePerm)
    fmt.Println("Raft Address:",raftAddress)
    store,err := raftboltdb.NewBoltStore(path.Join(dir,"bolt"))
    if err != nil {
        return nil ,fmt.Errorf("Could not create bolt store: %s",err)
    }

    snapshots,err  := raft.NewFileSnapshotStore(path.Join(dir,"snapshot"),2,os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("Could not create snapshots store: %s",err)
    }
    
    tcpAddr,err := net.ResolveTCPAddr("tcp",raftAddress)
    if err != nil {
        return nil,fmt.Errorf("Could not resolve address: %s",err)
    }

    transport ,err := raft.NewTCPTransport(raftAddress,tcpAddr,10, time.Second*10,os.Stderr)
    if err != nil {
        return nil, fmt.Errorf("Could not create tcp transport: %s",err)
    }

    raftCfg := raft.DefaultConfig()
    raftCfg.LocalID = raft.ServerID(nodeId)

    r,err := raft.NewRaft(raftCfg,kf,store,store,snapshots,transport)
    if err != nil {
        return nil, fmt.Errorf("Could not create raft instance: %s",err)
    }

    // Cluster consists of unjoined leaders. Picking a leader and
    // creating a real cluster is done manually after startup.

    r.BootstrapCluster(raft.Configuration{
        Servers: []raft.Server{
            {
                ID: raft.ServerID(nodeId),
                Address: transport.LocalAddr(),

            },  
        },
    })

    return r, nil

}


/*

An HTTP API
-----------

This key-value store application will have an HTTP API serving two purposes:

Cluster management: telling a leader to add followers
Key-value storage: setting and getting keys

*/


type httpServer struct {
    r *raft.Raft
    db *sync.Map
}


func (hs httpServer) joinHandler(w http.ResponseWriter,r *http.Request){
    followerID := r.URL.Query().Get("followerID")
    followerAddr := r.URL.Query().Get("followerAddr")

    if hs.r.State() != raft.Leader {
        json.NewEncoder(w).Encode(struct{
            Error string `json:"error"`
        }{
            "Not a leader",
        })

        http.Error(w,http.StatusText(http.StatusBadRequest),http.StatusBadRequest)
        return
    }

    err := hs.r.AddVoter(raft.ServerID(followerID),raft.ServerAddress(followerAddr),0,0).Error()
    if err != nil {
        log.Printf("Failed to add follower addr: %s",err)
        http.Error(w,http.StatusText(http.StatusBadRequest),http.StatusBadRequest)
    }
    w.WriteHeader(http.StatusOK)
}

func (hs httpServer) setHandler(w http.ResponseWriter,r *http.Request){
    defer r.Body.Close()
    bs, err := io.ReadAll(r.Body)
    if err != nil {
        log.Printf("Could not read key-value in http request %s",err)
        http.Error(w,http.StatusText(http.StatusBadRequest),http.StatusBadRequest)
        return
    }

    future := hs.r.Apply(bs,500 *time.Millisecond)

    // Blocks until completion
    if err := future.Error(); err != nil {
        log.Printf("Could not write key-value: %s", err)
        http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
        return
    }

    e := future.Response()
    if e != nil {
        log.Printf("Could not write key value: %s",e)
        http.Error(w,http.StatusText(http.StatusBadRequest),http.StatusBadRequest)
        return
    }

    w.WriteHeader(http.StatusOK)
}


func (hs httpServer) getHandler(w http.ResponseWriter,r *http.Request){
    key := r.URL.Query().Get("key")
    value, _ := hs.db.Load(key)
    if value == nil {
        value = ""
    }
    rsp := struct{
        Data string `json:data`
    }{
        Data: value.(string),
    }
    err := json.NewEncoder(w).Encode(rsp)
    if err != nil {
        log.Println("Could not encode key-value in http response: %s",err)
        http.Error(w,http.StatusText(http.StatusInternalServerError),http.StatusInternalServerError)
    }
}

type config struct {
    addr string
    id string
    httpPort string
    raftPort string
}

func getConfig() config {
    cfg := config{}
    for i,arg := range os.Args[1:] {
        if arg == "--iface" {
            cfg.addr = os.Args[i+2]
            i++
            continue
        }
        if arg == "--node-id"{
            cfg.id = os.Args[i+2]
            i++
            continue
        }
        if arg == "--http-port"{
            cfg.httpPort = os.Args[i+2]
            i++
            continue
        }
        if arg == "--raft-port"{
            cfg.raftPort = os.Args[i+2]
            i++
            continue
        }
    }
    if cfg.addr == ""{
        log.Fatal("Missing required parameter:--iface")
    }
    if cfg.id == ""{
        log.Fatal("Missing required parameter: --node-id")
    }
    if cfg.httpPort == ""{
        log.Fatal("Missing required parameter: --http-port")
    }
    if cfg.raftPort == ""{
        log.Fatal("Missing required parameter: --raft-port")
    }
    return cfg
}


func main(){
    cfg := getConfig()
    db := &sync.Map{}
    kf := &kvFsm{db}
    
    dataDir := "data"
    err := os.MkdirAll(dataDir,os.ModePerm)
    if err != nil {
        log.Fatal("Could not create directory: %s",err)
    }

    addr,err := util.GetInterfaceIpv4Addr(cfg.addr)
    if err != nil {
        log.Fatal("Could not get the ipv4 address of the interface:",err)
    }

    r,err := setupRaft(path.Join(dataDir,"raft"+ cfg.id),cfg.id,addr+":"+cfg.raftPort,kf)
    if err != nil {
        log.Fatal(err)
    }
    hs := httpServer{r,db}

    http.HandleFunc("/set",hs.setHandler)
    http.HandleFunc("/get",hs.getHandler)
    http.HandleFunc("/join",hs.joinHandler)
    http.ListenAndServe(":"+cfg.httpPort,nil)
}

