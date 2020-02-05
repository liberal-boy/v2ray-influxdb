package main

import (
	"context"
	"flag"
	"github.com/influxdata/influxdb/client/v2"
	"google.golang.org/grpc"
	"gopkg.in/ini.v1"
	"log"
	"strings"
	"time"
	"v2ray.com/core/app/stats/command"
)

type Stat struct {
	User  string
	Type  string
	value int64
}

func QueryStats(c command.StatsServiceClient) (stats []Stat, err error) {
	resp, err := c.QueryStats(context.Background(), &command.QueryStatsRequest{
		Pattern: "user>>>",
		Reset_:  true,
	})
	if err != nil {
		return
	}
	stats = make([]Stat, len(resp.GetStat()))
	for i, s := range resp.GetStat() {
		slice := strings.Split(s.Name, ">>>")
		stats[i].User = slice[1]
		stats[i].Type = slice[3]
		stats[i].value = s.Value
	}
	return
}

func QueryServerStats(addr string) (stats []Stat, err error) {
	cmdConn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return
	}
	defer func() { _ = cmdConn.Close() }()
	statsClient := command.NewStatsServiceClient(cmdConn)
	stats, err = QueryStats(statsClient)
	if err != nil {
		return
	}
	return
}

func WriteToDB(addr, dbname string, serverStats map[string][]Stat) (err error) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: addr,
	})
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  dbname,
		Precision: "s",
	})
	if err != nil {
		return
	}

	for server, stats := range serverStats {
		for _, stat := range stats {
			var pt *client.Point
			pt, err = client.NewPoint("stat", map[string]string{
				"server": server,
				"user":   stat.User,
				"type":   stat.Type,
			}, map[string]interface{}{
				"value": stat.value,
			}, time.Now())
			if err != nil {
				return
			}
			bp.AddPoint(pt)
		}
	}
	err = conn.Write(bp)
	return
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Llongfile)

	configPath := flag.String("config", "./config.ini", "path to config file")
	flag.Parse()
	conf, err := ini.Load(*configPath)
	if err != nil {
		log.Fatalln(err)
	}

	var serverStats = make(map[string][]Stat)

	for _, server := range conf.Section("servers").Keys() {
		stats, err := QueryServerStats(server.Value())
		if err != nil {
			log.Println("fail to QueryServerStats", server.Name(), err)
		}

		if len(stats) > 0 {
			serverStats[server.Name()] = stats
		}
	}

	if len(serverStats) <= 0 {
		return
	}

	sectInfluxDB := conf.Section("influxDB")
	err = WriteToDB(sectInfluxDB.Key("addr").Value(), sectInfluxDB.Key("dbname").Value(), serverStats)
	if err != nil {
		log.Fatalln(err)
	}
}
