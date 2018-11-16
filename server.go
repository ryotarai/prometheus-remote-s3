package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type Server struct {
	buffer *Buffer

	totalReceivedTimeseries uint64
	totalSentTimeseries     uint64
	totalWriteRequests      uint64
}

func NewServer(buffer *Buffer) (*Server, error) {
	s := &Server{
		buffer:                  buffer,
		totalReceivedTimeseries: 0,
		totalSentTimeseries:     0,
		totalWriteRequests:      0,
	}

	return s, nil
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "prometheus_remote_s3_total_received_timeseries{} %d\n", s.totalReceivedTimeseries)
	fmt.Fprintf(w, "prometheus_remote_s3_total_sent_timeseries{} %d\n", s.totalSentTimeseries)
	fmt.Fprintf(w, "prometheus_remote_s3_total_write_requests{} %d\n", s.totalWriteRequests)
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	atomic.AddUint64(&s.totalWriteRequests, 1)
	err = s.writeTimeseries(req.Timeseries)
	if err != nil {
		log.Printf("Error in writeTimeseries: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) writeTimeseries(tss []*prompb.TimeSeries) error {
	atomic.AddUint64(&s.totalReceivedTimeseries, uint64(len(tss)))
	defer atomic.AddUint64(&s.totalSentTimeseries, uint64(len(tss)))

	for _, ts := range tss {
		for _, ss := range ts.Samples {
			labels := map[string]string{}
			for _, l := range ts.Labels {
				labels[l.Name] = l.Value
			}

			err := s.buffer.Put(ss.Timestamp, ss.Value, labels)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/write" {
		s.handleWrite(w, r)
	} else if r.URL.Path == "/metrics" {
		s.handleMetrics(w, r)
	} else {
		http.NotFound(w, r)
	}
}
