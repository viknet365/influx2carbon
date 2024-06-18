package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
	"github.com/gorilla/mux"
	"flag"
	"io/ioutil"
//	"os"
//	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"time"
	"strconv"
	"strings"
	"net/url"
   log	"github.com/sirupsen/logrus"
)

type TagSet map[string]string

type DataPoint struct {
	Prefix    string  `json:"prefix"`
	Metric    string  `json:"metric"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
	Tags      TagSet  `json:"tags"`
}

//type targetdata map[string]interface{}
type targets []targetdata

//map[string]interface{}
type targetdata struct {
	Target string `json:"target"`
	//        Datapoints [] interface{} `json:"datapoints"`
	Datapoints [][]float64 `json:"datapoints"`
	Tags       TagSet      `json:"tags"`
}

func trimFirstRune(s string) string {
	_, i := utf8.DecodeRuneInString(s)
	return s[i:]
}
func trimLastRune(s string) string {
    r, size := utf8.DecodeLastRuneInString(s)
    if r == utf8.RuneError && (size == 0 || size == 1) {
        size = 0
    }
    return s[:len(s)-size]
}


var graphitePostUrl = ""
var carbonUrl = ""

func main() {
//	log.SetFormatter(&log.TextFormatter{
//		DisableColors: true,
//		FullTimestamp: true,
//	})

	log.Info("Application Started")
//	log.Info("GRAPHITEIP:", os.Getenv("GRAPHITEIP"))
//	graphiteip := os.Getenv("GRAPHITEIP")
//	log.Info("GRAPHITEIP:", graphiteip)
	graphiteipPtr   := flag.String("graphiteip", "127.0.0.1", "ip of target graphite server         ( Render API )")
	graphiteportPtr := flag.Int("graphiteport" , 80         , "port of target graphite server       ( Render API )")
	carbonipPtr     := flag.String("carbonip"  , "127.0.0.1", "ip of target carbon server   ( plaintext protocol )")
	carbonportPtr   := flag.Int("carbonport"   , 2003       , "port of target carbon server ( plaintext protocol )")
	listenportPtr   := flag.Int("listenport"   , 8383       , "listening port of faked influxDB server")
	flag.Parse()


	log.Info("graphiteip:"  , *graphiteipPtr)
	log.Info("graphiteport:", *graphiteportPtr)
	log.Info("carbonip:"    , *carbonipPtr)
        log.Info("carbonport:"  , *carbonportPtr)
	log.Info("listenport:"  , *listenportPtr)
	//log.SetLevel(log.DebugLevel)

	graphitePostUrl = fmt.Sprintf("http://%s:%d/render", *graphiteipPtr, *graphiteportPtr)
	carbonUrl = fmt.Sprintf("%s:%d", *carbonipPtr, *carbonportPtr)
	serverporturl := fmt.Sprintf(":%d", *listenportPtr)

	log.Info("graphitePostUrl : ", graphitePostUrl)
	log.Info("carbonUrl : ", carbonUrl)
	log.Info("serverporturl : ", serverporturl)

	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/write", writeInfluxdbV1).Methods("POST")
	myRouter.HandleFunc("/query", queryInfluxdbV1).Methods("GET")
	myRouter.HandleFunc("/api/v2/write", writeInfluxdbV2).Methods("POST")
	myRouter.HandleFunc("/api/v2/query", queryInfluxdbV1)
	myRouter.PathPrefix("/").HandlerFunc(catchAllHandler)
	log.Fatal(http.ListenAndServe(serverporturl, myRouter))
}

func catchAllHandler(w http.ResponseWriter, r *http.Request) {
	log.Info("Handle Function: catchAllHandler")
	log.Debug("Method  :",r.Method)
        log.Debug("Url     :",r.URL.Path)
        log.Debug("PostForm:", r.PostForm)
	dump, _ := httputil.DumpRequest(r, true)
	log.Debug("DUMPREQUEST: ", dump)
	w.WriteHeader(http.StatusOK)
        log.Debug("End of Function: catchAllHandler")
}

func queryInfluxdbV1(w http.ResponseWriter, r *http.Request) {
	log.Info("Handle Function: queryInfluxdbV1")
        log.Debug("Method  :",r.Method)
        log.Debug("Url     :",r.URL.Path)
        log.Debug("PostForm:", r.PostForm)
	var buf bytes.Buffer
	var err error
	var influx_db, influx_epoch, influx_q string
	dump, _ := httputil.DumpRequest(r, true)
	log.Info("DUMPREQUEST: ", dump)
	influx_dbs, ok := r.URL.Query()["db"]
	if !ok || len(influx_dbs[0]) < 1 {
		log.Error("Url Param 'db' is missing")
	} else {
		influx_db = influx_dbs[0]
	}
	influx_epochs, ok := r.URL.Query()["epoch"]
	if !ok || len(influx_epochs[0]) < 1 {
		log.Error("Url Param 'epoch' is missing")
	} else {
		influx_epoch = influx_epochs[0]
	}
	influx_qs, ok := r.URL.Query()["q"]
	if !ok || len(influx_qs[0]) < 1 {
		log.Error("Url Param 'q' query is missing")
	} else {
		influx_q = influx_qs[0]
	}
	log.Debug("influxdb    db: ",influx_db)
        log.Debug("influxdb epoch: ",influx_epoch)
	log.Debug("influxdb query: ",influx_q)

	if _, err = buf.ReadFrom(r.Body); err != nil {
		log.Error("Error while reading")
		return
	}
	if err = r.Body.Close(); err != nil {
		log.Error("Error while closing")
		return
	}
	log.Debug("Body :", &buf)
	queryList := strings.Split(influx_q, " WHERE ")
	queryAfterWhere  := queryList[1]
	queryBeforeWhere := queryList[0]
	queryList = strings.Split(queryBeforeWhere, " FROM ")
	queryList = strings.SplitN(queryList[1], ".", 2)
	queryRp  := queryList[0]
	queryMetric := queryList[1]
	queryList  = strings.Split(queryAfterWhere, " ORDER BY ")
	queryWhere := queryList[0]
	queryOrder := queryList[1]
	log.Debug("SELECT value FROM:",queryRp)
	log.Debug("",queryMetric)
	log.Debug("WHERE:",queryWhere)
	log.Debug("ORDER BY:",queryOrder)
//WHERE:time >= '2021-12-20T09:42:02.472Z' AND time <= '2022-01-03T09:42:02.471Z'
	queryList  = strings.Split(queryWhere, "time >= ")
	queryTimeFrom := queryList[1]
	queryList  = strings.Split(queryTimeFrom, " ")
	queryTimeFrom = queryList[0]
	queryList  = strings.Split(queryWhere, "time <= ")
	queryTimeTo := queryList[1]
	queryList  = strings.Split(queryTimeTo, " ")
	queryTimeTo = queryList[0]
	log.Info("TimeFrom:",queryTimeFrom )
	log.Info("TimeTo:", queryTimeTo)

	startRange, err := time.Parse(time.RFC3339, trimLastRune(trimFirstRune(queryTimeFrom)))
if err != nil {
    log.Error(err)
}
endRange, err := time.Parse(time.RFC3339, trimLastRune(trimFirstRune(queryTimeTo)))
if err != nil {
    log.Error(err)
}
log.Info("TimeFrom:",startRange.Format("15:04_20060102"))
log.Info("TimeTo:",endRange.Format("15:04_20060102"))

	log.Info("URL:>", graphitePostUrl)

	//  var jsonStr = "target=openhab.consommeHP_Fours&from=-150d&until=now&format=json&maxDataPoints=1000"
	//    str := strings.NewReader(jsonStr)
	v := url.Values{}
	v.Set("target", influx_db+"."+queryMetric)
	v.Add("from",startRange.Format("15:04_20060102"))
	v.Add("until", endRange.Format("15:04_20060102"))
	v.Add("format", "json")
	v.Add("maxDataPoints", "500")

	//req, err := http.NewRequest("POST", url, str )
	postreq, err := http.PostForm(graphitePostUrl, v)
	//    req.Header.Set("X-Custom-Header", "myvalue")
	//  req.Header.Set("Content-Type", "application/json")

	//   client := &http.Client{}
	// resp, err := client.Do(postreq)
	if err != nil {
		panic(err)
	}
	//defer resp.Body.Close()
	log.Debug("response Status:", postreq.Status)
	log.Debug("response Headers:", postreq.Header)
	body, _ := ioutil.ReadAll(postreq.Body)
	log.Debug("response Body:", string(body))
	datares := targets{}
	//    json.NewDecoder(postreq.Body).Decode(datares)
	json.Unmarshal([]byte(body), &datares)
	for _, value := range datares {

		log.Debug("datapoints:", value.Datapoints)
		log.Debug("Target    :", value.Target)
		log.Debug("Tags      :", value.Tags)
		//fmt.Printf("response Body json:", tdata)
	}

	//FIR : Formated Influx Response
	FIR := formatDataToInflux(&datares)
	log.Info("FormatedInfluxResponse :", FIR)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w,"%s",FIR)
	log.Trace("truc:", FIR)
	log.Debug("End of Function: queryInfluxdbV1")
}

func writeInfluxdbV2(w http.ResponseWriter, r *http.Request) {
	log.Info("Handle Function: writeInfluxdbV2")
	log.Debug("Method  :",r.Method)
        log.Debug("Url     :",r.URL.Path)
        log.Debug("PostForm:", r.PostForm)
	var buf bytes.Buffer
	var err error
	var influx_db, influx_rp, influx_precision string
	dump, _ := httputil.DumpRequest(r, true)
	log.Debug("DUMPREQUEST: ", dump)
	influx_dbs, ok := r.URL.Query()["org"]
	if !ok || len(influx_dbs[0]) < 1 {
		log.Warn("Url Param 'org' is missing")
	} else {
		influx_db = influx_dbs[0]
	}
	influx_rps, ok := r.URL.Query()["bucket"]
	if !ok || len(influx_rps[0]) < 1 {
		log.Warn("Url Param 'rp' is missing")
	} else {
		influx_rp = influx_rps[0]
	}
	influx_precisions, ok := r.URL.Query()["precision"]
	if !ok || len(influx_precisions[0]) < 1 {
		log.Warn("Url Param 'precision' is missing")
	} else {
		influx_precision = influx_precisions[0]
	}
	log.Debug("influxdb db              :",influx_db);
        log.Debug("influxdb retention policy:",influx_rp);
        log.Debug("influxdb precision       :",influx_precision);

	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	if _, err = buf.ReadFrom(r.Body); err != nil {
		return
	}
	if err = r.Body.Close(); err != nil {
		return
	}
	log.Debug("Body :", &buf)
	dpstrings := strings.Split(buf.String(), "\n")
	for _, dpstring := range dpstrings {
		dp, err := InfluxStringToDatapoint(dpstring)
		dp.Prefix = influx_db
		if err == nil {
			writeGraphite(dp)
		}
	}
	if err != nil {
		log.Error(err)
	}
	w.WriteHeader(http.StatusOK)
	//      json.NewEncoder(w).Encode("article")
        log.Debug("End of Function: writeInfluxdbV2")
}

func writeInfluxdbV1(w http.ResponseWriter, r *http.Request) {
	log.Info("Handle Function: writeInfluxdbV1")
        log.Debug("Method  :",r.Method)
        log.Debug("Url     :",r.URL.Path)
        log.Debug("PostForm:", r.PostForm)
	var buf bytes.Buffer
	var err error
	var influx_db, influx_rp, influx_precision string
	influx_dbs, ok := r.URL.Query()["db"]
	if !ok || len(influx_dbs[0]) < 1 {
		log.Warn("Url Param 'db' is missing")
	} else {
		influx_db = influx_dbs[0]
	}
	influx_rps, ok := r.URL.Query()["rp"]
	if !ok || len(influx_rps[0]) < 1 {
		log.Warn("Url Param 'rp' is missing")
	} else {
		influx_rp = influx_rps[0]
	}
	influx_precisions, ok := r.URL.Query()["precision"]
	if !ok || len(influx_precisions[0]) < 1 {
		log.Warn("Url Param 'precision' is missing")
	} else {
		influx_precision = influx_precisions[0]
	}
	log.Debug("influx_db:",influx_db)
	log.Debug("influx_rp:",influx_rp)
	log.Debug("influx_precision:", influx_precision)
	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	if _, err = buf.ReadFrom(r.Body); err != nil {
		return
	}
	if err = r.Body.Close(); err != nil {
		return
	}
	log.Debug("Body:", &buf)
	dpstrings := strings.Split(buf.String(), "\n")
	for _, dpstring := range dpstrings {
		dp, err := InfluxStringToDatapoint(dpstring)
		dp.Prefix = influx_db
		if err == nil {
			writeGraphite(dp)
		}
	}
	if err != nil {
		log.Error(err)
	}
	w.WriteHeader(http.StatusOK)
	//	json.NewEncoder(w).Encode("article")
	log.Debug("End of Function: writeInfluxdbV1")
}

func formatDataToInflux( t * targets ) string {
         //FIR : Formated Influx Response
        FIR :="{\"results\": [  { \"statement_id\": 0,"
        influxdb := ""
        for i, value := range *t {
                metric := strings.SplitN(value.Target, ".", 2)
                if influxdb == "" {
                        influxdb = metric[0]
                } else {
                        if influxdb != metric[0] {
                                influxdb = ""
                        }
                }
                if i != 0 {
                        FIR += ","
                }
                FIR += "\"series\": [ "
                FIR += "{ \"name\": \"" + metric[1] + "\", \"columns\": [   \"time\", \"value\" ],"
                FIR += "\"values\": ["
                for i, value := range value.Datapoints {
                        if i != 0 {
                                FIR += ","
                        }
                        FIR += "["  + strconv.FormatFloat(1000*value[1], 'f', -1, 64) + ","+   strconv.FormatFloat(value[0], 'f', -1, 64)    +"]"
                }
                FIR += "] }"
                //fmt.Printf("response Body json:", tdata)
        }
        FIR += "] } ] }"
        return FIR
}



func format2graphite(dp DataPoint) string {
	graphitedp := ""
	if len(dp.Prefix) > 0 {
		graphitedp = graphitedp + dp.Prefix + "."
	}
	graphitedp = graphitedp + dp.Metric
	graphitedp = graphitedp + " " + strconv.FormatFloat(dp.Value, 'f', -1, 64) + " " + strconv.FormatInt(dp.Timestamp/1000000000, 10) + "\n"

	return graphitedp
}

func writeGraphite(dp DataPoint) {
	var err error
	tcpAddr, err := net.ResolveTCPAddr("tcp", carbonUrl)
	if err != nil {
		log.Error("ResolveTCPAddr failed:", err.Error())
	} else {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			log.Error("Dial failed:", err.Error())
		} else {
		_, err = conn.Write([]byte(format2graphite(dp)))
		if err != nil {
			log.Error("Write to server failed:", err.Error())
			}
		}
		log.Info("write to server =", format2graphite(dp))
		conn.Close()
	}

}

func InfluxStringToDatapoint(dpstring string) (DataPoint, error) {
	var d DataPoint
	var err error = nil
	m := make(map[string]string)

	if len(dpstring) == 0 {
		return d, errors.New("empty String")
	}
	entries := strings.Split(dpstring, " value=")
	if len(entries) != 2 {
		return d, errors.New("cannot detect ' value='")
	}
	valtime := strings.Split(entries[1], " ")
	if len(valtime) != 2 {
		return d, errors.New("no space after 'value=' (no timestamp)")
	}
	//	fmt.Printf("entries0||%s||\n", entries[0])
	optionmap := strings.Split(entries[0], ",")
	for _, f := range optionmap {
		if len(f) != 0 {
			parts := strings.Split(f, "=")
			if len(parts) == 1 {
				d.Metric = parts[0]
			} else if len(parts) == 2 {
				m[parts[0]] = replaceInfluxEscapeChar(parts[1])
				//				fmt.Printf("||%s||%s||\n", parts[0], replaceInfluxEscapeChar(parts[1]))
			}
		}
	}
	// handle case of integer, remove the i to change it to float ()
	valtime[0] = strings.TrimRight(valtime[0], "i")
	// TODO handle string case

	d.Value, err = strconv.ParseFloat(valtime[0], 64)
	d.Timestamp, err = strconv.ParseInt(valtime[1], 10, 64)
	d.Tags = m

	return d, err

}
func replaceInfluxEscapeChar(escapedstring string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(escapedstring, "\\ ", " "), "\\\\", "\\"), "\\,", ","), "\\=", "=")
}
