package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	iex "github.com/notizwerk/go-iex"
	"github.com/notizwerk/go-iex/iextp/tops"
)

var maxBulkSize = 20000
var defaultSymbols = []string{"AAPL", "AMD", "AMZN", "BABA", "CGC", "FB", "GLD", "GOOGL", "GOOS", "IQ", "JD", "LABU", "M", "MDB", "MU", "NFLX", "NVDA", "QQQ", "ROKU", "SHOP", "SNAP", "SPOT", "SPY", "SQ", "TLRY", "TWLO", "TWTR", "UGAZ", "VXX", "WMT"}

// SymbolMetaData stores some parsing/encoding specific data for a symbol
type SymbolMetaData struct {
	BulkCount         int
	BulkMessageCount  int
	TotalMessageCount int
	LastPriceChanged  bool
	Encoder           *json.Encoder
	LastPrice         *tops.TradeReportMessage
	LastOrderBook     *tops.QuoteUpdateMessage
}

var symbolMetaDataMap = make(map[string]*SymbolMetaData)

// EnrichedTOPS containing all relevant data in one doc
type EnrichedTOPS struct {
	Timestamp time.Time `json:"t"`
	// Traded symbol represented in Nasdaq integrated symbology.
	Symbol string `json:"s"`
	// Size of the last trade, in number of shares.
	LastSaleSize uint32 `json:"lss"`
	// Execution price of last sale.
	LastSalePrice float64 `json:"lsp"`
	// Timestamp of the last sale
	LastSaleTimestamp time.Time `json:"lst"`
	// First TOP after a new sale
	LastSaleChange bool `json:"ls"`
	// Size of the quote at the bid, in number of shares.
	BidSize uint32 `json:"bs"`
	// Price of the quote at the bid.
	BidPrice float64 `json:"bp"`
	// Price of the quote at the ask.
	AskPrice float64 `json:"ap"`
	// Size of the quote at the ask, in number of shares.
	AskSize uint32 `json:"as"`
}

// IndexMetaData for generating the bulk meta data part
type IndexMetaData struct {
	// Index string `json:"_index"`
	Doc string `json:"_type"`
}

// ActionMetaData for generating the bulk meta data part
type ActionMetaData struct {
	Index IndexMetaData `json:"index"`
}

// logfile
var logger *log.Logger

func main() {
	if len(os.Args) < 2 {
		println("for generating an ndjson bulk upload file(s):\n")
		println("usage:\n .\\iex-pcap-to-json [-symbol=AAPL] [-filter=filter] pcpaFileOrDir [destinationDir]")
		println("\nor for uploading:\n")
		println("usage:\n .\\iex-pcap-to-json -index=indexPrefix -url=elasticurl -user=user -pass=pass ndJsonBulkFile")
		return
	}
	symbolPtr := flag.String("symbol", "", "show only messages for symbol")
	filterPtr := flag.String("filter", "", "parse only files with match the filter (strings.Index()>-1)")
	urlPtr := flag.String("url", "", "the url to elastic")
	indexPrefixPtr := flag.String("index", "", "the prefix of the index. symbol and year will be added")
	userPtr := flag.String("user", "", "user name for elastic")
	passPtr := flag.String("pass", "", "password for elastic")
	flag.Parse()

	logfileName := "pcaplog-" + time.Now().Format("20060102") + ".log"
	logFile, err := os.OpenFile(logfileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(mw, "", log.LstdFlags)
	log.SetOutput(mw)

	fileOrDirectory := flag.Args()[0]
	fi, err := os.Lstat(fileOrDirectory)
	if err != nil {
		panic(err)
	}

	if len(*urlPtr) > 0 {
		if fi.Mode().IsRegular() {
			upload(*urlPtr, *userPtr, *passPtr, *indexPrefixPtr, flag.Args()[0])
		} else if fi.Mode().IsDir() {
			dir, err := os.Open(fileOrDirectory)
			if err != nil {
				panic(err)
			}
			if !strings.HasSuffix(fileOrDirectory, string(os.PathSeparator)) {
				fileOrDirectory = fileOrDirectory + string(os.PathSeparator)
			}
			names, err := dir.Readdirnames(0)
			if err != nil {
				panic(err)
			}
			for index := 0; index < len(names); index++ {
				if strings.HasSuffix(names[index], "ndjson") {
					upload(*urlPtr, *userPtr, *passPtr, *indexPrefixPtr, fileOrDirectory+names[index])
				}
			}
		} else {
			println("given file is neither regular file nor directory")
		}
		return
	}

	var destDir string
	if len(flag.Args()) > 1 {
		destDir = flag.Args()[1]
	}
	var symbols []string
	if len(*symbolPtr) > 0 {
		symbols = strings.Split(*symbolPtr, ",")
		for i, s := range symbols {
			symbols[i] = strings.TrimSpace(s)
			fmt.Printf("symbol '%v'\n", symbols[i])
		}
	} else {
		symbols = defaultSymbols
	}

	if fi.Mode().IsRegular() {
		convertToMergedJSON(fileOrDirectory, destDir, symbols)
	} else if fi.Mode().IsDir() {
		dir, err := os.Open(fileOrDirectory)
		if err != nil {
			panic(err)
		}
		if !strings.HasSuffix(fileOrDirectory, string(os.PathSeparator)) {
			fileOrDirectory = fileOrDirectory + string(os.PathSeparator)
		}
		names, err := dir.Readdirnames(0)
		if err != nil {
			panic(err)
		}
		for index := 0; index < len(names); index++ {
			subFileName := names[index]
			subFile, subFileErr := os.Lstat(fileOrDirectory + subFileName)
			if subFileErr != nil || subFile.Mode().IsRegular() == false {
				logger.Printf("cannot open or is not a file %v", subFileName)
			} else if strings.Contains(subFileName, "pcap") || strings.Contains(subFileName, "pcap.gz") { //strings.HasSuffix(names[index], "pcap") || strings.HasSuffix(names[index], "pcap.gz")
				if len(*filterPtr) > 0 && (strings.Index(subFileName, *filterPtr) > -1) || len(*filterPtr) == 0 {
					convertToMergedJSON(fileOrDirectory+subFileName, destDir, symbols)
				}

			}
		}
	}
	for _, sym := range symbols {
		symbolMetaData, found := symbolMetaDataMap[sym]
		if found {
			symbolMetaData = symbolMetaDataMap[sym]
			logger.Printf("finished %v %v\n", sym, symbolMetaData.TotalMessageCount)
		}
	}
	return
}

var dateRegexp = regexp.MustCompile("[\\d]{8}")
var errorRegExp = regexp.MustCompile("errors\"\\s*:\\s*true")

func upload(url string, username string, password string, indexPrefix string, fileName string) {

	file, err := os.Open(fileName)
	if err != nil {
		logger.Printf("cannot open %v: %v", fileName, err)
		return
	}
	payloadReader := bufio.NewReader(file)
	last := strings.LastIndex(fileName, "_")
	fileNameShort := fileName[0:last]
	last = strings.LastIndex(fileNameShort, "_")
	symbol := strings.ToLower(fileNameShort[last+1:])
	date := dateRegexp.FindString(fileNameShort)[0:4]

	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}
	url = url + indexPrefix + "-" + symbol + "-" + date + "/_bulk"

	lastPS := strings.LastIndex(fileName, string(os.PathSeparator))
	pcapIndex := strings.Index(fileName, ".ndjson")
	fileNameOnly := fileName[lastPS+1 : pcapIndex]
	logger.Printf("uploading %v %v\n", fileNameOnly, url)

	req, err := http.NewRequest("POST", url, payloadReader)
	if err != nil {
		logger.Printf("cannot create request %v\n", err)
		return
	}
	req.SetBasicAuth(username, password)
	req.Header.Add("content-type", "application/x-ndjson")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Printf("cannot make request %v\n", err)
		return
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logger.Printf("cannot read response %v\n", err)
		return
	}
	logger.Printf("uploaded %v\n", fileNameOnly)
	responseJSON := string(body)
	error := errorRegExp.FindString(responseJSON)
	if len(error) > 0 {
		responseFileName := fileName + ".error.json"
		logger.Printf("error while uploading. writing response to  " + responseFileName)
		datOut, err := os.Create(responseFileName)
		if err != nil {
			fmt.Printf("cannot write response to file ")
			return
		}
		datOut.Write(body)
		datOut.Close()
	} else {
		responseFileName := fileName + ".response.json"
		datOut, err := os.Create(responseFileName)
		if err != nil {
			logger.Printf("cannot write response to file %v ", responseFileName)
			return
		}
		datOut.WriteString("{\"result\":\"uploaded successfully\"}")
		datOut.Close()
	}
}

func convertToMergedJSON(pcapFile string, destDir string, symbols []string) {
	for _, sym := range symbols {
		symbolMetaData, found := symbolMetaDataMap[sym]
		if !found {
			symbolMetaData = &SymbolMetaData{BulkMessageCount: 0,
				BulkCount: 0}
			symbolMetaDataMap[sym] = symbolMetaData
		}
		if symbolMetaData.Encoder != nil {
			symbolMetaData.Encoder = nil
			symbolMetaData.BulkMessageCount = 0
			symbolMetaData.BulkCount = 0
		}
	}

	dat, err := os.Open(pcapFile)
	if err != nil {
		panic(err)
	}
	packetDataSource, err := iex.NewPcapDataSource(dat)
	if err != nil {
		panic(err)
	}
	pcapScanner := iex.NewPcapScanner(packetDataSource)
	for {
		msg, err := pcapScanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				logger.Printf("io.EOF %v\n", err)
				break
			}
			logger.Printf("cannot read next message: %v\n", err)
			break
		}

		switch msg := msg.(type) {
		case *tops.QuoteUpdateMessage:
			symbol := msg.Symbol
			if len(symbols) > 0 && Contains(symbols, symbol) {

				symbolMetaData := symbolMetaDataMap[symbol]
				symbolMetaData.LastOrderBook = msg
				topMessage := EnrichedTOPS{Timestamp: msg.Timestamp,
					Symbol:         msg.Symbol,
					AskPrice:       msg.AskPrice,
					AskSize:        msg.AskSize,
					BidPrice:       msg.BidPrice,
					BidSize:        msg.BidSize,
					LastSaleChange: false}

				lastPriceMsg := symbolMetaData.LastPrice

				if lastPriceMsg != nil {
					topMessage.LastSalePrice = lastPriceMsg.Price
					topMessage.LastSaleSize = lastPriceMsg.Size
					topMessage.LastSaleTimestamp = lastPriceMsg.Timestamp
					if symbolMetaData.LastPriceChanged {
						topMessage.LastSaleChange = true
						symbolMetaData.LastPriceChanged = false
					}
				}
				enc := jsonEncoder(pcapFile, destDir, symbol)
				indexMetaData := IndexMetaData{
					// Index: "tops_" + strings.ToLower(symbol) + "_" + topMessage.Timestamp.Format("200601"),
					Doc: "_doc"}
				actionMeta := ActionMetaData{Index: indexMetaData}
				bulkMsgCounter := symbolMetaData.BulkMessageCount
				symbolMetaData.TotalMessageCount++
				totalCounter := symbolMetaData.TotalMessageCount
				enc.Encode(actionMeta)
				enc.Encode(topMessage)
				bulkMsgCounter++
				if totalCounter%5000 == 0 {
					fmt.Printf("%v %v\n", symbol, totalCounter)
				}
				if bulkMsgCounter >= maxBulkSize {
					logger.Printf("%v reaching bulk size\n", symbol)
					symbolMetaData.BulkMessageCount = 0
					symbolMetaData.Encoder = nil
					symbolMetaData.BulkCount++
				} else {
					symbolMetaData.BulkMessageCount = bulkMsgCounter
				}
			}
		case *tops.TradeReportMessage:
			symbol := msg.Symbol
			if len(symbols) > 0 && Contains(symbols, symbol) {
				// symbolToLastPrice[symbol] = msg
				symbolMetaData := symbolMetaDataMap[symbol]
				symbolMetaData.LastPrice = msg
				symbolMetaData.LastPriceChanged = true
			}
		default:
		}
	}
}

func jsonEncoder(pcapFile string, destDir string, symbol string) *json.Encoder {
	symbolMetaData, ok := symbolMetaDataMap[symbol]
	if !ok {
		panic("cannot find symbolMetaData for " + symbol)
	}

	enc := symbolMetaData.Encoder
	if enc != nil {
		return enc
	}
	bulkcount := symbolMetaData.BulkCount
	lastPS := strings.LastIndex(pcapFile, string(os.PathSeparator))
	pcapIndex := strings.Index(pcapFile, ".pcap")
	fileName := pcapFile[lastPS+1 : pcapIndex]
	fileName = strings.Replace(fileName, "%2F", "-", -1)
	if destDir != "" {
		if !strings.HasSuffix(destDir, string(os.PathSeparator)) {
			destDir = destDir + string(os.PathSeparator)
		}
	}
	if len(symbol) > 0 {
		fileName = destDir + fileName + "_" + symbol + "_" + strconv.Itoa(bulkcount) + ".ndjson"
	} else {
		fileName = destDir + fileName + ".ndjson"
	}
	logger.Print("converting file " + pcapFile + " to " + fileName)

	datOut, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	enc = json.NewEncoder(datOut)
	symbolMetaData.Encoder = enc
	return enc
}

func convertToJSON(pcapFile string, destDir string, symbol string) {

	dat, err := os.Open(pcapFile)
	if err != nil {
		panic(err)
	}
	lastPS := strings.LastIndex(pcapFile, string(os.PathSeparator))
	pcapIndex := strings.Index(pcapFile, ".pcap")
	fileName := pcapFile[lastPS+1 : pcapIndex]
	if destDir != "" {
		if !strings.HasSuffix(destDir, string(os.PathSeparator)) {
			destDir = destDir + string(os.PathSeparator)
		}
	}
	if len(symbol) > 0 {
		fileName = destDir + fileName + "_" + symbol + ".json"
	} else {
		fileName = destDir + fileName + ".json"
	}
	println("converting file " + pcapFile + " to " + fileName)
	datOut, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	packetDataSource, err := iex.NewPcapDataSource(dat)
	if err != nil {
		panic(err)
	}
	pcapScanner := iex.NewPcapScanner(packetDataSource)

	enc := json.NewEncoder(datOut)
	for {
		msg, err := pcapScanner.NextMessage()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		switch msg := msg.(type) {
		case *tops.QuoteUpdateMessage:
			if len(symbol) == 0 || msg.Symbol == symbol {
				enc.Encode(msg)
			}
		case *tops.TradeReportMessage:
			if len(symbol) == 0 || msg.Symbol == symbol {
				enc.Encode(msg)
			}
		case *tops.OfficialPriceMessage:
			if len(symbol) == 0 || msg.Symbol == symbol {
				enc.Encode(msg)
			}
		default:
		}
	}

}

//Contains tests if string x is in the array a of strings
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
