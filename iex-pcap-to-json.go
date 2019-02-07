package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"

	iex "github.com/notizwerk/go-iex"
	"github.com/notizwerk/go-iex/iextp/tops"
)

func main() {
	if len(os.Args) < 2 {
		println("usage:\n .\\iex-pcap-to-json [-symbol=AAPL] pcpaFileOrDir [destinationDir]")
		return
	}
	symbolPtr := flag.String("symbol", "", "show only messages for symbol")
	flag.Parse()

	var destDir string
	if len(flag.Args()) > 1 {
		destDir = flag.Args()[1]
	}
	fileOrDirectory := flag.Args()[0]
	fi, err := os.Lstat(fileOrDirectory)
	if err != nil {
		panic(err)
	}

	if fi.Mode().IsRegular() {
		convertToJSON(fileOrDirectory, destDir, *symbolPtr)
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
			if strings.HasSuffix(names[index], "pcap") ||
				strings.HasSuffix(names[index], "pcap.gz") {
				convertToJSON(fileOrDirectory+names[index], destDir, *symbolPtr)
			}
		}
	}
	return
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
