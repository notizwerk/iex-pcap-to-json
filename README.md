# iex-pcap-to-json

A small tool for reading pcap(.gz) files with iex tops messages and converting the messages into a small json format.

The tool uses a fork of the go-iex project of Timothy Palpant https://github.com/timpalpant/go-iex. Our fork uses shorter property names to save disk space https://github.com/notizwerk/go-iex

The IEX is a fair, simple and transparent stock exchange dedicated to investor protection. IEX provides realtime and historical market data for free through the IEX Developer API. IEX is not affiliated and does not endorse or recommend this application.

## usage
* clone the project
* go build
* .\iex-pcap-to-json.exe folder/with/pcap/iex/files destination/dir
* or
* .\iex-pcap-to-json.exe IEX_tops.pcap destination/dir

the destination dir is optional
