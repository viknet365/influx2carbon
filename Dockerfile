FROM golang:1.16

WORKDIR /go/src/app
COPY . .
ENV GRAPHITEIP=127.0.0.1
ENV GRAPHITEPORT=80
ENV CARBONIP=127.0.0.1
ENV CARBONPORT=2003
ENV LISTENPORT=8383

RUN go get -d -v ./...
RUN go install -v ./...

# CMD ["go", "run", ".", "-graphiteip", "${GRAPHITEIP}","-graphiteport","80","-listenport","8383", "-carbonip", "192.168.2.200","-carbonport", "2003"]
CMD go run . -graphiteip $GRAPHITEIP -graphiteport $GRAPHITEPORT -listenport $LISTENPORT -carbonip $CARBONIP -carbonport $CARBONPORT

