FROM golang:1.15

WORKDIR /app

COPY go.mod .

RUN go mod download

COPY . .

RUN go build

CMD ["./go-producer"]
