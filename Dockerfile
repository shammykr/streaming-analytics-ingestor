FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingestor ./cmd/ingestor \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingestor /out/producer /usr/local/bin/
COPY config.yaml /etc/ingestor/config.yaml
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/ingestor", "-config", "/etc/ingestor/config.yaml"]
