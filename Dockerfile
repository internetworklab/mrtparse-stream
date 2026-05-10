# -- Stage 1: Download dependencies --
FROM golang:1.25 AS deps

WORKDIR /app

COPY go.mod go.sum ./
RUN mkdir cloudping
COPY cloudping/go.mod cloudping/go.sum ./cloudping/
RUN go mod download

# -- Stage 2: Build --
FROM golang:1.25 AS build

WORKDIR /app

COPY --from=deps /go /go
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/mrtparse-serve ./cmd/serve
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/mrtparse-ingest ./cmd/ingest

# -- Stage 3: Ship --
FROM debian:trixie

COPY --from=build /bin/mrtparse-serve /usr/local/bin/mrtparse-serve
COPY --from=build /bin/mrtparse-ingest /usr/local/bin/mrtparse-ingest

EXPOSE 8190

ENTRYPOINT ["mrtparse-serve"]
