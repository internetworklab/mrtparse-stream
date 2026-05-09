# -- Stage 1: Download dependencies --
FROM golang:1.25 AS deps

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# -- Stage 2: Build --
FROM golang:1.25 AS build

WORKDIR /app

COPY --from=deps /go /go
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/mrtparse-serve ./cmd/serve

# -- Stage 3: Ship --
FROM debian:trixie

COPY --from=build /bin/mrtparse-serve /usr/local/bin/mrtparse-serve

EXPOSE 8190

ENTRYPOINT ["mrtparse-serve"]
