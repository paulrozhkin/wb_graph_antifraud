FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /graph-service ./cmd/graph-service

FROM scratch
WORKDIR /app
COPY --from=build /graph-service /app/graph-service
COPY config/config.json /app/config/config.json
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/graph-service"]
