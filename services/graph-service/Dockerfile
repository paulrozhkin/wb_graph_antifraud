FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /graph-api ./cmd/api

FROM scratch
COPY --from=build /graph-api /graph-api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/graph-api"]
