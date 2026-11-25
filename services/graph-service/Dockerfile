FROM node:22-bookworm-slim AS frontend
WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -trimpath -o /graph-service ./cmd/graph-service

FROM scratch
WORKDIR /app
COPY --from=build /graph-service /app/graph-service
COPY --from=frontend /frontend/dist /app/web
COPY config/config.json /app/config/config.json
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/graph-service"]
