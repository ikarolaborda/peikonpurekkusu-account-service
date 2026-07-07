# syntax=docker/dockerfile:1.7
# Build context: repo root (dockerfile: services/account-service/Dockerfile)

FROM golang:1.26 AS build
WORKDIR /src
# -p=1 keeps peak compiler memory low enough for small Docker VMs
ENV CGO_ENABLED=0 GOFLAGS=-p=1
COPY contracts/gen/go ./contracts/gen/go
COPY services/account-service ./services/account-service
WORKDIR /src/services/account-service
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download && \
    go vet ./... && \
    go test ./... && \
    go build -trimpath -ldflags="-s -w" -o /out/account-service ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/account-service /account-service
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/account-service"]
