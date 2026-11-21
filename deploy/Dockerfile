# Service-mode container. Multi-stage so the runtime image carries no toolchain.
#
# Builds cmd/notifyd (T049a). Its environment is docs/configuration.md; the compose stack beside this
# file shows a working one, and creates the database role the service insists on (one with neither
# SUPERUSER nor BYPASSRLS).

# Pinned to the toolchain go.mod names, so the image builds what CI tests.
FROM golang:1.26.8-trixie AS build
WORKDIR /src
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifyd ./cmd/notifyd

FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /
COPY --from=build /out/notifyd /notifyd
COPY --from=build /src/migrations /migrations
# The migrations are applied by the database's first boot (deploy/initdb), not by this process.
USER nonroot:nonroot
EXPOSE 8080 9090
# 8080 REST (recipient-facing), 9090 gRPC (producer-facing).
# The two are separate ports because they have different trust models: 9090 is a
# capability to send messages as your product and belongs on an internal network.
ENTRYPOINT ["/notifyd"]
