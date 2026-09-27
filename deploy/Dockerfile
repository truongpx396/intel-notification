# Service-mode container. Multi-stage so the runtime image carries no toolchain.
#
# There is no Go code yet, so `go build` has nothing to compile -- this file is
# the intended shape, kept in the repository so the deployment story is reviewable
# alongside the design rather than invented later under time pressure.

FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifyd ./cmd/notifyd

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/notifyd /notifyd
COPY --from=build /src/migrations /migrations
USER nonroot:nonroot
EXPOSE 8080 9090
# 8080 REST (recipient-facing), 9090 gRPC (producer-facing).
# The two are separate ports because they have different trust models: 9090 is a
# capability to send messages as your product and belongs on an internal network.
ENTRYPOINT ["/notifyd"]
