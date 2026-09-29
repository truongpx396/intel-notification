# Service-mode container. Multi-stage so the runtime image carries no toolchain.
#
# cmd/notifyd does not exist yet (tasks T043-T049), so this image cannot build
# today -- the file is the intended shape, kept in the repository so the
# deployment story is reviewable alongside the design rather than invented later
# under time pressure.

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
USER nonroot:nonroot
EXPOSE 8080 9090
# 8080 REST (recipient-facing), 9090 gRPC (producer-facing).
# The two are separate ports because they have different trust models: 9090 is a
# capability to send messages as your product and belongs on an internal network.
ENTRYPOINT ["/notifyd"]
