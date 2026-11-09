# C2.8 evaluation fixture (server, ingress, tcpproxy modes). Standard library only; static, non-root, no shell.
# Build from this directory: docker build -t <registry>/c28-fixture:<tag> deploy/eval-c28/fixture
# The run-lock records the pushed image by digest; tags are never used by the rendered manifests.
FROM golang:1.25.14-bookworm AS build
WORKDIR /src
# Every non-test Go file of the package (the build context's .dockerignore keeps *_test.go out); check-build-context.sh
# (run by scripts/verify.sh) compiles exactly what this COPY selects, so a new source file cannot be left behind.
COPY go.mod *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -buildid=" -o /out/c28-fixture .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/c28-fixture /c28-fixture
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/c28-fixture"]
