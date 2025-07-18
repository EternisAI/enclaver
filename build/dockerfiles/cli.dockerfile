###############################

FROM scratch AS build-amd64

###############################

FROM scratch AS build-arm64

###############################

FROM build-${TARGETARCH} AS build

ARG TARGETARCH

COPY --from=artifacts ${TARGETARCH}/enclaver /usr/local/bin/enclaver

ENTRYPOINT ["/usr/local/bin/enclaver"]
