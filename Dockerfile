############################################################
# Build stage
############################################################

FROM golang:1.9 as builder

# Building Router
WORKDIR /go/src/github.com/chepeftw/raft
ADD raft.go .
RUN go get \
    && CGO_ENABLED=0 go install -a -tags netgo -ldflags '-extldflags "-static"' \
    && ldd /go/bin/raft | grep -q "not a dynamic executable"


############################################################
# Run stage
############################################################

FROM alpine

#RUN apk update && apk add bash

COPY wrapper.sh /wrapper.sh
RUN chmod +x /wrapper.sh
COPY --from=builder /go/bin/raft /raft

#CMD ["bash", "/wrapper.sh"]
CMD /wrapper.sh

# docker build -t raft-test .
# docker run raft-test
# docker image ls raft-test

# docker run --privileged -dit --net=none  --name raft01 raft-test
# docker run --privileged -dit --env RAFT_PORT=10123 --env RAFT_TIMEOUT=11000 --env RAFT_TARGET_SYNC=0 --name raft01 raft-test