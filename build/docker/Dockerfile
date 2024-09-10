FROM golang:1.23.1-alpine3.20 AS builder

ARG AES_KEY="********************************"

WORKDIR /
COPY . .

RUN apk add --update build-base upx && apk upgrade --available

RUN go build -ldflags "-X git.brobridge.com/gravity/gravity-adapter-mysql/pkg/adapter/service.aesKey=$AES_KEY -s -w" -o /gravity-adapter-mysql ./cmd/gravity-adapter-mysql/gravity-adapter-mysql.go

RUN go install -ldflags "-X main.aesKey=$AES_KEY -s -w" github.com/BrobridgeOrg/pwd-encrypt@latest

RUN upx -6 /go/bin/pwd-encrypt /gravity-adapter-mysql

FROM alpine:3.20
WORKDIR /

RUN apk add --update tzdata mysql-client && apk upgrade --available

COPY --from=builder /gravity-adapter-mysql /gravity-adapter-mysql
COPY --from=builder /go/bin/pwd-encrypt /pwd_encrypt
COPY ./configs /configs
COPY ./settings/ /settings/
COPY ./build/docker/startup.sh /startup.sh

RUN mkdir /statestore && \
        chown -R 1001:0  /settings /configs /statestore /gravity-adapter-mysql /pwd_encrypt /startup.sh && \
        #chmod 777 /settings/sources.json /configs/config.toml  && \
        chmod -R g+rwX /statestore /settings /configs

USER 1001
ENV TZ="Asia/Taipei"

CMD ["sh", "/startup.sh"]
