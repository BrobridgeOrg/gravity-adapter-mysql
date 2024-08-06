FROM golang:1.21-alpine3.19 AS builder

ARG AES_KEY="********************************"

WORKDIR /
COPY . .

RUN apk add --update build-base git openssh && apk upgrade --available

RUN go build -ldflags "-X git.brobridge.com/gravity/gravity-adapter-mysql/pkg/adapter/service.aesKey=$AES_KEY" -o /gravity-adapter-mysql ./cmd/gravity-adapter-mysql/gravity-adapter-mysql.go

RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN git config --global url."git@github.com:BrobridgeOrg/pwd-encrypt".insteadOf "https://github.com/BrobridgeOrg/pwd-encrypt"
RUN --mount=type=ssh GOPRIVATE=github.com/BrobridgeOrg/pwd-encrypt go install -ldflags "-X main.aesKey=$AES_KEY" github.com/BrobridgeOrg/pwd-encrypt@latest

FROM alpine:3.19.2
WORKDIR /

RUN apk add --update build-base tzdata mysql-client && apk upgrade --available

COPY --from=builder /gravity-adapter-mysql /gravity-adapter-mysql
COPY --from=builder /pwd-encrypt /pwd_encrypt
COPY ./configs /configs
COPY ./settings/ /settings/
COPY ./build/docker/startup.sh /startup.sh

RUN mkdir /statestore && \
        chown -R 1001:0  /settings /configs /statestore /gravity-adapter-mysql /pwd_encrypt /startup.sh && \
        chmod 777 /settings/sources.json /configs/config.toml  && \
        chmod -R g+rwX /statestore

USER 1001
ENV TZ="Asia/Taipei"

CMD ["sh", "/startup.sh"]
