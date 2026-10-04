# Network-delay helper for Amendment 5 (PROTOCOL.md 12.2): runs `tc` inside the
# network namespace of a use-case service (`docker run --net container:<id>
# --cap-add NET_ADMIN`), so neither orchestrator version is changed.
FROM alpine:3.20
RUN apk add --no-cache iproute2-tc
ENTRYPOINT ["tc"]
