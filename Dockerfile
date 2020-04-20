FROM debian:latest AS builder
LABEL maintainer="roman.atachiants@gmail.com"

# add ca certificated for http secured connection
RUN apk --no-cache add ca-certificates gcompat libc6-compat

# copy the binary
WORKDIR /root/  
ARG GO_BINARY
COPY "$GO_BINARY" .
RUN chmod +x /root/talaria

# Expose the port and start the service
EXPOSE 8027
CMD ["/root/talaria"]