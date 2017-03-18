FROM alpine:latest

MAINTAINER zweizeichen@element-43.com

#
# Copy release to container and set command
#

# Copy build
COPY migrations migrations
COPY order-server order-server

ENV PORT 80
EXPOSE 80

CMD ["/order-server"]