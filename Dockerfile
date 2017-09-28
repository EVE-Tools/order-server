FROM alpine:3.6

#
# Copy release to container and set command
#

# Do not run as root
USER element43:element43

# Copy build
COPY migrations migrations
COPY order-server order-server

ENV PORT 8000
EXPOSE 8000

CMD ["/order-server"]