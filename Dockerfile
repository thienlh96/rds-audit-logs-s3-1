# syntax=docker/dockerfile:1

FROM golang:1.19

# Set destination for COPY
WORKDIR /app

# Download Go modules


# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/engine/reference/builder/#copy
COPY ./lambda/main ./
# Build
# RUN CGO_ENABLED=0 GOOS=linux go build -o /collector 

# Optional:
# To bind to a TCP port, runtime parameters must be supplied to the docker command.
# But we can document in the Dockerfile what ports
# the application is going to listen on by default.
# https://docs.docker.com/engine/reference/builder/#expose
# Run
CMD ["./main"]