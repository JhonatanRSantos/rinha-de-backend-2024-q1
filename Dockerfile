FROM golang:1.22.0 AS builder

WORKDIR /app
# copies the go.mod and go.sum files to the container
COPY go.mod go.sum ./
# downloads the dependencies
RUN go mod download
# copies the source code from cmd folder to the container
COPY cmd/ ./cmd
# builds the go app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app ./cmd/api/main.go

FROM alpine:latest AS runner

WORKDIR /
# copies the app binary from the builder stage to the runner stage
COPY --from=builder /app/app .
# sets the app as executable
RUN chmod +x app
# exposes the port
EXPOSE 9910
EXPOSE 9920
# runs the app
CMD ["/app"]
