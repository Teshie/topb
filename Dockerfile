# ---------- Build stage ----------
    FROM golang:1.23-bookworm AS build
    WORKDIR /app
    
    RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && rm -rf /var/lib/apt/lists/*
    
    ENV CGO_ENABLED=0 GOOS=linux GOTOOLCHAIN=auto GOFLAGS=-buildvcs=false
    
    COPY go.mod go.sum ./
    RUN go mod download
    
    COPY . .
    RUN echo "Project structure:" && ls -la
    
    RUN go build -v -ldflags="-s -w" -o /app/bin/server .
    
    # ---------- Runtime stage ----------
    FROM gcr.io/distroless/static-debian12
    WORKDIR /app
    
    # app binary
    COPY --from=build /app/bin/server /app/server
    
    # ✅ include the boards file
    COPY --from=build /app/boards.json /app/boards.json
    COPY --from=build /app/conf.json /app/conf.json
    
    ENV PORT=8001
    EXPOSE 8001
    USER 65532:65532
    ENTRYPOINT ["/app/server"]
    