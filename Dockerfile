# --- build ---
FROM golang:1.24-alpine AS build
WORKDIR /src

# Сначала только зависимости: этот слой кэшируется и не пересобирается при правке кода.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# --- runtime ---
# distroless: нет shell и пакетного менеджера — меньше размер и поверхность атаки.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /app/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/app/api"]
