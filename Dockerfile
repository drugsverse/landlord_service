# Stage 1; Building the binary.
FROM rust:1.90-slim AS builder

# Installing system dependencies needed for building dependencies like librdkafka.
RUN apt-get update && apt-get install -y pkg-config build-essential libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire source code
COPY . .

# Build highly optimized production binary.
RUN cargo build --release

# STAGE 2; Create minimal production image.
FROM debian:bookworm-slim

# Install runtime security certificates and SSL support.
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copying compiled binary from builder stage.
COPY --from=builder /app/target/release/landlord_service .

# Expose actix web monitoring port.
EXPOSE 8180

# Run the binary.
CMD ["./landlord_service"]
