# # Use Ubuntu 24.04 as base for both stages to ensure GLIBC compatibility
# FROM ubuntu:24.04 as builder

# # Install Rust and system dependencies
# RUN apt-get update && apt-get install -y \
#     curl \
#     build-essential \
#     pkg-config \
#     libssl-dev \
#     libpq-dev \
#     ca-certificates \
#     && rm -rf /var/lib/apt/lists/*

# # Install Rust
# RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# ENV PATH="/root/.cargo/bin:${PATH}"

# # Set the working directory
# WORKDIR /app

# # Set environment variables
# ENV SQLX_OFFLINE=true

# # Copy the Cargo.toml and Cargo.lock files
# COPY Cargo.toml Cargo.lock ./

# # Copy the source code
# COPY . .

# # Build the application
# RUN cargo build --release

# # Runtime stage - use the same Ubuntu version
# FROM ubuntu:24.04

# # Install runtime dependencies
# RUN apt-get update && apt-get install -y \
#     ca-certificates \
#     libssl3 \
#     libpq5 \
#     && rm -rf /var/lib/apt/lists/*

# # Copy the binary from the builder stage
# COPY --from=builder /app/target/release/indexer /usr/local/bin/indexer

# # Expose the port
# EXPOSE 3000

# # Run the binary
# CMD ["indexer"]





# Use Ubuntu 24.04 as base for all stages to ensure GLIBC compatibility
FROM ubuntu:24.04 as chef

# Install Rust and system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cargo-chef
RUN cargo install cargo-chef

WORKDIR /app

# Planner stage - analyze dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage - build dependencies and application
FROM chef AS builder

# Set environment variables
ENV SQLX_OFFLINE=true

# Copy the recipe from planner stage
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this layer will be cached unless dependencies change
RUN cargo chef cook --release --recipe-path recipe.json

# Copy source code and build application
COPY . .
RUN cargo build --release

# Runtime stage - use the same Ubuntu version
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/index-api /usr/local/bin/index-api

# Expose the port
EXPOSE 3001

# Run the binary
CMD ["index-api"]