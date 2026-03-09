#!/bin/bash

# Script khởi động nhanh Data Warehouse system
# Author: PTIT-KPDL Team

set -e

echo "=================================================="
echo "  Data Warehouse - Quick Start Script"
echo "=================================================="
echo ""

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check Docker
print_info "Checking Docker..."
if ! command -v docker &> /dev/null; then
    print_error "Docker not found. Please install Docker first."
    exit 1
fi
print_success "Docker is installed"

# Check Docker Compose
print_info "Checking Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose not found. Please install Docker Compose first."
    exit 1
fi
print_success "Docker Compose is installed"

# Stop existing containers
print_info "Stopping existing containers..."
docker-compose down 2>/dev/null || true
print_success "Stopped existing containers"

# Start services
echo ""
print_info "Starting Data Warehouse services..."
docker-compose up -d postgres clickhouse

# Wait for databases
echo ""
print_info "Waiting for databases to be ready..."
echo -n "  PostgreSQL: "
for i in {1..30}; do
    if docker exec postgres_oltp pg_isready -U admin -d oltp &>/dev/null; then
        print_success "Ready"
        break
    fi
    echo -n "."
    sleep 2
done

echo -n "  ClickHouse: "
for i in {1..30}; do
    if curl -s http://localhost:8123/ping &>/dev/null; then
        print_success "Ready"
        break
    fi
    echo -n "."
    sleep 2
done

# Run ETL
echo ""
print_info "Running ETL pipeline..."
docker-compose up -d etl

# Wait for ETL to complete
sleep 5
docker-compose logs etl

# Start Superset
echo ""
print_info "Starting Superset..."
docker-compose up -d superset

# Summary
echo ""
echo "=================================================="
echo "  Services Status"
echo "=================================================="
docker-compose ps

echo ""
echo "=================================================="
echo "  Access Information"
echo "=================================================="
echo "  PostgreSQL:  localhost:5432"
echo "    - Database: oltp"
echo "    - User: admin"
echo "    - Password: admin"
echo ""
echo "  ClickHouse:  localhost:8123 (HTTP)"
echo "               localhost:9000 (Native)"
echo ""
echo "  Superset:    http://localhost:8088"
echo "    - Setup admin user with:"
echo "      docker exec -it superset_dashboard superset fab create-admin"
echo "=================================================="
echo ""

print_success "Data Warehouse is ready!"
echo ""
echo "Next steps:"
echo "  1. Check ETL logs: docker-compose logs etl"
echo "  2. Access ClickHouse: docker exec -it clickhouse_dw clickhouse-client"
echo "  3. Setup Superset: See README.md"
echo ""
