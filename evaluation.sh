#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Resource limits
MEMORY="8g"
CPUS="4"

echo "Starting evaluation script with memory limit: $MEMORY, CPUs: $CPUS"

# Function to check Docker system
check_docker() {
    echo -e "\n${YELLOW}Checking Docker system...${NC}"
    docker system df
    docker system info
}

# Function to clean up
cleanup() {
    echo -e "\n${YELLOW}Cleaning up Docker resources...${NC}"
    docker system prune -f
}

# Function to build and test Dask image
test_dask() {
    echo -e "\n${GREEN}Building Dask Docker image...${NC}"
    if docker build -f Dockerfile.dask -t dask-test .; then
        echo -e "${GREEN}Successfully built Dask image${NC}"
        
        echo -e "\n${GREEN}Running Dask container with resource limits...${NC}"
        # First, test if Dask is properly installed
        if docker run --rm --memory=$MEMORY --cpus=$CPUS dask-test python -c "import dask; print('Dask version:', dask.__version__)"; then
            echo -e "${GREEN}Dask installation test passed${NC}"
            
            # Now run the actual processing test
            echo -e "\n${GREEN}Running Dask processing test...${NC}"
            if docker run --rm \
                --memory=$MEMORY \
                --cpus=$CPUS \
                -v "$(pwd)/output:/app/output" \
                dask-test; then
                echo -e "${GREEN}Dask processing test passed successfully${NC}"
                return 0
            else
                echo -e "${RED}Dask processing test failed${NC}"
                return 1
            fi
        else
            echo -e "${RED}Dask installation test failed${NC}"
            return 1
        fi
    else
        echo -e "${RED}Failed to build Dask image${NC}"
        return 1
    fi
}

# Function to build and test Spark image
test_spark() {
    echo -e "\n${GREEN}Building Spark Docker image...${NC}"
    if docker build -f Dockerfile.spark -t spark-test .; then
        echo -e "${GREEN}Successfully built Spark image${NC}"
        
        echo -e "\n${GREEN}Running Spark container with resource limits...${NC}"
        # First, test if Spark is properly installed
        if docker run --rm --memory=$MEMORY --cpus=$CPUS spark-test spark-submit --version; then
            echo -e "${GREEN}Spark installation test passed${NC}"
            
            # Now run the actual processing test
            echo -e "\n${GREEN}Running Spark processing test...${NC}"
            if docker run --rm \
                --memory=$MEMORY \
                --cpus=$CPUS \
                -v "$(pwd)/output:/app/output" \
                spark-test; then
                echo -e "${GREEN}Spark processing test passed successfully${NC}"
                return 0
            else
                echo -e "${RED}Spark processing test failed${NC}"
                return 1
            fi
        else
            echo -e "${RED}Spark installation test failed${NC}"
            return 1
        fi
    else
        echo -e "${RED}Failed to build Spark image${NC}"
        return 1
    fi
}

# Function to compare results
compare_results() {
    echo -e "\n${YELLOW}Comparing performance metrics...${NC}"
    if [ -f "output/dask_metrics.txt" ] && [ -f "output/spark_metrics.txt" ]; then
        echo -e "Performance comparison:"
        echo -e "Dask results:"
        cat output/dask_metrics.txt
        echo -e "\nSpark results:"
        cat output/spark_metrics.txt
    else
        echo -e "${RED}Cannot find performance metrics files${NC}"
    fi
}

# Main execution
mkdir -p output
check_docker
cleanup

echo -e "\n${GREEN}Running tests for both Dask and Spark...${NC}"

# Test Dask
if test_dask; then
    dask_status="passed"
else
    dask_status="failed"
fi

# Test Spark
if test_spark; then
    spark_status="passed"
else
    spark_status="failed"
fi

# Compare results
compare_results

# Print summary
echo -e "\n${GREEN}=== Test Summary ===${NC}"
echo -e "Dask tests: ${dask_status}"
echo -e "Spark tests: ${spark_status}"
echo -e "Resource limits used:"
echo -e "Memory: $MEMORY"
echo -e "CPUs: $CPUS"

# Cleanup at the end
cleanup

# Exit with error if any test failed
if [ "$dask_status" = "failed" ] || [ "$spark_status" = "failed" ]; then
    exit 1
fi

exit 0

