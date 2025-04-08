#!/bin/bash

# Function to run dbt command with retries
run_dbt_with_retries() {
  local retries=$1
  local count=0
  local success=0

  while [ $count -lt $retries ]; do
    dbt run --select +order_summary
    if [ $? -eq 0 ]; then
      success=1
      break
    fi
    count=$((count + 1))
    echo "Retry $count/$retries failed. Retrying..."
  done

  if [ $success -eq 0 ]; then
    echo "All retries failed."
    exit 1
  else
    echo "dbt run succeeded."
  fi
}

# Number of retries
RETRIES=1

# Run dbt with retries
run_dbt_with_retries $RETRIES
