#!/bin/bash
# Example script to create and manage dynamic sampling rules

ES_URL="http://localhost:9200"
ES_USER="elastic"
ES_PASS="password"
INDEX=".elastic-sampling-config"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function create_rule() {
  local rule_id=$1
  local rule_json=$2
  
  echo -e "${BLUE}Creating rule: ${rule_id}${NC}"
  curl -X PUT "${ES_URL}/${INDEX}/_doc/${rule_id}" \
    -H "Content-Type: application/json" \
    -u "${ES_USER}:${ES_PASS}" \
    -d "${rule_json}"
  echo -e "\n${GREEN}✓ Rule created${NC}\n"
}

function update_rule() {
  local rule_id=$1
  local update_json=$2
  
  echo -e "${BLUE}Updating rule: ${rule_id}${NC}"
  curl -X POST "${ES_URL}/${INDEX}/_update/${rule_id}" \
    -H "Content-Type: application/json" \
    -u "${ES_USER}:${ES_PASS}" \
    -d "${update_json}"
  echo -e "\n${GREEN}✓ Rule updated${NC}\n"
}

function list_rules() {
  echo -e "${BLUE}Listing all sampling rules:${NC}"
  curl -X GET "${ES_URL}/${INDEX}/_search?pretty" \
    -H "Content-Type: application/json" \
    -u "${ES_USER}:${ES_PASS}"
}

function delete_rule() {
  local rule_id=$1
  
  echo -e "${YELLOW}Deleting rule: ${rule_id}${NC}"
  curl -X DELETE "${ES_URL}/${INDEX}/_doc/${rule_id}" \
    -u "${ES_USER}:${ES_PASS}"
  echo -e "\n${GREEN}✓ Rule deleted${NC}\n"
}

# Main menu
case "$1" in
  "example1")
    # Sample 100% of nginx errors
    create_rule "nginx-errors" '{
      "enabled": true,
      "priority": 100,
      "match": {
        "stream": "logs-nginx*",
        "condition": "severity_text == \"ERROR\""
      },
      "sample_rate": 1.0,
      "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
    }'
    ;;
    
  "example2")
    # Sample 10% of all warnings
    create_rule "all-warnings" '{
      "enabled": true,
      "priority": 50,
      "match": {
        "stream": "*",
        "condition": "severity_text == \"WARN\" or severity_text == \"WARNING\""
      },
      "sample_rate": 0.1,
      "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
    }'
    ;;
    
  "example3")
    # Sample production errors at 100%
    create_rule "prod-errors" '{
      "enabled": true,
      "priority": 100,
      "match": {
        "resource_attrs": {
          "deployment.environment": "production"
        },
        "condition": "severity_text == \"ERROR\""
      },
      "sample_rate": 1.0,
      "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
    }'
    ;;
    
  "update-rate")
    # Update sample rate for a rule
    if [ -z "$2" ] || [ -z "$3" ]; then
      echo "Usage: $0 update-rate <rule_id> <new_rate>"
      echo "Example: $0 update-rate nginx-errors 0.5"
      exit 1
    fi
    update_rule "$2" '{
      "doc": {
        "sample_rate": '"$3"',
        "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
      }
    }'
    ;;
    
  "disable")
    # Disable a rule
    if [ -z "$2" ]; then
      echo "Usage: $0 disable <rule_id>"
      exit 1
    fi
    update_rule "$2" '{
      "doc": {
        "enabled": false,
        "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
      }
    }'
    ;;
    
  "enable")
    # Enable a rule
    if [ -z "$2" ]; then
      echo "Usage: $0 enable <rule_id>"
      exit 1
    fi
    update_rule "$2" '{
      "doc": {
        "enabled": true,
        "updated_at": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
      }
    }'
    ;;
    
  "list")
    list_rules
    ;;
    
  "delete")
    if [ -z "$2" ]; then
      echo "Usage: $0 delete <rule_id>"
      exit 1
    fi
    delete_rule "$2"
    ;;
    
  *)
    echo "Dynamic Sampling Rule Management"
    echo "================================"
    echo ""
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  example1              - Create nginx error rule (100% sampling)"
    echo "  example2              - Create warning rule (10% sampling)"
    echo "  example3              - Create production error rule"
    echo "  update-rate <id> <rate> - Update sampling rate (0.0-1.0)"
    echo "  disable <id>          - Disable a rule"
    echo "  enable <id>           - Enable a rule"
    echo "  list                  - List all rules"
    echo "  delete <id>           - Delete a rule"
    echo ""
    echo "Examples:"
    echo "  $0 example1"
    echo "  $0 update-rate nginx-errors 0.5"
    echo "  $0 disable nginx-errors"
    echo "  $0 list"
    echo ""
    echo "Note: Changes take effect after poll_interval (default: 30s)"
    ;;
esac
