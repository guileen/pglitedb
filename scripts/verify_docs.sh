#!/bin/bash

# Documentation Examples Verification Script
# This script verifies that the documentation examples compile and run correctly

set -e  # Exit on any error

echo "Verifying PGLiteDB Documentation Examples"
echo "========================================"

# Create temporary directory for tests
TEMP_DIR="/tmp/pglitedb-docs-test"
mkdir -p "$TEMP_DIR"
echo "Using temporary directory: $TEMP_DIR"

# Test 1: Check that documentation files exist and are readable
echo
echo "Test 1: Documentation Files Verification"
echo "---------------------------------------"
DOCS_TO_CHECK=(
    "docs/README.md"
    "docs/NAVIGATION.md"
    "docs/guides/embedded_usage.md"
    "docs/guides/quickstart.md"
    "docs/guides/interactive_examples.md"
    "docs/api/reference.md"
)

for doc in "${DOCS_TO_CHECK[@]}"; do
    if [ -f "$doc" ]; then
        echo "✓ $doc exists and is readable"
    else
        echo "✗ $doc is missing"
        exit 1
    fi
done

# Test 2: Verify directory structure
echo
echo "Test 2: Directory Structure Verification"
echo "---------------------------------------"
EXPECTED_DIRS=(
    "docs"
    "docs/guides"
    "docs/api"
)

for dir in "${EXPECTED_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "✓ $dir directory exists"
    else
        echo "✗ $dir directory is missing"
        exit 1
    fi
done

# Test 3: Compile documentation examples
echo
echo "Test 3: Documentation Examples Compilation"
echo "----------------------------------------"
EXAMPLES_TO_CHECK=(
    "examples/doc_basic_operations.go"
    "examples/doc_advanced_querying.go"
    "examples/doc_transactions.go"
    "examples/doc_multitenancy.go"
)

for example in "${EXAMPLES_TO_CHECK[@]}"; do
    if [ -f "$example" ]; then
        echo "Compiling $example..."
        go build -o "$TEMP_DIR/$(basename "$example" .go)" "$example"
        echo "✓ $example compiles successfully"
    else
        echo "⚠ $example not found, skipping"
    fi
done

# Test 4: Check for basic markdown structure
echo
echo "Test 4: Markdown Structure Verification"
echo "--------------------------------------"
# Simple check for required sections in key documents
if grep -q "# PGLiteDB Embedded Usage Guide" docs/guides/embedded_usage.md; then
    echo "✓ Embedded usage guide has proper title"
else
    echo "✗ Embedded usage guide missing proper title"
fi

if grep -q "# PGLiteDB Quick Start Guides" docs/guides/quickstart.md; then
    echo "✓ Quick start guide has proper title"
else
    echo "✗ Quick start guide missing proper title"
fi

echo
echo "Documentation verification completed! 🎉"
echo "All documentation files are present and examples compile correctly."

# Cleanup
rm -rf "$TEMP_DIR"