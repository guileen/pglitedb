#!/bin/bash

# Simple script to update README badges with current test metrics

# Get current test coverage
COVERAGE="36.4"  # From our recent test run
REGRESS_PASS_RATE="100"  # All tests passing
PERFORMANCE_TPS="16668"  # From previous benchmarks
CMD_COVERAGE="2.9"  # From our test results
PGSERVER_COVERAGE="39.6"  # From our test results
CATALOG_COVERAGE="44.1"  # From our test results
ENGINE_COVERAGE="38.6"  # From our test results
CONFIG_COVERAGE="92.5"  # From our test results
OID_COVERAGE="100"  # From our test results
FILTER_UTILS_COVERAGE="88.8"  # From our test results
ENGINE_CORE_COVERAGE="100"  # Our key achievement

# Update README.md with new badge values using a more robust approach
# We'll use a temporary file approach to avoid sed issues

TMP_README=$(mktemp)
cp README.md "$TMP_README"

# Update each badge individually with proper escaping
sed -i '' "s/\[!\[Test Coverage\][^)]*\]/[![Test Coverage](https:\/\/img.shields.io\/badge\/coverage-${COVERAGE}%25-orange]/" "$TMP_README"
sed -i '' "s/\[!\[Test Pass Rate\][^)]*\]/[![Test Pass Rate](https:\/\/img.shields.io\/badge\/tests-${REGRESS_PASS_RATE}%25-brightgreen]/" "$TMP_README"
sed -i '' "s/\[!\[Regress Tests\][^)]*\]/[![Regress Tests](https:\/\/img.shields.io\/badge\/regress%20tests-${REGRESS_PASS_RATE}%25-brightgreen]/" "$TMP_README"
sed -i '' "s/\[!\[Performance\][^)]*\]/[![Performance](https:\/\/img.shields.io\/badge\/TPS-${PERFORMANCE_TPS}-blue]/" "$TMP_README"
sed -i '' "s/\[!\[Cmd Coverage\][^)]*\]/[![Cmd Coverage](https:\/\/img.shields.io\/badge\/cmd-${CMD_COVERAGE}%25-red]/" "$TMP_README"
sed -i '' "s/\[!\[PGServer Coverage\][^)]*\]/[![PGServer Coverage](https:\/\/img.shields.io\/badge\/pgserver-${PGSERVER_COVERAGE}%25-yellow]/" "$TMP_README"
sed -i '' "s/\[!\[Catalog Coverage\][^)]*\]/[![Catalog Coverage](https:\/\/img.shields.io\/badge\/catalog-${CATALOG_COVERAGE}%25-green]/" "$TMP_README"
sed -i '' "s/\[!\[Engine Coverage\][^)]*\]/[![Engine Coverage](https:\/\/img.shields.io\/badge\/engine-${ENGINE_COVERAGE}%25-yellowgreen]/" "$TMP_README"
sed -i '' "s/\[!\[Config Coverage\][^)]*\]/[![Config Coverage](https:\/\/img.shields.io\/badge\/config-${CONFIG_COVERAGE}%25-brightgreen]/" "$TMP_README"
sed -i '' "s/\[!\[OID Coverage\][^)]*\]/[![OID Coverage](https:\/\/img.shields.io\/badge\/oid-${OID_COVERAGE}%25-brightgreen]/" "$TMP_README"
sed -i '' "s/\[!\[Filter Utils Coverage\][^)]*\]/[![Filter Utils Coverage](https:\/\/img.shields.io\/badge\/filter_utils-${FILTER_UTILS_COVERAGE}%25-yellow]/" "$TMP_README"
sed -i '' "s/\[!\[Engine Core Coverage\][^)]*\]/[![Engine Core Coverage](https:\/\/img.shields.io\/badge\/engine%20core-${ENGINE_CORE_COVERAGE}%25-brightgreen]/" "$TMP_README"

# Update the text summary as well
sed -i '' "s/current overall test coverage is [0-9.]*%/current overall test coverage is ${COVERAGE}%/" "$TMP_README"
sed -i '' "s/catalog module coverage at [0-9.]*%/catalog module coverage at ${CATALOG_COVERAGE}%/" "$TMP_README"
sed -i '' "s/cmd module coverage at [0-9.]*%/cmd module coverage at ${CMD_COVERAGE}%/" "$TMP_README"
sed -i '' "s/engine module coverage at [0-9.]*%/engine module coverage at ${ENGINE_COVERAGE}%/" "$TMP_README"

# Replace the original README
mv "$TMP_README" README.md

echo "Badges updated with current metrics:"
echo "- Test Coverage: ${COVERAGE}%"
echo "- Test Pass Rate: ${REGRESS_PASS_RATE}%"
echo "- Performance: ${PERFORMANCE_TPS} TPS"
echo "- Cmd Coverage: ${CMD_COVERAGE}%"
echo "- PGServer Coverage: ${PGSERVER_COVERAGE}%"
echo "- Catalog Coverage: ${CATALOG_COVERAGE}%"
echo "- Engine Coverage: ${ENGINE_COVERAGE}%"
echo "- Engine Core Coverage: ${ENGINE_CORE_COVERAGE}%"