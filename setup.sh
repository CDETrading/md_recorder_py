#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting project setup...${NC}"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python3 is not installed. Please install Python3 first.${NC}"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo -e "${GREEN}Creating virtual environment...${NC}"
    python3 -m venv venv
else
    echo -e "${GREEN}Virtual environment already exists.${NC}"
fi

# Activate virtual environment
echo -e "${GREEN}Activating virtual environment...${NC}"
source venv/bin/activate

# Upgrade pip
echo -e "${GREEN}Upgrading pip...${NC}"
pip install --upgrade pip

# Install requirements if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo -e "${GREEN}Installing requirements...${NC}"
    pip install -r requirements.txt
else
    echo -e "${RED}No requirements.txt found. Skipping package installation.${NC}"
fi

# Create necessary directories
echo -e "${GREEN}Creating project directories...${NC}"
mkdir -p data
mkdir -p logs

# Set up git hooks directory if .git exists
if [ -d ".git" ]; then
    echo -e "${GREEN}Setting up git hooks...${NC}"
    mkdir -p .git/hooks
fi

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${GREEN}To activate the virtual environment, run: source venv/bin/activate${NC}"