# My Python Project

A simple Python project template with hello world functionality, set up with modern Python tooling including `uv` for fast package management.

## Features

- Simple hello world functionality
- Modern Python project structure
- Fast package management with `uv`
- Code formatting with `black` and `ruff`
- Testing with `pytest`
- Type checking with `mypy`
- Command-line interface

## Prerequisites

- Python 3.8 or higher
- `uv` package manager ([installation guide](https://docs.astral.sh/uv/getting-started/installation/))

## Installation

### Using uv (recommended)

1. Clone the repository:

```bash
git clone https://github.com/yourusername/my-python-project.git
cd my-python-project
```

2. Install dependencies with uv:

```bash
# Install the project in development mode
uv sync

# Or install with development dependencies
uv sync --dev
```

3. Activate the virtual environment:

```bash
# uv automatically creates and manages the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

## Usage

### Command Line Interface

```bash
# Run the hello world application
uv run hello

# Show help
uv run hello --help

# Show version
uv run hello --version

# Show system information
uv run hello --info
```

### Python Module

```python
from video_artifact_processing_engine import hello_world, print_welcome_message

# Simple hello world
print(hello_world())

# Print welcome message
print_welcome_message()
```

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src --cov-report=html

# Run only fast tests
uv run pytest -m "not slow"
```

### Code Quality

```bash
# Format code with black
uv run black src/ tests/

# Lint code with ruff
uv run ruff check src/ tests/

# Auto-fix linting issues
uv run ruff check --fix src/ tests/

# Type checking
uv run mypy src/
```

### Adding Dependencies

```bash
# Add a runtime dependency
uv add requests

# Add a development dependency
uv add --dev pytest-mock

# Remove a dependency
uv remove requests
```

### Building and Distribution

```bash
# Build the package
uv build

# Install the built package
uv pip install dist/*.whl
```

## Project Structure

```text
video-artifact-processing-engine/
├── src/
│   └── video_artifact_processing_engine/
│       ├── __init__.py
│       ├── core/
│       ├── processors/
│       └── utils/
├── tests/
├── docs/
├── examples/
├── pyproject.toml
├── README.md
└── requirements.txt
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
# VideoArtifactProcessingEngine
