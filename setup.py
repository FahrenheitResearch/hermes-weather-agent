from setuptools import setup, find_packages

setup(
    name="hermes-weather-agent",
    version="0.1.0",
    description="MCP tools for AI-driven weather model training — fetch, compute, visualize, build datasets",
    author="Fahrenheit Research",
    packages=find_packages(),
    py_modules=["mcp_server", "agent"],
    install_requires=[
        "rustmet>=0.1.0",
        "metrust>=0.3.5",
        "numpy",
        "Pillow",
        "requests",
        "mcp>=1.0.0",
    ],
    entry_points={
        "console_scripts": [
            "weather-agent=agent:main",
            "weather-mcp=mcp_server:run_cli",
        ],
    },
    python_requires=">=3.10",
)
