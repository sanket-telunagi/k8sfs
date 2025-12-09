from setuptools import setup, find_packages

setup(
    name="k8s-fs-monitor",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "kubernetes>=28.1.0",
        "prometheus-client>=0.19.0",
        "pyyaml>=6.0.1",
        "tenacity>=8.2.3",
    ],
    python_requires=">=3.9",
    author="DevOps Team",
    description="Production-grade Kubernetes filesystem monitoring",
    entry_points={
        "console_scripts": [
            "k8s-fs-monitor=src.main:main",
        ],
    },
)
