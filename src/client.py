import gpudb
import os


def create_kinetica_client():
    """Create and return a GPUdb client instance using env variables."""
    options = gpudb.GPUdb.Options()
    options.username = os.getenv("KINETICA_USER")
    options.password = os.getenv("KINETICA_PASSWORD")
    return gpudb.GPUdb(
        host=[os.getenv("KINETICA_URL")],
        options=options
    )
