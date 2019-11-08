from setuptools import setup, find_packages

setup(
    name = "uvr",
    packages = find_packages(),
    include_package_data = True,
    install_requires = [
        "numpy",
        "xarray",
    ],
    entry_points={
        'console_scripts': [
            'read_uvr = read_uvr.uvrApp:main',
        ],
    },
    author = "Magnus Hagdorn",
    description = "download UVR satellite data",
)
