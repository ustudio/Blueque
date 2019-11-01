try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


install_requires = [
    "argparse",
    "redis < 3.0.0",
]

setup(name="blueque",
      version="0.3.2",
      description="Simple job queuing for very long tasks",
      url="https://github.com/ustudio/Blueque",
      packages=["blueque"],
      install_requires=install_requires)
