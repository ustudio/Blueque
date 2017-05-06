try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


install_requires = [
    "argparse",
    "redis"
]

setup(name="blueque",
      version="0.2.5",
      description="Simple job queuing for very long tasks",
      url="https://github.com/ustudio/Blueque",
      packages=["blueque"],
      install_requires=install_requires)
