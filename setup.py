from distutils.core import setup


with open("requirements.txt") as requirements_file:
    requirements = filter(lambda r_striped: r_striped,
                          map(lambda r: r.strip(), requirements_file.readlines()))

setup(name="blueque",
      version="0.1",
      description="Simple job queuing for very long tasks",
      url="https://github.com/ustudio/Blueque",
      packages=["blueque"],
      install_requires=requirements)
