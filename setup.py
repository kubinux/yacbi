from distutils.core import setup

with open('README.rst') as readme_file:
    long_description = readme_file.read()

setup(name='yacbi',
      version='0.1',
      py_modules=['yacbi'],
      description='Yet Another Clang-Based Indexer',
      long_description=long_description,
      author='Jakub Lewandowski',
      author_email='kuba@linux.com',
      url='http://github.com/kubus-puchatek/yacbi',
      )
