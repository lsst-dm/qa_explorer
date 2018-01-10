from setuptools import setup

setup(name='explorer',
      version='0.1',
      description='Interactive QA explorer',
      url='http://github.com/timothydmorton/qa_explorer',
      author='Timothy D Morton',
      license='MIT',
      packages=['explorer'],
      package_data={'explorer':['data/*']},
      zip_safe=False)
