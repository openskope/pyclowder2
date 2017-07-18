from setuptools import setup

setup(name='pyclowder',
      version='2.0.0',
      packages=['pyclowder'],
      entry_points={
          'console_scripts': [
              'clowder = pyclowder.__main__:main',
          ]
      },
      zip_safe=True,
      )
