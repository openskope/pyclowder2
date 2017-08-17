from setuptools import setup

setup(
    name='pyclowder',
    version='2.0.0',
    packages=['pyclowder'],
    zip_safe=True,

    install_requires=[
        'enum34',
        'pika',
        'PyYAML',
        'requests',
    ],

    entry_points={
        'console_scripts': [
            'clowder = pyclowder.__main__:main',
        ]
    },
)
