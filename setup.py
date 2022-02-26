from setuptools import setup

setup(
    name='neohelper',
    version='0.1.0',
    py_modules=['neohelper'],
    install_requires=[
        'click',
        'neo4j-driver'
    ],
    entry_points={
        'console_scripts': [
            'neohelper=neohelper.cli:cli',
        ],
    },
)
