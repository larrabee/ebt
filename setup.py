"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from shutil import copyfile
import os
# To use a consistent encoding
from os import path
import ebt_cli.__version__

here = path.abspath(path.dirname(__file__))


setup(
    name=ebt_cli.__version__.__name__,

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=ebt_cli.__version__.__version__,

    description=ebt_cli.__version__.__description__,
    long_description=ebt_cli.__version__.__long_description__,

    # The project's main homepage.
    url=ebt_cli.__version__.__url__,

    # Author details
    author=ebt_cli.__version__.__author__,
    author_email=ebt_cli.__version__.__email__,

    # Choose your license
    license=ebt_cli.__version__.__license__,

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Unix',
        'Topic :: Utilities',
        'Topic :: System :: Archiving :: Backup',
        
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],

    # What does your project relate to?
    keywords='diff-dd diff disk dump tool',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=["configobj", "boto>=2,<3", "mysqlclient", "lxml", 'multiprocess'],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={},

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.spec', ],
    },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'ddd=ebt_cli:ddd_cli',
            'ebt=ebt_cli:ebt_cli',
        ],
    },
)

config_dir = '/etc/ebt/'
config_files = [
                   (config_dir, 'ebt.conf'),
                   (config_dir, 'plans.py'),
               ]

for config in config_files:
    if not path.exists(config[0]):
        os.makedirs(config[0])
    if not path.exists(path.join(config[0], config[1])):
        copyfile(config[1], path.join(config[0], config[1]))
