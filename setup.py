import os
from setuptools import setup, find_packages


setup(
    name='uindex',
    version='0.1.dev0',
    description="Micro file index, with diff and dedup tools.",
    url='https://github.com/vfxetc/uindex',
    
    packages=find_packages(exclude=['build*', 'tests*']),
    include_package_data=True,
    
    author='Mike Boers',
    author_email='floss+uindex@vfxetc.com',
    license='BSD-3',
    
    entry_points={
        'console_scripts': '''
            uindex-create = uindex.create:main
            uindex-dedupe = uindex.dedupe:main
            uindex-diff = uindex.diff:main
        ''',
    },

    classifiers=[
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Archiving',
        'Topic :: System :: Filesystems',
    ],
    
)
