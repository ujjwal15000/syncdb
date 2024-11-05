from setuptools import setup, find_packages

setup(
    name="my_project",
    version="0.1.0",
    description="A brief description of your project",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/my_project",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "requests>=2.25.1"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    # entry_points={
    #     'console_scripts': [
    #         'my_script=my_project.module:function',
    #     ],
    # },
)
